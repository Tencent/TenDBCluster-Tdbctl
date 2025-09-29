#include "tc_restore.h"
#include "mysql/service_my_snprintf.h"
#include "sql_string.h"
#include "mysql.h"
#include "errmsg.h"
#include "tc_dump.h"
#include "tc_base.h"
#include "my_dir.h"

#include <cstring>
#include <sys/times.h>
#include <algorithm>

#ifdef HAVE_SYS_IOCTL_H
#include <sys/ioctl.h>
#endif

#ifdef FN_NO_CASE_SENSE
#define cmp_database(cs,A,B) my_strcasecmp((cs), (A), (B))
#else
#define cmp_database(cs,A,B) strcmp((A),(B))
#endif

#define DEFAULT_DELIMITER ";"
#define MAX_BATCH_BUFFER_SIZE (1024L * 1024L * 1024L)

const char DELIMITER_NAME[]= "delimiter";
const uint DELIMITER_NAME_LEN= sizeof(DELIMITER_NAME) - 1;
inline bool is_delimiter_command(char *name, ulong len)
{
  /*
    Delimiter command has a parameter, so the length of the whole command
    is larger than DELIMITER_NAME_LEN.  We don't care the parameter, so
    only name(first DELIMITER_NAME_LEN bytes) is checked.
  */
  return (len >= DELIMITER_NAME_LEN &&
          !my_strnncoll(&my_charset_latin1, (uchar*) name, DELIMITER_NAME_LEN,
                        (uchar *) DELIMITER_NAME, DELIMITER_NAME_LEN));
}

/* Various printing flags */
#define MY_PRINT_ESC_0 1  /* Replace 0x00 bytes to "\0"              */
#define MY_PRINT_SPS_0 2  /* Replace 0x00 bytes to space             */
#define MY_PRINT_XML   4  /* Encode XML entities                     */
#define MY_PRINT_MB    8  /* Recognize multi-byte characters         */
#define MY_PRINT_CTRL 16  /* Replace TAB, NL, CR to "\t", "\n", "\r" */

static const char *xmlmeta[] = {
  "&", "&amp;",
  "<", "&lt;",
  ">", "&gt;",
  "\"", "&quot;",
  /* Turn \0 into a space. Why not &#0;? That's not valid XML or HTML. */
  "\0", " ",
  0, 0
};

/* Maximum memory limit that can be claimed by alloca(). */
#define MAX_ALLOCA_SIZE              512
/* Don't try to make a nice table if the data is too big */
#define MAX_COLUMN_LENGTH	     1024

Restore_Handler::COMMANDS Restore_Handler::commands[] = {
  { "?",      '?', &Restore_Handler::com_help_static,   1, "Synonym for `help'." },
  { "clear",  'c', &Restore_Handler::com_clear_static,  0, "Clear the current input statement."},
  { "connect",'r', &Restore_Handler::com_connect_static,1,
    "Reconnect to the server. Optional arguments are db and host." },
  { "delimiter", 'd', &Restore_Handler::com_delimiter_static,    1,
    "Set statement delimiter." },
  { "edit",   'e', &Restore_Handler::com_edit_static,   0, "Edit command with $EDITOR."},
  { "ego",    'G', &Restore_Handler::com_ego_static,    0,
    "Send command to mysql server, display result vertically."},
  { "exit",   'q', &Restore_Handler::com_quit_static,   0, "Exit mysql. Same as quit."},
  { "go",     'g', &Restore_Handler::com_go_static,     0, "Send command to mysql server." },
  { "help",   'h', &Restore_Handler::com_help_static,   1, "Display this help." },
  { "nopager",'n', &Restore_Handler::com_nopager_static,0, "Disable pager, print to stdout." },
  { "notee",  't', &Restore_Handler::com_notee_static,  0, "Don't write into outfile." },
  { "pager",  'P', &Restore_Handler::com_pager_static,  1, 
    "Set PAGER [to_pager]. Print the query results via PAGER." },
  { "print",  'p', &Restore_Handler::com_print_static,  0, "Print current command." },
  { "prompt", 'R', &Restore_Handler::com_prompt_static, 1, "Change your mysql prompt."},
  { "quit",   'q', &Restore_Handler::com_quit_static,   0, "Quit mysql." },
  { "rehash", '#', &Restore_Handler::com_rehash_static, 0, "Rebuild completion hash." },
  { "source", '.', &Restore_Handler::com_source_static, 1,
    "Execute an SQL script file. Takes a file name as an argument."},
  { "status", 's', &Restore_Handler::com_status_static, 0, "Get status information from the server."},
  { "system", '!', &Restore_Handler::com_shell_static,  1, "Execute a system shell command."},
  { "tee",    'T', &Restore_Handler::com_tee_static,    1, 
    "Set outfile [to_outfile]. Append everything into given outfile." },
  { "use",    'u', &Restore_Handler::com_use_static,    1,
    "Use another database. Takes database name as argument." },
  { "charset",    'C', &Restore_Handler::com_charset_static,    1,
    "Switch to another charset. Might be needed for processing binlog with multi-byte charsets." },
  { "warnings", 'W', &Restore_Handler::com_warnings_static,  0,
    "Show warnings after every statement." },
  { "nowarning", 'w', &Restore_Handler::com_nowarnings_static, 0,
    "Don't show warnings after every statement." },
  { "resetconnection",  'x', &Restore_Handler::com_resetconnection_static, 0,
    "Clean session context." },
  /* Get bash-like expansion for some commands */
  { "create table",     0, 0, 0, ""},
  { "create database",  0, 0, 0, ""},
  { "show databases",   0, 0, 0, ""},
  { "show fields from", 0, 0, 0, ""},
  { "show keys from",   0, 0, 0, ""},
  { "show tables",      0, 0, 0, ""},
  { "load data from",   0, 0, 0, ""},
  { "alter table",      0, 0, 0, ""},
  { "set option",       0, 0, 0, ""},
  { "lock tables",      0, 0, 0, ""},
  { "unlock tables",    0, 0, 0, ""},
  /* generated 2006-12-28.  Refresh occasionally from lexer. */
  { "ACTION", 0, 0, 0, ""},
  { "ADD", 0, 0, 0, ""},
  { "AFTER", 0, 0, 0, ""},
  { "AGAINST", 0, 0, 0, ""},
  { "AGGREGATE", 0, 0, 0, ""},
  { "ALL", 0, 0, 0, ""},
  { "ALGORITHM", 0, 0, 0, ""},
  { "ALTER", 0, 0, 0, ""},
  { "ANALYZE", 0, 0, 0, ""},
  { "AND", 0, 0, 0, ""},
  { "ANY", 0, 0, 0, ""},
  { "AS", 0, 0, 0, ""},
  { "ASC", 0, 0, 0, ""},
  { "ASCII", 0, 0, 0, ""},
  { "ASENSITIVE", 0, 0, 0, ""},
  { "AUTO_INCREMENT", 0, 0, 0, ""},
  { "AVG", 0, 0, 0, ""},
  { "AVG_ROW_LENGTH", 0, 0, 0, ""},
  { "BACKUP", 0, 0, 0, ""},
  { "BDB", 0, 0, 0, ""},
  { "BEFORE", 0, 0, 0, ""},
  { "BEGIN", 0, 0, 0, ""},
  { "BERKELEYDB", 0, 0, 0, ""},
  { "BETWEEN", 0, 0, 0, ""},
  { "BIGINT", 0, 0, 0, ""},
  { "BINARY", 0, 0, 0, ""},
  { "BINLOG", 0, 0, 0, ""},
  { "BIT", 0, 0, 0, ""},
  { "BLOB", 0, 0, 0, ""},
  { "BOOL", 0, 0, 0, ""},
  { "BOOLEAN", 0, 0, 0, ""},
  { "BOTH", 0, 0, 0, ""},
  { "BTREE", 0, 0, 0, ""},
  { "BY", 0, 0, 0, ""},
  { "BYTE", 0, 0, 0, ""},
  { "CACHE", 0, 0, 0, ""},
  { "CALL", 0, 0, 0, ""},
  { "CASCADE", 0, 0, 0, ""},
  { "CASCADED", 0, 0, 0, ""},
  { "CASE", 0, 0, 0, ""},
  { "CHAIN", 0, 0, 0, ""},
  { "CHANGE", 0, 0, 0, ""},
  { "CHANGED", 0, 0, 0, ""},
  { "CHAR", 0, 0, 0, ""},
  { "CHARACTER", 0, 0, 0, ""},
  { "CHARSET", 0, 0, 0, ""},
  { "CHECK", 0, 0, 0, ""},
  { "CHECKSUM", 0, 0, 0, ""},
  { "CIPHER", 0, 0, 0, ""},
  { "CLIENT", 0, 0, 0, ""},
  { "CLOSE", 0, 0, 0, ""},
  { "CODE", 0, 0, 0, ""},
  { "COLLATE", 0, 0, 0, ""},
  { "COLLATION", 0, 0, 0, ""},
  { "COLUMN", 0, 0, 0, ""},
  { "COLUMNS", 0, 0, 0, ""},
  { "COMMENT", 0, 0, 0, ""},
  { "COMMIT", 0, 0, 0, ""},
  { "COMMITTED", 0, 0, 0, ""},
  { "COMPACT", 0, 0, 0, ""},
  { "COMPRESSED", 0, 0, 0, ""},
  { "CONCURRENT", 0, 0, 0, ""},
  { "CONDITION", 0, 0, 0, ""},
  { "CONNECTION", 0, 0, 0, ""},
  { "CONSISTENT", 0, 0, 0, ""},
  { "CONSTRAINT", 0, 0, 0, ""},
  { "CONTAINS", 0, 0, 0, ""},
  { "CONTINUE", 0, 0, 0, ""},
  { "CONVERT", 0, 0, 0, ""},
  { "CREATE", 0, 0, 0, ""},
  { "CROSS", 0, 0, 0, ""},
  { "CUBE", 0, 0, 0, ""},
  { "CURRENT_DATE", 0, 0, 0, ""},
  { "CURRENT_TIME", 0, 0, 0, ""},
  { "CURRENT_TIMESTAMP", 0, 0, 0, ""},
  { "CURRENT_USER", 0, 0, 0, ""},
  { "CURSOR", 0, 0, 0, ""},
  { "DATA", 0, 0, 0, ""},
  { "DATABASE", 0, 0, 0, ""},
  { "DATABASES", 0, 0, 0, ""},
  { "DATE", 0, 0, 0, ""},
  { "DATETIME", 0, 0, 0, ""},
  { "DAY", 0, 0, 0, ""},
  { "DAY_HOUR", 0, 0, 0, ""},
  { "DAY_MICROSECOND", 0, 0, 0, ""},
  { "DAY_MINUTE", 0, 0, 0, ""},
  { "DAY_SECOND", 0, 0, 0, ""},
  { "DEALLOCATE", 0, 0, 0, ""},     
  { "DEC", 0, 0, 0, ""},
  { "DECIMAL", 0, 0, 0, ""},
  { "DECLARE", 0, 0, 0, ""},
  { "DEFAULT", 0, 0, 0, ""},
  { "DEFINER", 0, 0, 0, ""},
  { "DELAYED", 0, 0, 0, ""},
  { "DELAY_KEY_WRITE", 0, 0, 0, ""},
  { "DELETE", 0, 0, 0, ""},
  { "DESC", 0, 0, 0, ""},
  { "DESCRIBE", 0, 0, 0, ""},
  { "DES_KEY_FILE", 0, 0, 0, ""},
  { "DETERMINISTIC", 0, 0, 0, ""},
  { "DIRECTORY", 0, 0, 0, ""},
  { "DISABLE", 0, 0, 0, ""},
  { "DISCARD", 0, 0, 0, ""},
  { "DISTINCT", 0, 0, 0, ""},
  { "DISTINCTROW", 0, 0, 0, ""},
  { "DIV", 0, 0, 0, ""},
  { "DO", 0, 0, 0, ""},
  { "DOUBLE", 0, 0, 0, ""},
  { "DROP", 0, 0, 0, ""},
  { "DUAL", 0, 0, 0, ""},
  { "DUMPFILE", 0, 0, 0, ""},
  { "DUPLICATE", 0, 0, 0, ""},
  { "DYNAMIC", 0, 0, 0, ""},
  { "EACH", 0, 0, 0, ""},
  { "ELSE", 0, 0, 0, ""},
  { "ELSEIF", 0, 0, 0, ""},
  { "ENABLE", 0, 0, 0, ""},
  { "ENCLOSED", 0, 0, 0, ""},
  { "END", 0, 0, 0, ""},
  { "ENGINE", 0, 0, 0, ""},
  { "ENGINES", 0, 0, 0, ""},
  { "ENUM", 0, 0, 0, ""},
  { "ERRORS", 0, 0, 0, ""},
  { "ESCAPE", 0, 0, 0, ""},
  { "ESCAPED", 0, 0, 0, ""},
  { "EVENTS", 0, 0, 0, ""},
  { "EXECUTE", 0, 0, 0, ""},
  { "EXISTS", 0, 0, 0, ""},
  { "EXIT", 0, 0, 0, ""},
  { "EXPANSION", 0, 0, 0, ""},
  { "EXPLAIN", 0, 0, 0, ""},
  { "EXTENDED", 0, 0, 0, ""},
  { "FALSE", 0, 0, 0, ""},
  { "FAST", 0, 0, 0, ""},
  { "FETCH", 0, 0, 0, ""},
  { "FIELDS", 0, 0, 0, ""},
  { "FILE", 0, 0, 0, ""},
  { "FIRST", 0, 0, 0, ""},
  { "FIXED", 0, 0, 0, ""},
  { "FLOAT", 0, 0, 0, ""},
  { "FLOAT4", 0, 0, 0, ""},
  { "FLOAT8", 0, 0, 0, ""},
  { "FLUSH", 0, 0, 0, ""},
  { "FOR", 0, 0, 0, ""},
  { "FORCE", 0, 0, 0, ""},
  { "FOREIGN", 0, 0, 0, ""},
  { "FOUND", 0, 0, 0, ""},
  { "FROM", 0, 0, 0, ""},
  { "FULL", 0, 0, 0, ""},
  { "FULLTEXT", 0, 0, 0, ""},
  { "FUNCTION", 0, 0, 0, ""},
  { "GEOMETRY", 0, 0, 0, ""},
  { "GEOMETRYCOLLECTION", 0, 0, 0, ""},
  { "GET_FORMAT", 0, 0, 0, ""},
  { "GLOBAL", 0, 0, 0, ""},
  { "GRANT", 0, 0, 0, ""},
  { "GRANTS", 0, 0, 0, ""},
  { "GROUP", 0, 0, 0, ""},
  { "HANDLER", 0, 0, 0, ""},
  { "HASH", 0, 0, 0, ""},
  { "HAVING", 0, 0, 0, ""},
  { "HELP", 0, 0, 0, ""},
  { "HIGH_PRIORITY", 0, 0, 0, ""},
  { "HOSTS", 0, 0, 0, ""},
  { "HOUR", 0, 0, 0, ""},
  { "HOUR_MICROSECOND", 0, 0, 0, ""},
  { "HOUR_MINUTE", 0, 0, 0, ""},
  { "HOUR_SECOND", 0, 0, 0, ""},
  { "IDENTIFIED", 0, 0, 0, ""},
  { "IF", 0, 0, 0, ""},
  { "IGNORE", 0, 0, 0, ""},
  { "IMPORT", 0, 0, 0, ""},
  { "IN", 0, 0, 0, ""},
  { "INDEX", 0, 0, 0, ""},
  { "INDEXES", 0, 0, 0, ""},
  { "INFILE", 0, 0, 0, ""},
  { "INNER", 0, 0, 0, ""},
  { "INNOBASE", 0, 0, 0, ""},
  { "INNODB", 0, 0, 0, ""},
  { "INOUT", 0, 0, 0, ""},
  { "INSENSITIVE", 0, 0, 0, ""},
  { "INSERT", 0, 0, 0, ""},
  { "INSERT_METHOD", 0, 0, 0, ""},
  { "INT", 0, 0, 0, ""},
  { "INT1", 0, 0, 0, ""},
  { "INT2", 0, 0, 0, ""},
  { "INT3", 0, 0, 0, ""},
  { "INT4", 0, 0, 0, ""},
  { "INT8", 0, 0, 0, ""},
  { "INTEGER", 0, 0, 0, ""},
  { "INTERVAL", 0, 0, 0, ""},
  { "INTO", 0, 0, 0, ""},
  { "IO_THREAD", 0, 0, 0, ""},
  { "IS", 0, 0, 0, ""},
  { "ISOLATION", 0, 0, 0, ""},
  { "ISSUER", 0, 0, 0, ""},
  { "ITERATE", 0, 0, 0, ""},
  { "INVOKER", 0, 0, 0, ""},
  { "JOIN", 0, 0, 0, ""},
  { "KEY", 0, 0, 0, ""},
  { "KEYS", 0, 0, 0, ""},
  { "KILL", 0, 0, 0, ""},
  { "LANGUAGE", 0, 0, 0, ""},
  { "LAST", 0, 0, 0, ""},
  { "LEADING", 0, 0, 0, ""},
  { "LEAVE", 0, 0, 0, ""},
  { "LEAVES", 0, 0, 0, ""},
  { "LEFT", 0, 0, 0, ""},
  { "LEVEL", 0, 0, 0, ""},
  { "LIKE", 0, 0, 0, ""},
  { "LIMIT", 0, 0, 0, ""},
  { "LINES", 0, 0, 0, ""},
  { "LINESTRING", 0, 0, 0, ""},
  { "LOAD", 0, 0, 0, ""},
  { "LOCAL", 0, 0, 0, ""},
  { "LOCALTIME", 0, 0, 0, ""},
  { "LOCALTIMESTAMP", 0, 0, 0, ""},
  { "LOCK", 0, 0, 0, ""},
  { "LOCKS", 0, 0, 0, ""},
  { "LOGS", 0, 0, 0, ""},
  { "LONG", 0, 0, 0, ""},
  { "LONGBLOB", 0, 0, 0, ""},
  { "LONGTEXT", 0, 0, 0, ""},
  { "LOOP", 0, 0, 0, ""},
  { "LOW_PRIORITY", 0, 0, 0, ""},
  { "MASTER", 0, 0, 0, ""},
  { "MASTER_CONNECT_RETRY", 0, 0, 0, ""},
  { "MASTER_HOST", 0, 0, 0, ""},
  { "MASTER_LOG_FILE", 0, 0, 0, ""},
  { "MASTER_LOG_POS", 0, 0, 0, ""},
  { "MASTER_PASSWORD", 0, 0, 0, ""},
  { "MASTER_PORT", 0, 0, 0, ""},
  { "MASTER_SERVER_ID", 0, 0, 0, ""},
  { "MASTER_SSL", 0, 0, 0, ""},
  { "MASTER_SSL_CA", 0, 0, 0, ""},
  { "MASTER_SSL_CAPATH", 0, 0, 0, ""},
  { "MASTER_SSL_CERT", 0, 0, 0, ""},
  { "MASTER_SSL_CIPHER", 0, 0, 0, ""},
  { "MASTER_TLS_VERSION", 0, 0, 0, ""},
  { "MASTER_SSL_KEY", 0, 0, 0, ""},
  { "MASTER_USER", 0, 0, 0, ""},
  { "MATCH", 0, 0, 0, ""},
  { "MAX_CONNECTIONS_PER_HOUR", 0, 0, 0, ""},
  { "MAX_QUERIES_PER_HOUR", 0, 0, 0, ""},
  { "MAX_ROWS", 0, 0, 0, ""},
  { "MAX_UPDATES_PER_HOUR", 0, 0, 0, ""},
  { "MAX_USER_CONNECTIONS", 0, 0, 0, ""},
  { "MEDIUM", 0, 0, 0, ""},
  { "MEDIUMBLOB", 0, 0, 0, ""},
  { "MEDIUMINT", 0, 0, 0, ""},
  { "MEDIUMTEXT", 0, 0, 0, ""},
  { "MERGE", 0, 0, 0, ""},
  { "MICROSECOND", 0, 0, 0, ""},
  { "MIDDLEINT", 0, 0, 0, ""},
  { "MIGRATE", 0, 0, 0, ""},
  { "MINUTE", 0, 0, 0, ""},
  { "MINUTE_MICROSECOND", 0, 0, 0, ""},
  { "MINUTE_SECOND", 0, 0, 0, ""},
  { "MIN_ROWS", 0, 0, 0, ""},
  { "MOD", 0, 0, 0, ""},
  { "MODE", 0, 0, 0, ""},
  { "MODIFIES", 0, 0, 0, ""},
  { "MODIFY", 0, 0, 0, ""},
  { "MONTH", 0, 0, 0, ""},
  { "MULTILINESTRING", 0, 0, 0, ""},
  { "MULTIPOINT", 0, 0, 0, ""},
  { "MULTIPOLYGON", 0, 0, 0, ""},
  { "MUTEX", 0, 0, 0, ""},
  { "NAME", 0, 0, 0, ""},
  { "NAMES", 0, 0, 0, ""},
  { "NATIONAL", 0, 0, 0, ""},
  { "NATURAL", 0, 0, 0, ""},
  { "NDB", 0, 0, 0, ""},
  { "NDBCLUSTER", 0, 0, 0, ""},
  { "NCHAR", 0, 0, 0, ""},
  { "NEW", 0, 0, 0, ""},
  { "NEXT", 0, 0, 0, ""},
  { "NO", 0, 0, 0, ""},
  { "NONE", 0, 0, 0, ""},
  { "NOT", 0, 0, 0, ""},
  { "NO_WRITE_TO_BINLOG", 0, 0, 0, ""},
  { "NULL", 0, 0, 0, ""},
  { "NUMERIC", 0, 0, 0, ""},
  { "NVARCHAR", 0, 0, 0, ""},
  { "OFFSET", 0, 0, 0, ""},
  { "ON", 0, 0, 0, ""},
  { "ONE", 0, 0, 0, ""},
  { "ONE_SHOT", 0, 0, 0, ""},
  { "OPEN", 0, 0, 0, ""},
  { "OPTIMIZE", 0, 0, 0, ""},
  { "OPTION", 0, 0, 0, ""},
  { "OPTIONALLY", 0, 0, 0, ""},
  { "OR", 0, 0, 0, ""},
  { "ORDER", 0, 0, 0, ""},
  { "OUT", 0, 0, 0, ""},
  { "OUTER", 0, 0, 0, ""},
  { "OUTFILE", 0, 0, 0, ""},
  { "PACK_KEYS", 0, 0, 0, ""},
  { "PARTIAL", 0, 0, 0, ""},
  { "PASSWORD", 0, 0, 0, ""},
  { "PHASE", 0, 0, 0, ""},
  { "POINT", 0, 0, 0, ""},
  { "POLYGON", 0, 0, 0, ""},
  { "PRECISION", 0, 0, 0, ""},
  { "PREPARE", 0, 0, 0, ""},
  { "PREV", 0, 0, 0, ""},
  { "PRIMARY", 0, 0, 0, ""},
  { "PRIVILEGES", 0, 0, 0, ""},
  { "PROCEDURE", 0, 0, 0, ""},
  { "PROCESS", 0, 0, 0, ""},
  { "PROCESSLIST", 0, 0, 0, ""},
  { "PURGE", 0, 0, 0, ""},
  { "QUARTER", 0, 0, 0, ""},
  { "QUERY", 0, 0, 0, ""},
  { "QUICK", 0, 0, 0, ""},
  { "READ", 0, 0, 0, ""},
  { "READS", 0, 0, 0, ""},
  { "REAL", 0, 0, 0, ""},
  { "RECOVER", 0, 0, 0, ""},
  { "REDUNDANT", 0, 0, 0, ""},
  { "REFERENCES", 0, 0, 0, ""},
  { "REGEXP", 0, 0, 0, ""},
  { "RELAY_LOG_FILE", 0, 0, 0, ""},
  { "RELAY_LOG_POS", 0, 0, 0, ""},
  { "RELAY_THREAD", 0, 0, 0, ""},
  { "RELEASE", 0, 0, 0, ""},
  { "RELOAD", 0, 0, 0, ""},
  { "RENAME", 0, 0, 0, ""},
  { "REPAIR", 0, 0, 0, ""},
  { "REPEATABLE", 0, 0, 0, ""},
  { "REPLACE", 0, 0, 0, ""},
  { "REPLICATION", 0, 0, 0, ""},
  { "REPEAT", 0, 0, 0, ""},
  { "REQUIRE", 0, 0, 0, ""},
  { "RESET", 0, 0, 0, ""},
  { "RESTORE", 0, 0, 0, ""},
  { "RESTRICT", 0, 0, 0, ""},
  { "RESUME", 0, 0, 0, ""},
  { "RETURN", 0, 0, 0, ""},
  { "RETURNS", 0, 0, 0, ""},
  { "REVOKE", 0, 0, 0, ""},
  { "RIGHT", 0, 0, 0, ""},
  { "RLIKE", 0, 0, 0, ""},
  { "ROLLBACK", 0, 0, 0, ""},
  { "ROLLUP", 0, 0, 0, ""},
  { "ROUTINE", 0, 0, 0, ""},
  { "ROW", 0, 0, 0, ""},
  { "ROWS", 0, 0, 0, ""},
  { "ROW_FORMAT", 0, 0, 0, ""},
  { "RTREE", 0, 0, 0, ""},
  { "SAVEPOINT", 0, 0, 0, ""},
  { "SCHEMA", 0, 0, 0, ""},
  { "SCHEMAS", 0, 0, 0, ""},
  { "SECOND", 0, 0, 0, ""},
  { "SECOND_MICROSECOND", 0, 0, 0, ""},
  { "SECURITY", 0, 0, 0, ""},
  { "SELECT", 0, 0, 0, ""},
  { "SENSITIVE", 0, 0, 0, ""},
  { "SEPARATOR", 0, 0, 0, ""},
  { "SERIAL", 0, 0, 0, ""},
  { "SERIALIZABLE", 0, 0, 0, ""},
  { "SESSION", 0, 0, 0, ""},
  { "SET", 0, 0, 0, ""},
  { "SHARE", 0, 0, 0, ""},
  { "SHOW", 0, 0, 0, ""},
  { "SHUTDOWN", 0, 0, 0, ""},
  { "SIGNED", 0, 0, 0, ""},
  { "SIMPLE", 0, 0, 0, ""},
  { "SLAVE", 0, 0, 0, ""},
  { "SNAPSHOT", 0, 0, 0, ""},
  { "SMALLINT", 0, 0, 0, ""},
  { "SOME", 0, 0, 0, ""},
  { "SONAME", 0, 0, 0, ""},
  { "SOUNDS", 0, 0, 0, ""},
  { "SPATIAL", 0, 0, 0, ""},
  { "SPECIFIC", 0, 0, 0, ""},
  { "SQL", 0, 0, 0, ""},
  { "SQLEXCEPTION", 0, 0, 0, ""},
  { "SQLSTATE", 0, 0, 0, ""},
  { "SQLWARNING", 0, 0, 0, ""},
  { "SQL_BIG_RESULT", 0, 0, 0, ""},
  { "SQL_BUFFER_RESULT", 0, 0, 0, ""},
  { "SQL_CACHE", 0, 0, 0, ""},
  { "SQL_CALC_FOUND_ROWS", 0, 0, 0, ""},
  { "SQL_NO_CACHE", 0, 0, 0, ""},
  { "SQL_SMALL_RESULT", 0, 0, 0, ""},
  { "SQL_THREAD", 0, 0, 0, ""},
  { "SQL_TSI_SECOND", 0, 0, 0, ""},
  { "SQL_TSI_MINUTE", 0, 0, 0, ""},
  { "SQL_TSI_HOUR", 0, 0, 0, ""},
  { "SQL_TSI_DAY", 0, 0, 0, ""},
  { "SQL_TSI_WEEK", 0, 0, 0, ""},
  { "SQL_TSI_MONTH", 0, 0, 0, ""},
  { "SQL_TSI_QUARTER", 0, 0, 0, ""},
  { "SQL_TSI_YEAR", 0, 0, 0, ""},
  { "SSL", 0, 0, 0, ""},
  { "START", 0, 0, 0, ""},
  { "STARTING", 0, 0, 0, ""},
  { "STATUS", 0, 0, 0, ""},
  { "STOP", 0, 0, 0, ""},
  { "STORAGE", 0, 0, 0, ""},
  { "STRAIGHT_JOIN", 0, 0, 0, ""},
  { "STRING", 0, 0, 0, ""},
  { "STRIPED", 0, 0, 0, ""},
  { "SUBJECT", 0, 0, 0, ""},
  { "SUPER", 0, 0, 0, ""},
  { "SUSPEND", 0, 0, 0, ""},
  { "TABLE", 0, 0, 0, ""},
  { "TABLES", 0, 0, 0, ""},
  { "TABLESPACE", 0, 0, 0, ""},
  { "TEMPORARY", 0, 0, 0, ""},
  { "TEMPTABLE", 0, 0, 0, ""},
  { "TERMINATED", 0, 0, 0, ""},
  { "TEXT", 0, 0, 0, ""},
  { "THEN", 0, 0, 0, ""},
  { "TIME", 0, 0, 0, ""},
  { "TIMESTAMP", 0, 0, 0, ""},
  { "TIMESTAMPADD", 0, 0, 0, ""},
  { "TIMESTAMPDIFF", 0, 0, 0, ""},
  { "TINYBLOB", 0, 0, 0, ""},
  { "TINYINT", 0, 0, 0, ""},
  { "TINYTEXT", 0, 0, 0, ""},
  { "TO", 0, 0, 0, ""},
  { "TRAILING", 0, 0, 0, ""},
  { "TRANSACTION", 0, 0, 0, ""},
  { "TRIGGER", 0, 0, 0, ""},
  { "TRIGGERS", 0, 0, 0, ""},
  { "TRUE", 0, 0, 0, ""},
  { "TRUNCATE", 0, 0, 0, ""},
  { "TYPE", 0, 0, 0, ""},
  { "TYPES", 0, 0, 0, ""},
  { "UNCOMMITTED", 0, 0, 0, ""},
  { "UNDEFINED", 0, 0, 0, ""},
  { "UNDO", 0, 0, 0, ""},
  { "UNICODE", 0, 0, 0, ""},
  { "UNION", 0, 0, 0, ""},
  { "UNIQUE", 0, 0, 0, ""},
  { "UNKNOWN", 0, 0, 0, ""},
  { "UNLOCK", 0, 0, 0, ""},
  { "UNSIGNED", 0, 0, 0, ""},
  { "UNTIL", 0, 0, 0, ""},
  { "UPDATE", 0, 0, 0, ""},
  { "UPGRADE", 0, 0, 0, ""},
  { "USAGE", 0, 0, 0, ""},
  { "USE", 0, 0, 0, ""},
  { "USER", 0, 0, 0, ""},
  { "USER_RESOURCES", 0, 0, 0, ""},
  { "USE_FRM", 0, 0, 0, ""},
  { "USING", 0, 0, 0, ""},
  { "UTC_DATE", 0, 0, 0, ""},
  { "UTC_TIME", 0, 0, 0, ""},
  { "UTC_TIMESTAMP", 0, 0, 0, ""},
  { "VALUE", 0, 0, 0, ""},
  { "VALUES", 0, 0, 0, ""},
  { "VARBINARY", 0, 0, 0, ""},
  { "VARCHAR", 0, 0, 0, ""},
  { "VARCHARACTER", 0, 0, 0, ""},
  { "VARIABLES", 0, 0, 0, ""},
  { "VARYING", 0, 0, 0, ""},
  { "WARNINGS", 0, 0, 0, ""},
  { "WEEK", 0, 0, 0, ""},
  { "WHEN", 0, 0, 0, ""},
  { "WHERE", 0, 0, 0, ""},
  { "WHILE", 0, 0, 0, ""},
  { "VIEW", 0, 0, 0, ""},
  { "WITH", 0, 0, 0, ""},
  { "WORK", 0, 0, 0, ""},
  { "WRITE", 0, 0, 0, ""},
  { "X509", 0, 0, 0, ""},
  { "XOR", 0, 0, 0, ""},
  { "XA", 0, 0, 0, ""},
  { "YEAR", 0, 0, 0, ""},
  { "YEAR_MONTH", 0, 0, 0, ""},
  { "ZEROFILL", 0, 0, 0, ""},
  { "ABS", 0, 0, 0, ""},
  { "ACOS", 0, 0, 0, ""},
  { "ADDDATE", 0, 0, 0, ""},
  { "ADDTIME", 0, 0, 0, ""},
  { "AES_ENCRYPT", 0, 0, 0, ""},
  { "AES_DECRYPT", 0, 0, 0, ""},
  { "AREA", 0, 0, 0, ""},
  { "ASIN", 0, 0, 0, ""},
  { "ASBINARY", 0, 0, 0, ""},
  { "ASTEXT", 0, 0, 0, ""},
  { "ASWKB", 0, 0, 0, ""},
  { "ASWKT", 0, 0, 0, ""},
  { "ATAN", 0, 0, 0, ""},
  { "ATAN2", 0, 0, 0, ""},
  { "BENCHMARK", 0, 0, 0, ""},
  { "BIN", 0, 0, 0, ""},
  { "BIT_COUNT", 0, 0, 0, ""},
  { "BIT_OR", 0, 0, 0, ""},
  { "BIT_AND", 0, 0, 0, ""},
  { "BIT_XOR", 0, 0, 0, ""},
  { "CAST", 0, 0, 0, ""},
  { "CEIL", 0, 0, 0, ""},
  { "CEILING", 0, 0, 0, ""},
  { "BIT_LENGTH", 0, 0, 0, ""},
  { "CENTROID", 0, 0, 0, ""},
  { "CHAR_LENGTH", 0, 0, 0, ""},
  { "CHARACTER_LENGTH", 0, 0, 0, ""},
  { "COALESCE", 0, 0, 0, ""},
  { "COERCIBILITY", 0, 0, 0, ""},
  { "COMPRESS", 0, 0, 0, ""},
  { "CONCAT", 0, 0, 0, ""},
  { "CONCAT_WS", 0, 0, 0, ""},
  { "CONNECTION_ID", 0, 0, 0, ""},
  { "CONV", 0, 0, 0, ""},
  { "CONVERT_TZ", 0, 0, 0, ""},
  { "COUNT", 0, 0, 0, ""},
  { "COS", 0, 0, 0, ""},
  { "COT", 0, 0, 0, ""},
  { "CRC32", 0, 0, 0, ""},
  { "CROSSES", 0, 0, 0, ""},
  { "CURDATE", 0, 0, 0, ""},
  { "CURTIME", 0, 0, 0, ""},
  { "DATE_ADD", 0, 0, 0, ""},
  { "DATEDIFF", 0, 0, 0, ""},
  { "DATE_FORMAT", 0, 0, 0, ""},
  { "DATE_SUB", 0, 0, 0, ""},
  { "DAYNAME", 0, 0, 0, ""},
  { "DAYOFMONTH", 0, 0, 0, ""},
  { "DAYOFWEEK", 0, 0, 0, ""},
  { "DAYOFYEAR", 0, 0, 0, ""},
  { "DECODE", 0, 0, 0, ""},
  { "DEGREES", 0, 0, 0, ""},
  { "DES_ENCRYPT", 0, 0, 0, ""},
  { "DES_DECRYPT", 0, 0, 0, ""},
  { "DIMENSION", 0, 0, 0, ""},
  { "DISJOINT", 0, 0, 0, ""},
  { "ELT", 0, 0, 0, ""},
  { "ENCODE", 0, 0, 0, ""},
  { "ENCRYPT", 0, 0, 0, ""},
  { "ENDPOINT", 0, 0, 0, ""},
  { "ENVELOPE", 0, 0, 0, ""},
  { "EQUALS", 0, 0, 0, ""},
  { "EXTERIORRING", 0, 0, 0, ""},
  { "EXTRACT", 0, 0, 0, ""},
  { "EXP", 0, 0, 0, ""},
  { "EXPORT_SET", 0, 0, 0, ""},
  { "FIELD", 0, 0, 0, ""},
  { "FIND_IN_SET", 0, 0, 0, ""},
  { "FLOOR", 0, 0, 0, ""},
  { "FORMAT", 0, 0, 0, ""},
  { "FOUND_ROWS", 0, 0, 0, ""},
  { "FROM_DAYS", 0, 0, 0, ""},
  { "FROM_UNIXTIME", 0, 0, 0, ""},
  { "GET_LOCK", 0, 0, 0, ""},
  { "GEOMETRYN", 0, 0, 0, ""},
  { "GEOMETRYTYPE", 0, 0, 0, ""},
  { "GEOMCOLLFROMTEXT", 0, 0, 0, ""},
  { "GEOMCOLLFROMWKB", 0, 0, 0, ""},
  { "GEOMETRYCOLLECTIONFROMTEXT", 0, 0, 0, ""},
  { "GEOMETRYCOLLECTIONFROMWKB", 0, 0, 0, ""},
  { "GEOMETRYFROMTEXT", 0, 0, 0, ""},
  { "GEOMETRYFROMWKB", 0, 0, 0, ""},
  { "GEOMFROMTEXT", 0, 0, 0, ""},
  { "GEOMFROMWKB", 0, 0, 0, ""},
  { "GLENGTH", 0, 0, 0, ""},
  { "GREATEST", 0, 0, 0, ""},
  { "GROUP_CONCAT", 0, 0, 0, ""},
  { "GROUP_UNIQUE_USERS", 0, 0, 0, ""},
  { "HEX", 0, 0, 0, ""},
  { "IFNULL", 0, 0, 0, ""},
  { "INET_ATON", 0, 0, 0, ""},
  { "INET_NTOA", 0, 0, 0, ""},
  { "INSTR", 0, 0, 0, ""},
  { "INTERIORRINGN", 0, 0, 0, ""},
  { "INTERSECTS", 0, 0, 0, ""},
  { "ISCLOSED", 0, 0, 0, ""},
  { "ISEMPTY", 0, 0, 0, ""},
  { "ISNULL", 0, 0, 0, ""},
  { "IS_FREE_LOCK", 0, 0, 0, ""},
  { "IS_USED_LOCK", 0, 0, 0, ""},
  { "JSON_ARRAY_APPEND", 0, 0, 0, ""},
  { "JSON_ARRAY", 0, 0, 0, ""},
  { "JSON_CONTAINS", 0, 0, 0, ""},
  { "JSON_DEPTH", 0, 0, 0, ""},
  { "JSON_EXTRACT", 0, 0, 0, ""},
  { "JSON_INSERT", 0, 0, 0, ""},
  { "JSON_KEYS", 0, 0, 0, ""},
  { "JSON_LENGTH", 0, 0, 0, ""},
  { "JSON_MERGE", 0, 0, 0, ""},
  { "JSON_QUOTE", 0, 0, 0, ""},
  { "JSON_REPLACE", 0, 0, 0, ""},
  { "JSON_ROWOBJECT", 0, 0, 0, ""},
  { "JSON_SEARCH", 0, 0, 0, ""},
  { "JSON_SET", 0, 0, 0, ""},
  { "JSON_TYPE", 0, 0, 0, ""},
  { "JSON_UNQUOTE", 0, 0, 0, ""},
  { "JSON_VALID", 0, 0, 0, ""},
  { "JSON_CONTAINS_PATH", 0, 0, 0, ""},
  { "LAST_INSERT_ID", 0, 0, 0, ""},
  { "ISSIMPLE", 0, 0, 0, ""},
  { "LAST_DAY", 0, 0, 0, ""},
  { "LCASE", 0, 0, 0, ""},
  { "LEAST", 0, 0, 0, ""},
  { "LENGTH", 0, 0, 0, ""},
  { "LN", 0, 0, 0, ""},
  { "LINEFROMTEXT", 0, 0, 0, ""},
  { "LINEFROMWKB", 0, 0, 0, ""},
  { "LINESTRINGFROMTEXT", 0, 0, 0, ""},
  { "LINESTRINGFROMWKB", 0, 0, 0, ""},
  { "LOAD_FILE", 0, 0, 0, ""},
  { "LOCATE", 0, 0, 0, ""},
  { "LOG", 0, 0, 0, ""},
  { "LOG2", 0, 0, 0, ""},
  { "LOG10", 0, 0, 0, ""},
  { "LOWER", 0, 0, 0, ""},
  { "LPAD", 0, 0, 0, ""},
  { "LTRIM", 0, 0, 0, ""},
  { "MAKE_SET", 0, 0, 0, ""},
  { "MAKEDATE", 0, 0, 0, ""},
  { "MAKETIME", 0, 0, 0, ""},
  { "MASTER_POS_WAIT", 0, 0, 0, ""},
  { "MAX", 0, 0, 0, ""},
  { "MBRCONTAINS", 0, 0, 0, ""},
  { "MBRDISJOINT", 0, 0, 0, ""},
  { "MBREQUAL", 0, 0, 0, ""},
  { "MBRINTERSECTS", 0, 0, 0, ""},
  { "MBROVERLAPS", 0, 0, 0, ""},
  { "MBRTOUCHES", 0, 0, 0, ""},
  { "MBRWITHIN", 0, 0, 0, ""},
  { "MD5", 0, 0, 0, ""},
  { "MID", 0, 0, 0, ""},
  { "MIN", 0, 0, 0, ""},
  { "MLINEFROMTEXT", 0, 0, 0, ""},
  { "MLINEFROMWKB", 0, 0, 0, ""},
  { "MPOINTFROMTEXT", 0, 0, 0, ""},
  { "MPOINTFROMWKB", 0, 0, 0, ""},
  { "MPOLYFROMTEXT", 0, 0, 0, ""},
  { "MPOLYFROMWKB", 0, 0, 0, ""},
  { "MONTHNAME", 0, 0, 0, ""},
  { "MULTILINESTRINGFROMTEXT", 0, 0, 0, ""},
  { "MULTILINESTRINGFROMWKB", 0, 0, 0, ""},
  { "MULTIPOINTFROMTEXT", 0, 0, 0, ""},
  { "MULTIPOINTFROMWKB", 0, 0, 0, ""},
  { "MULTIPOLYGONFROMTEXT", 0, 0, 0, ""},
  { "MULTIPOLYGONFROMWKB", 0, 0, 0, ""},
  { "NAME_CONST", 0, 0, 0, ""},
  { "NOW", 0, 0, 0, ""},
  { "NULLIF", 0, 0, 0, ""},
  { "NUMGEOMETRIES", 0, 0, 0, ""},
  { "NUMINTERIORRINGS", 0, 0, 0, ""},
  { "NUMPOINTS", 0, 0, 0, ""},
  { "OCTET_LENGTH", 0, 0, 0, ""},
  { "OCT", 0, 0, 0, ""},
  { "ORD", 0, 0, 0, ""},
  { "OVERLAPS", 0, 0, 0, ""},
  { "PERIOD_ADD", 0, 0, 0, ""},
  { "PERIOD_DIFF", 0, 0, 0, ""},
  { "PI", 0, 0, 0, ""},
  { "POINTFROMTEXT", 0, 0, 0, ""},
  { "POINTFROMWKB", 0, 0, 0, ""},
  { "POINTN", 0, 0, 0, ""},
  { "POLYFROMTEXT", 0, 0, 0, ""},
  { "POLYFROMWKB", 0, 0, 0, ""},
  { "POLYGONFROMTEXT", 0, 0, 0, ""},
  { "POLYGONFROMWKB", 0, 0, 0, ""},
  { "POSITION", 0, 0, 0, ""},
  { "POW", 0, 0, 0, ""},
  { "POWER", 0, 0, 0, ""},
  { "QUOTE", 0, 0, 0, ""},
  { "RADIANS", 0, 0, 0, ""},
  { "RAND", 0, 0, 0, ""},
  { "RELEASE_LOCK", 0, 0, 0, ""},
  { "REVERSE", 0, 0, 0, ""},
  { "ROUND", 0, 0, 0, ""},
  { "ROW_COUNT", 0, 0, 0, ""},
  { "RPAD", 0, 0, 0, ""},
  { "RTRIM", 0, 0, 0, ""},
  { "SEC_TO_TIME", 0, 0, 0, ""},
  { "SESSION_USER", 0, 0, 0, ""},
  { "SUBDATE", 0, 0, 0, ""},
  { "SIGN", 0, 0, 0, ""},
  { "SIN", 0, 0, 0, ""},
  { "SHA", 0, 0, 0, ""},
  { "SHA1", 0, 0, 0, ""},
  { "SLEEP", 0, 0, 0, ""},
  { "SOUNDEX", 0, 0, 0, ""},
  { "SPACE", 0, 0, 0, ""},
  { "SQRT", 0, 0, 0, ""},
  { "SRID", 0, 0, 0, ""},
  { "STARTPOINT", 0, 0, 0, ""},
  { "STD", 0, 0, 0, ""},
  { "STDDEV", 0, 0, 0, ""},
  { "STDDEV_POP", 0, 0, 0, ""},
  { "STDDEV_SAMP", 0, 0, 0, ""},
  { "STR_TO_DATE", 0, 0, 0, ""},
  { "STRCMP", 0, 0, 0, ""},
  { "SUBSTR", 0, 0, 0, ""},
  { "SUBSTRING", 0, 0, 0, ""},
  { "SUBSTRING_INDEX", 0, 0, 0, ""},
  { "SUBTIME", 0, 0, 0, ""},
  { "SUM", 0, 0, 0, ""},
  { "SYSDATE", 0, 0, 0, ""},
  { "SYSTEM_USER", 0, 0, 0, ""},
  { "TAN", 0, 0, 0, ""},
  { "TIME_FORMAT", 0, 0, 0, ""},
  { "TIME_TO_SEC", 0, 0, 0, ""},
  { "TIMEDIFF", 0, 0, 0, ""},
  { "TO_DAYS", 0, 0, 0, ""},
  { "TOUCHES", 0, 0, 0, ""},
  { "TRIM", 0, 0, 0, ""},
  { "UCASE", 0, 0, 0, ""},
  { "UNCOMPRESS", 0, 0, 0, ""},
  { "UNCOMPRESSED_LENGTH", 0, 0, 0, ""},
  { "UNHEX", 0, 0, 0, ""},
  { "UNIQUE_USERS", 0, 0, 0, ""},
  { "UNIX_TIMESTAMP", 0, 0, 0, ""},
  { "UPPER", 0, 0, 0, ""},
  { "UUID", 0, 0, 0, ""},
  { "VARIANCE", 0, 0, 0, ""},
  { "VAR_POP", 0, 0, 0, ""},
  { "VAR_SAMP", 0, 0, 0, ""},
  { "VERSION", 0, 0, 0, ""},
  { "WEEKDAY", 0, 0, 0, ""},
  { "WEEKOFYEAR", 0, 0, 0, ""},
  { "WITHIN", 0, 0, 0, ""},
  { "X", 0, 0, 0, ""},
  { "Y", 0, 0, 0, ""},
  { "YEARWEEK", 0, 0, 0, ""},
  /* end sentinel */
  { (char *)NULL,       0, 0, 0, ""}
};



void putsln(const char *s, FILE *file)
{
  fputs(s, file);
  putc('\n', file);
}

/*
  Number of quotes present in the command's argument.
*/
static int
get_quote_count(const char *line)
{
  int quote_count= 0;
  const char *quote= line;

  while ((quote= strpbrk(quote, "'`\"")) != NULL) {
    quote_count++;
    quote++;
  }

  return quote_count;
}


/*
  Functions related to readline
*/

static bool
init_line_buffer(LINE_BUFFER *buffer,File file,ulong size,ulong max_buffer)
{
  buffer->file=file;
  buffer->bufread=size;
  buffer->max_size=max_buffer;
  if (!(buffer->buffer = (char*) my_malloc(PSI_NOT_INSTRUMENTED,
                                           buffer->bufread+1,
					   MYF(MY_WME | MY_FAE))))
    return 1;
  buffer->end_of_line=buffer->end=buffer->buffer;
  buffer->buffer[0]=0;				/* For easy start test */
  return 0;
}

/*
  Fill the buffer retaining the last n bytes at the beginning of the
  newly filled buffer (for backward context).	Returns the number of new
  bytes read from disk.
*/
static size_t fill_buffer(LINE_BUFFER *buffer)
{
  size_t read_count;
  uint bufbytes= (uint) (buffer->end - buffer->start_of_line);

  if (buffer->eof)
    return 0;					/* Everything read */

  /* See if we need to grow the buffer. */

  for (;;)
  {
    uint start_offset=(uint) (buffer->start_of_line - buffer->buffer);
    read_count=(buffer->bufread - bufbytes)/IO_SIZE;
    if ((read_count*=IO_SIZE))
      break;
    if (buffer->bufread * 2 > buffer->max_size)
    {
      /*
        So we must grow the buffer but we cannot due to the max_size limit.
        Return 0 w/o setting buffer->eof to signal this condition.
      */
      return 0;
    }
    buffer->bufread *= 2;
    if (!(buffer->buffer = (char*) my_realloc(PSI_NOT_INSTRUMENTED,
                                              buffer->buffer,
					      buffer->bufread+1,
					      MYF(MY_WME | MY_FAE))))
    {
      buffer->error= my_errno();
      return (size_t) -1;
    }
    buffer->start_of_line=buffer->buffer+start_offset;
    buffer->end=buffer->buffer+bufbytes;
  }

  /* Shift stuff down. */
  if (buffer->start_of_line != buffer->buffer)
  {
    memmove(buffer->buffer, buffer->start_of_line, bufbytes);
    buffer->end=buffer->buffer+bufbytes;
  }

  /* Read in new stuff. */
  if ((read_count= my_read(buffer->file, (uchar*) buffer->end, read_count,
			   MYF(MY_WME))) == MY_FILE_ERROR)
  {
    buffer->error= my_errno();
    return (size_t) -1;
  }

  DBUG_PRINT("fill_buff", ("Got %lu bytes", (ulong) read_count));

  if (!read_count)
  {
    buffer->eof = 1;
    /* Kludge to pretend every nonempty file ends with a newline. */
    if (bufbytes && buffer->end[-1] != '\n')
    {
      read_count = 1;
      *buffer->end = '\n';
    }
  }
  buffer->end_of_line=(buffer->start_of_line=buffer->buffer)+bufbytes;
  buffer->end+=read_count;
  *buffer->end=0;				/* Sentinel */
  return read_count;
}

char *intern_read_line(LINE_BUFFER *buffer, ulong *out_length)
{
  char *pos;
  size_t length;
  DBUG_ENTER("intern_read_line");

  buffer->start_of_line=buffer->end_of_line;
  for (;;)
  {
    pos=buffer->end_of_line;
    while (*pos != '\n' && pos != buffer->end)
      pos++;
    if (pos == buffer->end)
    {
      /*
        fill_buffer() can return NULL on EOF (in which case we abort),
        on error, or when the internal buffer has hit the size limit.
        In the latter case return what we have read so far and signal
        string truncation.
      */
      if (!(length= fill_buffer(buffer)))
      {
        if (buffer->eof)
          DBUG_RETURN(0);
      }
      else if (length == (size_t) -1)
        DBUG_RETURN(NULL);
      else
        continue;
      pos--;					/* break line here */
      buffer->truncated= 1;
    }
    else
      buffer->truncated= 0;
    buffer->end_of_line=pos+1;
    *out_length=(ulong) (pos + 1 - buffer->eof - buffer->start_of_line);

    DBUG_DUMP("Query: ", (unsigned char *) buffer->start_of_line, *out_length);
    DBUG_RETURN(buffer->start_of_line);
  }
}

LINE_BUFFER *batch_readline_init(ulong max_size, FILE *file)
{
  LINE_BUFFER *line_buff;

#ifndef _WIN32
  MY_STAT input_file_stat;
  if (my_fstat(fileno(file), &input_file_stat, MYF(MY_WME)) ||
      MY_S_ISDIR(input_file_stat.st_mode) ||
      MY_S_ISBLK(input_file_stat.st_mode))
    return 0;
#endif

  if (!(line_buff=(LINE_BUFFER*)
        my_malloc(PSI_NOT_INSTRUMENTED,
                  sizeof(*line_buff),MYF(MY_WME | MY_ZEROFILL))))
    return 0;
  if (init_line_buffer(line_buff,my_fileno(file),IO_SIZE,max_size))
  {
    my_free(line_buff);
    return 0;
  }
  return line_buff;
}

char *batch_readline(LINE_BUFFER *line_buff, bool binary_mode)
{
  char *pos;
  ulong out_length;

  if (!(pos=intern_read_line(line_buff, &out_length)))
    return 0;
  if (out_length && pos[out_length-1] == '\n')
  {
#if defined(_WIN32)
    /*
      On Windows platforms we also need to remove '\r', 
      unconditionally.
     */

    /* Remove '\n' */
    if (--out_length && pos[out_length-1] == '\r')  
      /* Remove '\r' */
      out_length--;                                 
#else
    /*
      On Unix-like platforms we only remove it if we are not 
      on binary mode.
     */

    /* Remove '\n' */
    if (--out_length && !binary_mode && pos[out_length-1] == '\r')
      /* Remove '\r' */
      out_length--;                                 
#endif
  }
  line_buff->read_length=out_length;
  pos[out_length]=0;
  DBUG_DUMP("Query: ", (unsigned char *) pos, out_length);
  return pos;
}

void batch_readline_end(LINE_BUFFER *line_buff)
{
  if (line_buff)
  {
    my_free(line_buff->buffer);
    my_free(line_buff);
  }
}

static ulong start_timer(void)
{
#if defined(_WIN32)
  return clock();
#else
  struct tms tms_tmp;
  return times(&tms_tmp);
#endif
}

int Restore_Handler::mysql_real_query_for_lazy(const char *buf, size_t length)
{
  for (uint retry=0;; retry++)
  {
    int error;
    if (!mysql_real_query(&mysql,buf,(ulong)length))
      return 0;
    error= put_error(&mysql);
    if (mysql_errno(&mysql) != CR_SERVER_GONE_ERROR || retry > 1 ||
        !opt_reconnect)
      return error;
    if (reconnect())
      return error;
  }
}

int Restore_Handler::mysql_store_result_for_lazy(MYSQL_RES **result)
{
  if ((*result=mysql_store_result(&mysql)))
    return 0;

  if (mysql_error(&mysql)[0])
    return put_error(&mysql);
  return 0;
}

/** 
  Write as many as 52+1 bytes to buff, in the form of a legible duration of time.

  len("4294967296 days, 23 hours, 59 minutes, 60.00 seconds")  ->  52
*/
static void nice_time(double sec,char *buff,bool part_second)
{
  ulong tmp;
  if (sec >= 3600.0*24)
  {
    tmp=(ulong) floor(sec/(3600.0*24));
    sec-=3600.0*24*tmp;
    buff=int10_to_str((long) tmp, buff, 10);
    buff=my_stpcpy(buff,tmp > 1 ? " days " : " day ");
  }
  if (sec >= 3600.0)
  {
    tmp=(ulong) floor(sec/3600.0);
    sec-=3600.0*tmp;
    buff=int10_to_str((long) tmp, buff, 10);
    buff=my_stpcpy(buff,tmp > 1 ? " hours " : " hour ");
  }
  if (sec >= 60.0)
  {
    tmp=(ulong) floor(sec/60.0);
    sec-=60.0*tmp;
    buff=int10_to_str((long) tmp, buff, 10);
    buff=my_stpcpy(buff," min ");
  }
  if (part_second)
    sprintf(buff,"%.2f sec",sec);
  else
    sprintf(buff,"%d sec",(int) sec);
}

static void end_timer(ulong start_time,char *buff)
{
  nice_time((double) (start_timer() - start_time) /
	    CLOCKS_PER_SEC,buff,1);
}

static void mysql_end_timer(ulong start_time,char *buff)
{
  buff[0]=' ';
  buff[1]='(';
  end_timer(start_time,buff+2);
  my_stpcpy(strend(buff),")");
}

/* Used to determine if we should invoke print_as_hex for this field */

static bool
is_binary_field(MYSQL_FIELD *field)
{
  if ((field->charsetnr == 63) &&
      (field->type == MYSQL_TYPE_BIT ||
       field->type == MYSQL_TYPE_BLOB ||
       field->type == MYSQL_TYPE_LONG_BLOB ||
       field->type == MYSQL_TYPE_MEDIUM_BLOB ||
       field->type == MYSQL_TYPE_TINY_BLOB ||
       field->type == MYSQL_TYPE_VAR_STRING ||
       field->type == MYSQL_TYPE_STRING ||
       field->type == MYSQL_TYPE_VARCHAR ||
       field->type == MYSQL_TYPE_GEOMETRY))
    return 1;
  return 0;
}

static const char *array_value(const char **array, char key)
{
  for (; *array; array+= 2)
    if (**array == key)
      return array[1];
  return 0;
}

static const char *fieldtype2str(enum enum_field_types type)
{
  switch (type) {
    case MYSQL_TYPE_BIT:         return "BIT";
    case MYSQL_TYPE_BLOB:        return "BLOB";
    case MYSQL_TYPE_DATE:        return "DATE";
    case MYSQL_TYPE_DATETIME:    return "DATETIME";
    case MYSQL_TYPE_NEWDECIMAL:  return "NEWDECIMAL";
    case MYSQL_TYPE_DECIMAL:     return "DECIMAL";
    case MYSQL_TYPE_DOUBLE:      return "DOUBLE";
    case MYSQL_TYPE_ENUM:        return "ENUM";
    case MYSQL_TYPE_FLOAT:       return "FLOAT";
    case MYSQL_TYPE_GEOMETRY:    return "GEOMETRY";
    case MYSQL_TYPE_INT24:       return "INT24";
    case MYSQL_TYPE_JSON:        return "JSON";
    case MYSQL_TYPE_LONG:        return "LONG";
    case MYSQL_TYPE_LONGLONG:    return "LONGLONG";
    case MYSQL_TYPE_LONG_BLOB:   return "LONG_BLOB";
    case MYSQL_TYPE_MEDIUM_BLOB: return "MEDIUM_BLOB";
    case MYSQL_TYPE_NEWDATE:     return "NEWDATE";
    case MYSQL_TYPE_NULL:        return "NULL";
    case MYSQL_TYPE_SET:         return "SET";
    case MYSQL_TYPE_SHORT:       return "SHORT";
    case MYSQL_TYPE_STRING:      return "STRING";
    case MYSQL_TYPE_TIME:        return "TIME";
    case MYSQL_TYPE_TIMESTAMP:   return "TIMESTAMP";
    case MYSQL_TYPE_TINY:        return "TINY";
    case MYSQL_TYPE_TINY_BLOB:   return "TINY_BLOB";
    case MYSQL_TYPE_VAR_STRING:  return "VAR_STRING";
    case MYSQL_TYPE_YEAR:        return "YEAR";
    default:                     return "?-unknown-?";
  }
}

static char *fieldflags2str(uint f) {
  static char buf[1024];
  char *s=buf;
  *s=0;
#define ff2s_check_flag(X) \
                if (f & X ## _FLAG) { s=my_stpcpy(s, # X " "); f &= ~ X ## _FLAG; }
  ff2s_check_flag(NOT_NULL);
  ff2s_check_flag(PRI_KEY);
  ff2s_check_flag(UNIQUE_KEY);
  ff2s_check_flag(MULTIPLE_KEY);
  ff2s_check_flag(BLOB);
  ff2s_check_flag(UNSIGNED);
  ff2s_check_flag(ZEROFILL);
  ff2s_check_flag(BINARY);
  ff2s_check_flag(ENUM);
  ff2s_check_flag(AUTO_INCREMENT);
  ff2s_check_flag(TIMESTAMP);
  ff2s_check_flag(SET);
  ff2s_check_flag(NO_DEFAULT_VALUE);
  ff2s_check_flag(NUM);
  ff2s_check_flag(PART_KEY);
  ff2s_check_flag(GROUP);
  ff2s_check_flag(UNIQUE);
  ff2s_check_flag(BINCMP);
  ff2s_check_flag(ON_UPDATE_NOW);
#undef ff2s_check_flag
  if (f)
    sprintf(s, " unknows=0x%04x", f);
  return buf;
}


/*
  Functions related to Restore_Handler
*/

/**
   Get the index of a command in the commands array.

   @param cmd_char    Short form command.

   @return int
     The index of the command is returned if it is found, else -1 is returned.
*/
inline int Restore_Handler::get_command_index(char cmd_char)
{
  /*
    All client-specific commands are in the first part of commands array
    and have a function to implement it.
  */
  for (uint i= 0; commands[i].func != NULL; i++)
    if (commands[i].cmd_char == cmd_char)
      return i;
  return -1;
}

void Restore_Handler::print_help_item(MYSQL_ROW *cur, int num_name, 
                                      int num_cat, char *last_char)
{
  char ccat= (*cur)[num_cat][0];
  if (*last_char != ccat)
  {
    put_info(ccat == 'Y' ? "categories:" : "topics:", INFO_INFO);
    *last_char= ccat;
  }
  fprintf(output_file, "   %s\n", (*cur)[num_name]);
}

int Restore_Handler::com_server_help(
                        String *buffer MY_ATTRIBUTE((unused)), 
                        char *line MY_ATTRIBUTE((unused)), char *help_arg)
{
  MYSQL_ROW cur;
  const char *server_cmd;
  char cmd_buf[100 + 1];
  MYSQL_RES *result;
  int error;
  
  if (help_arg[0] != '\'')
  {
    char *end_arg= strend(help_arg);
    if(--end_arg)
    {
      while (my_isspace(charset_info,*end_arg))
            end_arg--;
      *++end_arg= '\0';
    }
    (void) strxnmov(cmd_buf, sizeof(cmd_buf), "help '", help_arg, "'", NullS);
  }
  else
    (void) strxnmov(cmd_buf, sizeof(cmd_buf), "help ", help_arg, NullS);

  server_cmd= cmd_buf;

  if (!connected && reconnect())
    return 1;

  if ((error= mysql_real_query_for_lazy(server_cmd,(int)strlen(server_cmd))) ||
      (error= mysql_store_result_for_lazy(&result)))
    return error;

  if (result)
  {
    unsigned int num_fields= mysql_num_fields(result);
    my_ulonglong num_rows= mysql_num_rows(result);
    if (num_fields==3 && num_rows==1)
    {
      if (!(cur= mysql_fetch_row(result)))
      {
        error= -1;
        goto err;
      }

      fprintf(output_file,   "Name: \'%s\'\n", cur[0]);
      fprintf(output_file,   "Description:\n%s", cur[1]);
      if (cur[2] && *((char*)cur[2]))
	      fprintf(output_file, "Examples:\n%s", cur[2]);
      fprintf(output_file,   "\n");
    }
    else if (num_fields >= 2 && num_rows)
    {
      char last_char= 0;

      int num_name= 0, num_cat= 0;

      if (num_fields == 2)
      {
        put_info("Many help items for your request exist.", INFO_INFO);
        put_info("To make a more specific request, please type 'help <item>',\nwhere <item> is one of the following", INFO_INFO);
        num_name= 0;
        num_cat= 1;
      }
      else if ((cur= mysql_fetch_row(result)))
      {
        fprintf(output_file, "You asked for help about help category: \"%s\"\n", cur[0]);
        put_info("For more information, type 'help <item>', where <item> is one of the following", INFO_INFO);
        num_name= 1;
        num_cat= 2;
        print_help_item(&cur,1,2,&last_char);
      }

      while ((cur= mysql_fetch_row(result)))
	      print_help_item(&cur,num_name,num_cat,&last_char);
      fprintf(output_file, "\n");
    }
    else
    {
      put_info("\nNothing found", INFO_INFO);
      if (native_strncasecmp(server_cmd, "help 'contents'", 15) == 0)
      {
         put_info("\nPlease check if 'help tables' are loaded.\n", INFO_INFO); 
         goto err;
      }
      put_info("Please try to run 'help contents' for a list of all accessible topics\n", INFO_INFO);
    }
  }

err:
  mysql_free_result(result);
  return error;
}

int Restore_Handler::com_help(String *buffer MY_ATTRIBUTE((unused)),
                              char *line MY_ATTRIBUTE((unused)))
{
  int i, j;
  char * help_arg= strchr(line,' '), buff[32], *end;
  if (help_arg)
  {
    while (my_isspace(charset_info,*help_arg))
      help_arg++;
    if (*help_arg)	  
      return com_server_help(buffer,line,help_arg);
  }

  put_info("\nFor information about Percona products and services, visit:\n"
           "   http://www.percona.com/\n"
	   "Percona Server manual: http://www.percona.com/doc/percona-server/5.6\n"
           "For the MySQL Reference Manual: http://dev.mysql.com/\n"
           "To buy Percona support, training, or other products, visit:\n"
           "   https://www.percona.com/\n", INFO_INFO);
  put_info("List of all MySQL commands:", INFO_INFO);
  if (!named_cmds)
    put_info("Note that all text commands must be first on line and end with ';'",INFO_INFO);
  for (i = 0; commands[i].name; i++)
  {
    end= my_stpcpy(buff, commands[i].name);
    for (j= (int)strlen(commands[i].name); j < 10; j++)
      end= my_stpcpy(end, " ");
    if (commands[i].func)
      fprintf(output_file, "%s(\\%c) %s\n", buff,
		          commands[i].cmd_char, commands[i].doc);
  }
  if (connected && mysql_get_server_version(&mysql) >= 40100)
    put_info("\nFor server side help, type 'help contents'\n", INFO_INFO);
  return 0;
}

int Restore_Handler::com_clear(String *buffer,
                              char *line MY_ATTRIBUTE((unused)))
{
  buffer->length(0);
  return 0;
}

/*
  Gets argument from a command on the command line. If get_next_arg is
  not defined, skips the command and returns the first argument. The
  line is modified by adding zero to the end of the argument. If
  get_next_arg is defined, then the function searches for end of string
  first, after found, returns the next argument and adds zero to the
  end. If you ever wish to use this feature, remember to initialize all
  items in the array to zero first.
*/
char *Restore_Handler::get_arg(char *line, my_bool get_next_arg)
{
  char *ptr, *start;
  my_bool quoted= 0, valid_arg= 0;
  char qtype= 0;

  ptr= line;
  if (get_next_arg)
  {
    for (; *ptr; ptr++) ;
    if (*(ptr + 1))
      ptr++;
  }
  else
  {
    /* skip leading white spaces */
    while (my_isspace(charset_info, *ptr))
      ptr++;
    if (*ptr == '\\') // short command was used
      ptr+= 2;
    else
      while (*ptr &&!my_isspace(charset_info, *ptr)) // skip command
        ptr++;
  }
  if (!*ptr)
    return NullS;
  while (my_isspace(charset_info, *ptr))
    ptr++;
  if (*ptr == '\'' || *ptr == '\"' || *ptr == '`')
  {
    qtype= *ptr;
    quoted= 1;
    ptr++;
  }
  for (start=ptr ; *ptr; ptr++)
  {
    // if it is a quoted string do not remove backslash
    if (!quoted && *ptr == '\\' && ptr[1]) // escaped character
    {
      // Remove the backslash
      my_stpmov(ptr, ptr+1);
    }
    else if ((!quoted && *ptr == ' ') || (quoted && *ptr == qtype))
    {
      *ptr= 0;
      break;
    }
  }
  valid_arg= ptr != start;
  return valid_arg ? start : NullS;
}

int Restore_Handler::com_connect(String *buffer, char *line)
{
  char *tmp, buff[256];
  bool save_rehash= opt_rehash;
  int error;

  memset(buff, 0, sizeof(buff));

  if (buffer)
  {
    /*
      Two null bytes are needed in the end of buff to allow
      get_arg to find end of string the second time it's called.
    */
    tmp= strmake(buff, line, sizeof(buff)-2);
#ifdef EXTRA_DEBUG
    tmp[1]= 0;
#endif
    tmp= get_arg(buff, 0);
    if (tmp && *tmp)
    {
      current_db = tmp;

      tmp= get_arg(buff, 1);
      if (tmp)
      {
        current_host = tmp;
      }
    }
    else
    {
      /* Quick re-connect */
      opt_rehash= 0;                            /* purecov: tested */
    }
    buffer->length(0);				// command used
  }
  else
    opt_rehash= 0;

  error = sql_real_connect(current_host.c_str(),
                       server_options.get_port(),
                       current_user.c_str(),
                       server_options.get_password(),
                       current_db.c_str());

  opt_rehash= save_rehash;

  if (connected)
  {
    sprintf(buff,"Connection id:    %lu", mysql.thread_id);
    put_info(buff,INFO_INFO);
    sprintf(buff,"Current database: %.128s\n",
	    current_db.empty() ? "*** NONE ***" : current_db.c_str());
    put_info(buff,INFO_INFO);
  }
  return error;
}

int Restore_Handler::com_delimiter(String *buffer MY_ATTRIBUTE((unused)),
                                  char *line)
{
  char buff[256], *tmp;

  strmake(buff, line, sizeof(buff) - 1);
  tmp= get_arg(buff, 0);

  if (!tmp || !*tmp)
  {
    put_info("DELIMITER must be followed by a 'delimiter' character or string",
	     INFO_ERROR);
    return 0;
  }
  else
  {
    if (strstr(tmp, "\\")) 
    {
      put_info("DELIMITER cannot contain a backslash character", 
              INFO_ERROR);
      return 0;
    }
  }
  strmake(delimiter, tmp, sizeof(delimiter) - 1);
  return 0;
}

int Restore_Handler::com_edit(String *buffer,char *line MY_ATTRIBUTE((unused)))
{
  put_info("EDIT command is not supported for Restore Handler", 
          INFO_ERROR);
  return 0;
}

int Restore_Handler::com_ego(String *buffer,char *line)
{
  int result;
  bool oldvertical=vertical;
  vertical=1;
  result=com_go(buffer,line);
  vertical=oldvertical;
  return result;
}

/* If arg is given, exit without errors. This happens on command 'quit' */
int Restore_Handler::com_quit(String *buffer MY_ATTRIBUTE((unused)),
	                            char *line MY_ATTRIBUTE((unused)))
{
  status.exit_status=0;
  return 1;
}

int Restore_Handler::com_go(String *buffer,
                        char *line MY_ATTRIBUTE((unused)))
{
  char		buff[200]; /* about 110 chars used so far */
  char		time_buff[52+3+1]; /* time max + space&parens + NUL */
  MYSQL_RES	*result;
  ulong		timer, warnings= 0;
  uint		error= 0;
  int           err= 0;

  interrupted_query= 0;

  /* Remove garbage for nicer messages */
  buff[0]= 0;
  remove_cntrl(*buffer);

  if (buffer->is_empty())
  {
    return 0;
  }
  if (!connected && reconnect())
  {
    buffer->length(0);				// Remove query on error
    return opt_reconnect ? -1 : 1;          // Fatal error
  }
  if (verbose)
    (void) com_print(buffer,0);

  if (skip_updates &&
      (buffer->length() < 4 || my_strnncoll(charset_info,
					    (const uchar*)buffer->ptr(),4,
					    (const uchar*)"SET ",4)))
  {
    (void) put_info("Ignoring query to other database",INFO_INFO);
    return 0;
  }

  timer=start_timer();
  executing_query= 1;
  error= mysql_real_query_for_lazy(buffer->ptr(),buffer->length());

  buffer->length(0);

  if (error)
    goto end;

  do
  {
    char *pos;
    bool batchmode= (verbose <= 1) ? TRUE : FALSE;
    buff[0]= 0;

    if (quick)
    {
      if (!(result=client_mysql_use_result(&mysql)) && mysql.field_count)
      {
        error= put_error(&mysql);
        goto end;
      }
    }
    else
    {
      error= mysql_store_result_for_lazy(&result);
      if (error)
        goto end;
    }

    if (verbose >= 3 || !opt_silent)
      mysql_end_timer(timer,time_buff);
    else
      time_buff[0]= '\0';

    /* Every branch must truncate  buff . */
    if (result)
    {
      if (!mysql_num_rows(result) && !quick && !column_types_flag)
      {
	      my_stpcpy(buff, "Empty set");
      }
      else
      {
        if (vertical) {
          print_table_data_vertically(result);
        }
        else {
          if (opt_silent && verbose <= 2 && !output_tables)
            print_tab_data(result);
          else
            print_table_data(result);
        }
        if(!batchmode)
          sprintf(buff,"%lld %s in set",
                  mysql_num_rows(result),
                  mysql_num_rows(result) == 1LL ? "row" : "rows");
        
        if (mysql_errno(&mysql))
          error= put_error(&mysql);
      }
    }
    else if (mysql.affected_rows == ~(ulonglong) 0)
      my_stpcpy(buff,"Query OK");
    else if(!batchmode)
      sprintf(buff,"Query OK, %lld %s affected",
	      mysql.affected_rows,
	      mysql.affected_rows == 1LL ? "row" : "rows");

    pos=strend(buff);
    if ((warnings= mysql.warning_count) && !batchmode)
    {
      *pos++= ',';
      *pos++= ' ';
      pos=int10_to_str(warnings, pos, 10);
      pos=my_stpcpy(pos, " warning");
      if (warnings != 1)
	      *pos++= 's';
    }
    my_stpcpy(pos, time_buff);
    put_info(buff,INFO_RESULT);
    if (mysql.info)
      put_info(mysql.info,INFO_RESULT);
    put_info("",INFO_RESULT);			// Empty row

    if (result && !(result->eof))	/* Something wrong when using quick */
      error= put_error(&mysql);
    else if (unbuffered)
      fflush(output_file);
    mysql_free_result(result);
  } while (!(err= tc_mysql_next_result(&mysql)));
  if (err >= 1)
    error= put_error(&mysql);

end:

 /* Show warnings if any or error occured */
  if (show_warnings == 1 && (warnings >= 1 || error))
    print_warnings();

  executing_query= 0;
  return error;				/* New command follows */
}

int Restore_Handler::com_nopager(String *buffer MY_ATTRIBUTE((unused)),
                                char *line MY_ATTRIBUTE((unused)))
{
  put_info("NOPAGER command is not supported for Restore Handler", 
          INFO_ERROR);
  return 0;
}

int Restore_Handler::com_notee(String *buffer MY_ATTRIBUTE((unused)),
                              char *line MY_ATTRIBUTE((unused)))
{
  put_info("NOTEE command is not supported for Restore Handler", 
          INFO_ERROR);
  return 0;
}

int Restore_Handler::com_pager(String *buffer MY_ATTRIBUTE((unused)),
                              char *line MY_ATTRIBUTE((unused)))
{
  put_info("PAGER command is not supported for Restore Handler", 
          INFO_ERROR);
  return 0;
}

int Restore_Handler::com_print(String *buffer,char *line MY_ATTRIBUTE((unused)))
{
  putsln("--------------", output_file);
  (void) fputs(buffer->c_ptr(), output_file);
  if (!buffer->length() || (*buffer)[buffer->length()-1] != '\n')
    fputc('\n', output_file);
  putsln("--------------\n", output_file);
  return 0;					/* If empty buffer */
}

int Restore_Handler::com_prompt(String *buffer MY_ATTRIBUTE((unused)),
                                char *line)
{
  put_info("PROMPT command is not supported for Restore Handler", 
          INFO_ERROR);
  return 0;
}

int Restore_Handler::com_rehash(String *buffer MY_ATTRIBUTE((unused)),
	                              char *line MY_ATTRIBUTE((unused)))
{
  put_info("REHASH command is not supported for Restore Handler", 
          INFO_ERROR);
  return 0;
}

int Restore_Handler::com_source(String *buffer MY_ATTRIBUTE((unused)),
                                char *line)
{
  char source_name[FN_REFLEN], *end, *param;
  LINE_BUFFER *line_buff;
  int error;
  STATUS old_status;
  FILE *cur_sql_file;

  /* Skip space from file name */
  while (my_isspace(charset_info,*line))
    line++;
  if (!(param = strchr(line, ' ')))		// Skip command name
    return put_info("Usage: \\. <filename> | source <filename>", 
		    INFO_ERROR, 0);
  while (my_isspace(charset_info,*param))
    param++;
  end=strmake(source_name,param,sizeof(source_name)-1);
  while (end > source_name && (my_isspace(charset_info,end[-1]) || 
                               my_iscntrl(charset_info,end[-1])))
    end--;
  end[0]=0;
  unpack_filename(source_name,source_name);
  /* open file name */
  if (!(cur_sql_file = my_fopen(source_name, O_RDONLY | O_BINARY,MYF(0))))
  {
    char buff[FN_REFLEN+60];
    sprintf(buff,"Failed to open file '%s', error: %d", source_name,errno);
    return put_info(buff, INFO_ERROR, 0);
  }

  if (!(line_buff= batch_readline_init(MAX_BATCH_BUFFER_SIZE, cur_sql_file)))
  {
    my_fclose(cur_sql_file,MYF(0));
    return put_info("Can't initialize batch_readline", INFO_ERROR, 0);
  }

  /* Save old status */
  old_status=status;
  init_status();

  status.line_buff=line_buff;
  status.file_name=source_name;
  glob_buffer.length(0);			// Empty command buffer
  error= read_and_execute();
  status=old_status;				// Continue as before
  my_fclose(cur_sql_file,MYF(0));
  batch_readline_end(line_buff);
  return error;
}

int Restore_Handler::com_status(String *buffer MY_ATTRIBUTE((unused)),
                                char *line MY_ATTRIBUTE((unused)))
{
  put_info("STATUS command is not supported for Restore Handler", 
          INFO_ERROR);
  return 0;
}

int Restore_Handler::com_shell(String *buffer MY_ATTRIBUTE((unused)),
          char *line MY_ATTRIBUTE((unused)))
{
  put_info("SHELL command is not supported for Restore Handler", 
          INFO_ERROR);
  return 0;
}

int Restore_Handler::com_tee(String *buffer MY_ATTRIBUTE((unused)),
                            char *line MY_ATTRIBUTE((unused)))
{
  put_info("TEE command is not supported for Restore Handler", 
          INFO_ERROR);
  return 0;
}

/**
  Normalize database name.

  @param line [IN]          The command.
  @param buff [OUT]         Normalized db name.
  @param buff_size [IN]     Buffer size.

  @return Operation status
      @retval 0    Success
      @retval 1    Failure

  @note Sometimes server normilizes the database names
        & APIs like mysql_select_db() expect normalized
        database names. Since it is difficult to perform
        the name conversion/normalization on the client
        side, this function tries to get the normalized
        dbname (indirectly) from the server.
*/
int Restore_Handler::normalize_dbname(const char *line, char *buff, uint buff_size)
{
  MYSQL_RES *res= NULL;

  /* Send the "USE db" commmand to the server. */
  if (mysql_real_query(&mysql, line, strlen(line)))
    return 1;

  /*
    Now, get the normalized database name and store it
    into the buff.
  */
  const char *select_sql = "SELECT DATABASE()";
  if (!mysql_real_query(&mysql, select_sql, strlen(select_sql)) &&
      (res= client_mysql_use_result(&mysql)))
  {
    MYSQL_ROW row= mysql_fetch_row(res);
    if (row && row[0])
    {
      size_t len= strlen(row[0]);
      /* Make sure there is enough room to store the dbname. */
      if ((len > buff_size) || ! memcpy(buff, row[0], len))
      {
        mysql_free_result(res);
        return 1;
      }
    }
    mysql_free_result(res);
  }

  /* Restore the original database. */
  if (!current_db.empty() && 
      mysql_select_db(&mysql, current_db.c_str()))
    return 1;

  return 0;
}

void Restore_Handler::get_current_db()
{
  MYSQL_RES *res;

  /* If one_database is set, current_db is not supposed to change. */
  if (one_database)
    return;

  current_db = "";
  /* In case of error below current_db will be NULL */
  const char *select_sql = "SELECT DATABASE()";
  if (!mysql_real_query(&mysql, select_sql, strlen(select_sql)) &&
      (res= client_mysql_use_result(&mysql)))
  {
    MYSQL_ROW row= mysql_fetch_row(res);
    if (row && row[0])
      current_db= row[0];
    mysql_free_result(res);
  }
}

int Restore_Handler::com_use(String *buffer MY_ATTRIBUTE((unused)), 
                            char *line)
{
  char *tmp, buff[FN_REFLEN + 1];
  int select_db;
  uint warnings;

  memset(buff, 0, sizeof(buff));

  /*
    In case of quotes used, try to get the normalized db name.
  */
  if (get_quote_count(line) > 0)
  {
    if (normalize_dbname(line, buff, sizeof(buff)))
      return put_error(&mysql);
    tmp= buff;
  }
  else
  {
    strmake(buff, line, sizeof(buff) - 1);
    tmp= get_arg(buff, 0);
  }

  if (!tmp || !*tmp)
  {
    put_info("USE must be followed by a database name", INFO_ERROR);
    return 0;
  }
  /*
    We need to recheck the current database, because it may change
    under our feet, for example if DROP DATABASE or RENAME DATABASE
    (latter one not yet available by the time the comment was written)
  */
  get_current_db();

  if (current_db.empty() || 
      cmp_database(charset_info, current_db.c_str(), tmp))
  {
    if (one_database)
    {
      skip_updates= 1;
      select_db= 0;    // don't do mysql_select_db()
    }
    else
      select_db= 2;    // do mysql_select_db() and build_completion_hash()
  }
  else
  {
    /*
      USE to the current db specified.
      We do need to send mysql_select_db() to make server
      update database level privileges, which might
      change since last USE (see bug#10979).
      For performance purposes, we'll skip rebuilding of completion hash.
    */
    skip_updates= 0;
    select_db= 1;      // do only mysql_select_db(), without completion
  }

  if (select_db)
  {
    /*
      reconnect once if connection is down or if connection was found to
      be down during query
    */
    if (!connected && reconnect())
      return opt_reconnect ? -1 : 1;                        // Fatal error
    if (mysql_select_db(&mysql,tmp))
    {
      if (mysql_errno(&mysql) != CR_SERVER_GONE_ERROR)
        return put_error(&mysql);

      if (reconnect())
        return opt_reconnect ? -1 : 1;                      // Fatal error
      if (mysql_select_db(&mysql,tmp))
        return put_error(&mysql);
    }
    current_db=tmp;
  }


  if (0 < (warnings= mysql.warning_count))
  {
    my_snprintf(buff, sizeof(buff),
                "Database changed, %u warning%s", warnings,
                warnings > 1 ? "s" : "");
    put_info(buff, INFO_INFO);
    if (show_warnings == 1)
      print_warnings();
  }
  else
    put_info("Database changed",INFO_INFO);

  return 0;
}

int Restore_Handler::com_charset(String *buffer MY_ATTRIBUTE((unused)), 
                                char *line)
{
  char buff[256], *param;
  const CHARSET_INFO *new_cs;
  strmake(buff, line, sizeof(buff) - 1);
  param= get_arg(buff, 0);
  if (!param || !*param)
  {
    return put_info("Usage: \\C charset_name | charset charset_name", 
		    INFO_ERROR, 0);
  }
  new_cs= get_charset_by_csname(param, MY_CS_PRIMARY, MYF(MY_WME));
  if (new_cs)
  {
    charset_info= new_cs;
    mysql_set_character_set(&mysql, charset_info->csname);
    default_charset= (char *)charset_info->csname;
    put_info("Charset changed", INFO_INFO);
  }
  else put_info("Charset is not found", INFO_INFO);
  return 0;
}

int Restore_Handler::com_warnings(String *buffer MY_ATTRIBUTE((unused)),
                                  char *line MY_ATTRIBUTE((unused)))
{
  show_warnings = 1;
  put_info("Show warnings enabled.",INFO_INFO);
  return 0;
}

int Restore_Handler::com_nowarnings(String *buffer MY_ATTRIBUTE((unused)),
                                    char *line MY_ATTRIBUTE((unused)))
{
  show_warnings = 0;
  put_info("Show warnings disabled.",INFO_INFO);
  return 0;
}

int Restore_Handler::com_resetconnection(String *buffer MY_ATTRIBUTE((unused)),
                                        char *line MY_ATTRIBUTE((unused)))
{
  put_info("RESETCONNECTION command is not supported for Restore Handler",
          INFO_INFO);
  return 0;
}

Restore_Handler::Restore_Handler(const Server_options &server_options,
                                 const string &sql_file_name,
                                 const string &output_file_name) :
  server_options(server_options),
  sql_file_name(sql_file_name),
  output_file_name(output_file_name),
  delimiter(DEFAULT_DELIMITER)
{
  init_status();
  current_db = server_options.get_db() ? 
              server_options.get_db() : "";
  current_host = server_options.get_host() ? 
              server_options.get_host() : "";
  current_user = server_options.get_username() ? 
              server_options.get_username() : "";
}

void Restore_Handler::init_status()
{
  status.exit_status = 0;
  status.query_start_line = 0;
  status.file_name = NULL;
  status.line_buff = NULL;
  status.batch = true;  // Run in batch mode
  status.add_to_history = false;
}

int Restore_Handler::put_info(const char *str,
                              INFO_TYPE info_type, 
                              uint error, 
                              const char *sqlstate)
{
  FILE *file = output_file;

  if (info_type == INFO_ERROR)
  {
    (void) fflush(file);
    fprintf(file,"ERROR");
    if (error)
    {
      if (sqlstate)
        (void) fprintf(file," %d (%s)", error, sqlstate);
      else
        (void) fprintf(file," %d",error);
    }
    if (status.query_start_line)
    {
      (void) fprintf(file," at line %lu", status.query_start_line);
      if (status.file_name)
        (void) fprintf(file," in file: '%s'", status.file_name);
    }
    (void) fprintf(file, ": %s\n", str);
    (void) fflush(file);
    return 1;
  }
  else 
    if (info_type == INFO_RESULT && verbose > 1)
      putsln(str, file);
  
  return info_type == INFO_ERROR ? -1 : 0;
}

int Restore_Handler::put_error(MYSQL *con)
{
  return put_info(mysql_error(con), INFO_ERROR, mysql_errno(con),
		  client_mysql_sqlstate(con));
}

/* print_warnings should be called right after executing a statement */

void Restore_Handler::print_warnings()
{
  const char   *query;
  MYSQL_RES    *result;
  MYSQL_ROW    cur;
  my_ulonglong num_rows;
  
  /* Save current error before calling "show warnings" */
  uint error= mysql_errno(&mysql);

  /* Get the warnings */
  query= "show warnings";
  mysql_real_query_for_lazy(query, strlen(query));
  mysql_store_result_for_lazy(&result);

  /* Bail out when no warnings */
  if (!result || !(num_rows= mysql_num_rows(result)))
    goto end;

  cur= mysql_fetch_row(result);

  /*
    Don't print a duplicate of the current error.  It is possible for SHOW
    WARNINGS to return multiple errors with the same code, but different
    messages.  To be safe, skip printing the duplicate only if it is the only
    warning.
  */
  if (!cur || (num_rows == 1 && error == (uint) strtoul(cur[1], NULL, 10)))
    goto end;

  /* Print the warnings */
  do
  {
    fprintf(output_file, "%s (Code %s): %s\n", cur[0], cur[1], cur[2]);
  } while ((cur= mysql_fetch_row(result)));

end:
  mysql_free_result(result);
}

/* Print binary value as hex literal (0x ...) */
void Restore_Handler::print_as_hex(FILE *output_file, 
                  const char *str, ulong len, ulong total_bytes_to_send)
{
  const char *ptr= str, *end= ptr+len;
  ulong i;
  fprintf(output_file, "0x");
  for(; ptr < end; ptr++)
    fprintf(output_file, "%02X", *((uchar*)ptr));
  for (i= 2*len+2; i < total_bytes_to_send; i++)
    putc((int)' ', output_file);
}

/**
  Write data to a stream.
  Various modes, corresponding to --tab, --xml, --raw parameters,
  are supported.

  @param file   Stream to write to
  @param s      String to write
  @param slen   String length
  @flags        Flags for --tab, --xml, --raw.
*/
void Restore_Handler::tee_write(FILE *file, const char *s, 
                                size_t slen, int flags)
{
  const char *se;
  for (se= s + slen; s < se; s++)
  {
    const char *t;

    if (flags & MY_PRINT_MB)
    {
      int mblen;
      if (use_mb(charset_info) &&
          (mblen= my_ismbchar(charset_info, s, se)))
      {
        if (fwrite(s, 1, mblen, file) != (size_t) mblen) {
          perror("fwrite");
        }
        s+= mblen - 1;
        continue;
      }
    }

    if ((flags & MY_PRINT_XML) && (t= array_value(xmlmeta, *s)))
      fputs(t, file);
    else if ((flags & MY_PRINT_SPS_0) && *s == '\0')
      putc((int) ' ', file);   // This makes everything hard
    else if ((flags & MY_PRINT_ESC_0) && *s == '\0')
      fputs("\\0", file);      // This makes everything hard
    else if ((flags & MY_PRINT_CTRL) && *s == '\t')
      fputs("\\t", file);      // This would destroy tab format
    else if ((flags & MY_PRINT_CTRL) && *s == '\n')
      fputs("\\n", file);      // This too
    else if ((flags & MY_PRINT_CTRL) && *s == '\\')
      fputs("\\\\", file);
    else
    {
      putc((int) *s, file);
    }
  }
}

void Restore_Handler::tee_print_sized_data(const char *data, 
                                          unsigned int data_length, 
                                          unsigned int total_bytes_to_send, 
                                          bool right_justified)
{
  /* 
    For '\0's print ASCII spaces instead, as '\0' is eaten by (at
    least my) console driver, and that messes up the pretty table
    grid.  (The \0 is also the reason we can't use fprintf() .) 
  */
  unsigned int i;

  if (right_justified) 
    for (i= data_length; i < total_bytes_to_send; i++)
      putc((int)' ', output_file);

  tee_write(output_file, data, data_length, MY_PRINT_SPS_0 | MY_PRINT_MB);

  if (! right_justified) 
    for (i= data_length; i < total_bytes_to_send; i++)
      putc((int)' ', output_file);
}

/* Initialize options for the given connection handle. */
void Restore_Handler::init_connection_options(MYSQL *mysql)
{
  my_bool handle_expired= (opt_connect_expired_password) ?
    TRUE : FALSE;

  if (opt_init_command)
    mysql_options(mysql, MYSQL_INIT_COMMAND, opt_init_command);

  if (opt_connect_timeout)
  {
    uint timeout= opt_connect_timeout;
    mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, (char*) &timeout);
  }

  if (opt_bind_addr)
    mysql_options(mysql, MYSQL_OPT_BIND, opt_bind_addr);

  if (opt_compress)
    mysql_options(mysql, MYSQL_OPT_COMPRESS, NullS);

  if (using_opt_local_infile)
    mysql_options(mysql, MYSQL_OPT_LOCAL_INFILE, (char*) &opt_local_infile);

  // SSL_SET_OPTIONS(mysql);
  static uint opt_ssl_mode = SSL_MODE_PREFERRED;
  mysql_ssl_set(mysql, NULL, NULL, NULL,
                NULL, NULL);
  mysql_options(mysql, MYSQL_OPT_SSL_CRL, 0);
  mysql_options(mysql, MYSQL_OPT_SSL_CRLPATH, 0);
  mysql_options(mysql, MYSQL_OPT_TLS_VERSION, 0);
  mysql_options(mysql, MYSQL_OPT_SSL_MODE, &opt_ssl_mode);

  if (opt_protocol)
    mysql_options(mysql, MYSQL_OPT_PROTOCOL, (char*) &opt_protocol);

  if (safe_updates)
  {
    char init_command[100];
    sprintf(init_command,
	    "SET SQL_SAFE_UPDATES=1,SQL_SELECT_LIMIT=%lu,MAX_JOIN_SIZE=%lu",
	    select_limit, max_join_size);
    mysql_options(mysql, MYSQL_INIT_COMMAND, init_command);
  }

  mysql_set_character_set(mysql, default_charset);

  if (opt_plugin_dir && *opt_plugin_dir)
    mysql_options(mysql, MYSQL_PLUGIN_DIR, opt_plugin_dir);

  if (opt_default_auth && *opt_default_auth)
    mysql_options(mysql, MYSQL_DEFAULT_AUTH, opt_default_auth);

  if (using_opt_enable_cleartext_plugin)
    mysql_options(mysql, MYSQL_ENABLE_CLEARTEXT_PLUGIN,
                  (char*) &opt_enable_cleartext_plugin);

  mysql_options(mysql, MYSQL_OPT_CONNECT_ATTR_RESET, 0);
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "program_name", "mysql");

  mysql_options(mysql, MYSQL_OPT_CAN_HANDLE_EXPIRED_PASSWORDS, &handle_expired);
}

int Restore_Handler::sql_real_connect(const char *host, 
                                      unsigned int port,
                                      const char *user,
                                      const char *password,
                                      const char *database)
{
  if(connected) {
    connected = false;
    mysql_close(&mysql);
  }

  mysql_init(&mysql);
  init_connection_options(&mysql);
  int connect_flag = 0;

  if (!mysql_real_connect(&mysql, host, user, password,
                          database, port, NULL,
                          connect_flag | CLIENT_MULTI_STATEMENTS))
  {
    (void) put_error(&mysql);
    (void) fflush(output_file);
    return 1;
  }

  charset_info = mysql.charset;
  connected = true;

  return 0;
}

/**
   It checks if the input is a short form command. It returns the command's
   pointer if a command is found, else return NULL. Note that if binary-mode
   is set, then only \C is searched for.

   @param cmd_char    A character of one byte.

   @return
     the command's pointer or NULL.
*/
Restore_Handler::COMMANDS *Restore_Handler::find_command(char cmd_char)
{
  DBUG_ENTER("Restore_Handler::find_command");
  DBUG_PRINT("enter", ("cmd_char: %d", cmd_char));

  int index= -1;

  /*
    In binary-mode, we disallow all mysql commands except '\C'
    and DELIMITER.
  */
  if (real_binary_mode)
  {
    if (cmd_char == 'C')
      index= charset_index;
  }
  else
    index= get_command_index(cmd_char);

  if (index >= 0)
  {
    DBUG_PRINT("exit",("found command: %s", commands[index].name));
    DBUG_RETURN(&commands[index]);
  }
  else
    DBUG_RETURN((COMMANDS *) 0);
}

/**
   It checks if the input is a long form command. It returns the command's
   pointer if a command is found, else return NULL. Note that if binary-mode 
   is set, then only DELIMITER is searched for.

   @param name    A string.
   @return
     the command's pointer or NULL.
*/
Restore_Handler::COMMANDS *Restore_Handler::find_command(char *name)
{
  uint len;
  char *end;
  DBUG_ENTER("Restore_Handler::find_command");

  DBUG_ASSERT(name != NULL);
  DBUG_PRINT("enter", ("name: '%s'", name));

  while (my_isspace(charset_info, *name))
    name++;
  /*
    If there is an \\g in the row or if the row has a delimiter but
    this is not a delimiter command, let add_line() take care of
    parsing the row and calling find_command().
  */
  if ((!real_binary_mode && strstr(name, "\\g")) ||
      (strstr(name, delimiter) &&
       !is_delimiter_command(name, DELIMITER_NAME_LEN)))
      DBUG_RETURN((COMMANDS *) 0);

  if ((end=strcont(name, " \t")))
  {
    len=(uint) (end - name);
    while (my_isspace(charset_info, *end))
      end++;
    if (!*end)
      end= 0;					// no arguments to function
  }
  else
    len= (uint) strlen(name);

  int index= -1;
  if (real_binary_mode)
  {
    if (is_delimiter_command(name, len))
      index= delimiter_index;
  }
  else
  {
    /*
      All commands are in the first part of commands array and have a function
      to implement it.
    */
    for (uint i= 0; commands[i].func; i++)
    {
      if (!my_strnncoll(&my_charset_latin1, (uchar*) name, len,
                        (uchar*) commands[i].name, len) &&
          (commands[i].name[len] == '\0') &&
          (!end || commands[i].takes_params))
      {
        index= i;
        break;
      }
    }
  }

  if (index >= 0)
  {
    DBUG_PRINT("exit", ("found command: %s", commands[index].name));
    DBUG_RETURN(&commands[index]);
  }
  DBUG_RETURN((COMMANDS *) 0);
}

int Restore_Handler::reconnect()
{
  /* purecov: begin tested */
  if (opt_reconnect)
  {
    put_info("No connection. Trying to reconnect...",INFO_INFO);
    (void) com_connect((String *) 0, 0);
    if (opt_rehash)
      com_rehash(NULL, NULL);
  }
  if (!connected)
    return put_info("Can't connect to the server\n",INFO_ERROR);
  /* purecov: end */
  return 0;
}

void Restore_Handler::print_table_data_vertically(MYSQL_RES *result)
{
  MYSQL_ROW	cur;
  uint		max_length=0;
  MYSQL_FIELD	*field;

  while ((field = client_mysql_fetch_field(result)))
  {
    uint length= field->name_length;
    if (length > max_length)
      max_length= length;
    field->max_length=length;
  }

  client_mysql_field_seek(result,0);
  for (uint row_count=1; (cur= mysql_fetch_row(result)); row_count++)
  {
    if (interrupted_query)
      break;
    client_mysql_field_seek(result,0);
    fprintf(output_file, 
		"*************************** %d. row ***************************\n", row_count);

    ulong *lengths= mysql_fetch_lengths(result);

    for (uint off=0; off < mysql_num_fields(result); off++)
    {
      field= client_mysql_fetch_field(result);
      if (column_names)
        fprintf(output_file, "%*s: ",(int) max_length,field->name);
      if (cur[off])
      {
        if (opt_binhex && is_binary_field(field))
          print_as_hex(output_file, cur[off], lengths[off], lengths[off]);
        else
          tee_write(output_file, cur[off], lengths[off], MY_PRINT_SPS_0 | MY_PRINT_MB);
        putc('\n', output_file);
      }
      else
        fprintf(output_file, "NULL\n");
    }
  }
}

void Restore_Handler::safe_put_field(const char *pos,ulong length)
{
  if (!pos)
    fputs("NULL", output_file);
  else
  {
    int flags= MY_PRINT_MB | (opt_raw_data ? 0 : (MY_PRINT_ESC_0 | MY_PRINT_CTRL));
    /* Can't use tee_fputs(), it stops with NUL characters. */
    tee_write(output_file, pos, length, flags);
  }
}

void Restore_Handler::print_tab_data(MYSQL_RES *result)
{
  MYSQL_ROW	cur;
  MYSQL_FIELD	*field;
  ulong		*lengths;

  if (opt_silent < 2 && column_names)
  {
    int first=0;
    while ((field = client_mysql_fetch_field(result)))
    {
      if (first++)
	      (void) fputs("\t", output_file);
      (void) fputs(field->name, output_file);
    }
    (void) fputs("\n", output_file);
  }
  while ((cur = mysql_fetch_row(result)))
  {
    lengths=mysql_fetch_lengths(result);
    field= result->fields;
    if (opt_binhex && is_binary_field(&field[0]))
      print_as_hex(output_file, cur[0], lengths[0], lengths[0]);
    else
      safe_put_field(cur[0],lengths[0]);
    for (uint off=1 ; off < mysql_num_fields(result); off++)
    {
      (void) fputs("\t", output_file);
      if (opt_binhex && field && is_binary_field(&field[off]))
        print_as_hex(output_file, cur[off], lengths[off], lengths[off]);
      else
        safe_put_field(cur[off], lengths[off]);
    }
    (void) fputs("\n", output_file);
  }
}

void Restore_Handler::print_field_types(MYSQL_RES *result)
{
  MYSQL_FIELD   *field;
  uint i=0;

  while ((field = client_mysql_fetch_field(result)))
  {
    fprintf(output_file, "Field %3u:  `%s`\n"
            "Catalog:    `%s`\n"
            "Database:   `%s`\n"
            "Table:      `%s`\n"
            "Org_table:  `%s`\n"
            "Type:       %s\n"
            "Collation:  %s (%u)\n"
            "Length:     %lu\n"
            "Max_length: %lu\n"
            "Decimals:   %u\n"
            "Flags:      %s\n\n",
            ++i,
            field->name, field->catalog, field->db, field->table,
            field->org_table, fieldtype2str(field->type),
            get_charset_name(field->charsetnr), field->charsetnr,
            field->length, field->max_length, field->decimals,
            fieldflags2str(field->flags));
  }
  putsln("", output_file);
}

void Restore_Handler::print_table_data(MYSQL_RES *result)
{
  String separator(256);
  MYSQL_ROW	cur;
  MYSQL_FIELD	*field;
  bool		*num_flag;
  size_t        sz;

  sz= sizeof(bool) * mysql_num_fields(result);
  num_flag= (bool *) my_safe_alloca(sz, MAX_ALLOCA_SIZE);
  if (column_types_flag)
  {
    print_field_types(result);
    if (!mysql_num_rows(result)) {
      my_safe_afree((bool *) num_flag, sz, MAX_ALLOCA_SIZE);
      return;
    }
    client_mysql_field_seek(result,0);
  }
  separator.copy("+",1,charset_info);
  while ((field = client_mysql_fetch_field(result)))
  {
    size_t length= column_names ? field->name_length : 0;
    if (quick)
      length= std::max<size_t>(length, field->length);
    else
      length= std::max<size_t>(length, field->max_length);
    if (length < 4 && !IS_NOT_NULL(field->flags))
      length=4;					// Room for "NULL"
    if (opt_binhex && is_binary_field(field))
      length= 2 + length * 2;
    field->max_length=(ulong) length;
    separator.fill(separator.length()+length+2,'-');
    separator.append('+');
  }
  separator.append('\0');                       // End marker for \0
  putsln((char*) separator.ptr(), output_file);
  if (column_names)
  {
    client_mysql_field_seek(result,0);
    (void) fputs("|", output_file);
    for (uint off=0; (field = client_mysql_fetch_field(result)) ; off++)
    {
      size_t name_length= strlen(field->name);
      size_t numcells= charset_info->cset->numcells(charset_info,
                                                    field->name,
                                                    field->name + name_length);
      size_t display_length= field->max_length + name_length - numcells;
      fprintf(output_file, " %-*s |",
                  std::min<int>((int) display_length, MAX_COLUMN_LENGTH),
                  field->name);
      num_flag[off]= IS_NUM(field->type);
    }
    (void) fputs("\n", output_file);
    putsln((char*) separator.ptr(), output_file);
  }

  while ((cur= mysql_fetch_row(result)))
  {
    if (interrupted_query)
      break;
    ulong *lengths= mysql_fetch_lengths(result);
    (void) fputs("| ", output_file);
    client_mysql_field_seek(result, 0);
    for (uint off= 0; off < mysql_num_fields(result); off++)
    {
      const char *buffer;
      uint data_length;
      uint field_max_length;
      size_t visible_length;
      uint extra_padding;

      if (off)
        (void) fputs(" ", output_file);

      if (cur[off] == NULL)
      {
        buffer= "NULL";
        data_length= 4;
      } 
      else 
      {
        buffer= cur[off];
        data_length= (uint) lengths[off];
      }

      field= client_mysql_fetch_field(result);
      field_max_length= field->max_length;

      /* 
       How many text cells on the screen will this string span?  If it contains
       multibyte characters, then the number of characters we occupy on screen
       will be fewer than the number of bytes we occupy in memory.

       We need to find how much screen real-estate we will occupy to know how 
       many extra padding-characters we should send with the printing function.
      */
      visible_length= charset_info->cset->numcells(charset_info, buffer, buffer + data_length);
      extra_padding= (uint) (data_length - visible_length);

      if (opt_binhex && is_binary_field(field))
        print_as_hex(output_file, cur[off], lengths[off], field_max_length);
      else if (field_max_length > MAX_COLUMN_LENGTH)
        tee_print_sized_data(buffer, data_length, MAX_COLUMN_LENGTH+extra_padding, FALSE);
      else
      {
        if (num_flag[off] != 0) /* if it is numeric, we right-justify it */
          tee_print_sized_data(buffer, data_length, field_max_length+extra_padding, TRUE);
        else 
          tee_print_sized_data(buffer, data_length, field_max_length+extra_padding, FALSE);
      }
      fputs(" |", output_file);
    }
    (void) fputs("\n", output_file);
  }
  putsln((char*) separator.ptr(), output_file);
  my_safe_afree((bool *) num_flag, sz, MAX_ALLOCA_SIZE);
}

bool Restore_Handler::add_line(String &buffer, char *line, size_t line_length,
                              char *in_string, bool *ml_comment, bool truncated)
{
  uchar inchar;
  char buff[80], *pos, *out;
  COMMANDS *com;
  bool need_space= 0;
  enum { SSC_NONE= 0, SSC_CONDITIONAL, SSC_HINT } ss_comment= SSC_NONE;
  DBUG_ENTER("Restore_Handler::add_line");

  if (!line[0] && buffer.is_empty())
    DBUG_RETURN(0);

  char *end_of_line= line + line_length;

  for (pos= out= line; pos < end_of_line; pos++)
  {
    inchar= (uchar) *pos;
    if (!preserve_comments)
    {
      // Skip spaces at the beginning of a statement
      if (my_isspace(charset_info,inchar) && (out == line) &&
          buffer.is_empty())
        continue;
    }
    // Accept multi-byte characters as-is
    int length;
    if (use_mb(charset_info) &&
        (length= my_ismbchar(charset_info, pos, end_of_line)))
    {
      if (!*ml_comment || preserve_comments)
      {
        while (length--)
          *out++ = *pos++;
        pos--;
      }
      else
        pos+= length - 1;
      continue;
    }

    if (!*ml_comment && inchar == '\\' &&
        !(*in_string && 
          (mysql.server_status & SERVER_STATUS_NO_BACKSLASH_ESCAPES)))
    {
      // Found possbile one character command like \c

      if (!(inchar = (uchar) *++pos))
	      break;				// readline adds one '\'
      if (*in_string || inchar == 'N')	// \N is short for NULL
      {					// Don't allow commands in string
	      *out++='\\';
        if ((inchar == '`') && (*in_string == inchar))
          pos--;
        else
	        *out++= (char) inchar;
	      continue;
      }
      if ((com= find_command((char) inchar)))
      {
        // Flush previously accepted characters
        if (out != line)
        {
          buffer.append(line, (uint) (out-line));
          out= line;
        }
        
        if ((*com->func)(this, &buffer, pos-1) > 0)
          DBUG_RETURN(1);                       // Quit
        if (com->takes_params)
        {
          if (ss_comment)
          {
            /*
              If a client-side macro appears inside a server-side comment,
              discard all characters in the comment after the macro (that is,
              until the end of the comment rather than the next delimiter)
            */
            for (pos++; *pos && (*pos != '*' || *(pos + 1) != '/'); pos++)
              ;
            pos--;
          }
          else
          {
            for (pos++ ;
                 *pos && (*pos != *delimiter ||
                          !is_prefix(pos + 1, delimiter + 1)) ; pos++)
              ;	// Remove parameters
            if (!*pos)
              pos--;
            else 
              pos+= (uint)strlen(delimiter) - 1; // Point at last delim char
          }
        }
      }
      else
      {
        sprintf(buff,"Unknown command '\\%c'.",inchar);
        if (put_info(buff,INFO_ERROR) > 0)
          DBUG_RETURN(1);
        *out++='\\';
        *out++=(char) inchar;
        continue;
      }
    }
    else if (!*ml_comment && !*in_string && ss_comment != SSC_HINT &&
             is_prefix(pos, delimiter))
    {
      // Found a statement. Continue parsing after the delimiter
      pos+= (uint)strlen(delimiter);

      if (preserve_comments)
      {
        while (my_isspace(charset_info, *pos))
          *out++= *pos++;
      }
      // Flush previously accepted characters
      if (out != line)
      {
        buffer.append(line, (uint32) (out-line));
        out= line;
      }

      if (preserve_comments && ((*pos == '#') ||
                                ((*pos == '-') &&
                                 (pos[1] == '-') &&
                                 my_isspace(charset_info, pos[2]))))
      {
        // Add trailing single line comments to this statement
        buffer.append(pos);
        pos+= strlen(pos);
      }

      pos--;

      if ((com= find_command(buffer.c_ptr())))
      {
          
        if ((*com->func)(this, &buffer, buffer.c_ptr()) > 0)
          DBUG_RETURN(1);                       // Quit 
      }
      else
      {
        if (com_go(&buffer, 0) > 0)             // < 0 is not fatal
          DBUG_RETURN(1);
      }
      buffer.length(0);
    }
    else if (!*ml_comment && (!*in_string && (inchar == '#' ||
                                              (inchar == '-' && pos[1] == '-' &&
                              /*
                                The third byte is either whitespace or is the
                                end of the line -- which would occur only
                                because of the user sending newline -- which is
                                itself whitespace and should also match.
                              */
			      (my_isspace(charset_info,pos[2]) ||
                               !pos[2])))))
    {
      // Flush previously accepted characters
      if (out != line)
      {
        buffer.append(line, (uint32) (out - line));
        out= line;
      }

      // comment to end of line
      if (preserve_comments)
      {
        bool started_with_nothing= !buffer.length();

        buffer.append(pos);

        /*
          A single-line comment by itself gets sent immediately so that
          client commands (delimiter, status, etc) will be interpreted on
          the next line.
        */
        if (started_with_nothing)
        {
          if (com_go(&buffer, 0) > 0)             // < 0 is not fatal
            DBUG_RETURN(1);
          buffer.length(0);
        }
      }

      break;
    }
    else if (!*in_string && inchar == '/' && pos[1] == '*' &&
	     pos[2] != '!' && pos[2] != '+' && ss_comment != SSC_HINT)
    {
      if (preserve_comments)
      {
        *out++= *pos++;                       // copy '/'
        *out++= *pos;                         // copy '*'
      }
      else
        pos++;
      *ml_comment= 1;
      if (out != line)
      {
        buffer.append(line,(uint) (out-line));
        out=line;
      }
    }
    else if (*ml_comment && !ss_comment && inchar == '*' && *(pos + 1) == '/')
    {
      if (preserve_comments)
      {
        *out++= *pos++;                       // copy '*'
        *out++= *pos;                         // copy '/'
      }
      else
        pos++;
      *ml_comment= 0;
      if (out != line)
      {
        buffer.append(line, (uint32) (out - line));
        out= line;
      }
      // Consumed a 2 chars or more, and will add 1 at most,
      // so using the 'line' buffer to edit data in place is ok.
      need_space= 1;
    }      
    else
    {						// Add found char to buffer
      if (!*in_string && inchar == '/' && pos[1] == '*')
      {
        if (pos[2] == '!')
          ss_comment= SSC_CONDITIONAL;
        else if (pos[2] == '+')
          ss_comment= SSC_HINT;
      }
      else if (!*in_string && ss_comment && inchar == '*' && *(pos + 1) == '/')
        ss_comment= SSC_NONE;

      if (inchar == *in_string)
	      *in_string= 0;
      else if (!*ml_comment && !*in_string && ss_comment != SSC_HINT &&
	       (inchar == '\'' || inchar == '"' || inchar == '`'))
        *in_string= (char) inchar;

      if (!*ml_comment || preserve_comments)
      {
        if (need_space && !my_isspace(charset_info, (char)inchar))
          *out++= ' ';
        need_space= 0;
        *out++= (char) inchar;
      }
    }
  }

  if (out != line || !buffer.is_empty())
  {
    uint length=(uint) (out-line);

    if (!truncated && (!is_delimiter_command(line, length) ||
                       (*in_string || *ml_comment)))
    {
      /* 
        Don't add a new line in case there's a DELIMITER command to be 
        added to the glob buffer (e.g. on processing a line like 
        "<command>;DELIMITER <non-eof>") : similar to how a new line is 
        not added in the case when the DELIMITER is the first command 
        entered with an empty glob buffer. However, if the delimiter is
        part of a string or a comment, the new line should be added. (e.g.
        SELECT '\ndelimiter\n';\n)
      */
      *out++='\n';
      length++;
    }
    if (buffer.length() + length >= buffer.alloced_length())
      buffer.mem_realloc(buffer.length()+length+IO_SIZE);
    if ((!*ml_comment || preserve_comments) && buffer.append(line, length))
      DBUG_RETURN(1);
  }
  DBUG_RETURN(0);
}

void Restore_Handler::remove_cntrl(String &buffer)
{
  char *start,*end;
  end=(start=(char*) buffer.ptr())+buffer.length();
  while (start < end && !my_isgraph(charset_info,end[-1]))
    end--;
  buffer.length((uint) (end-start));
}

int Restore_Handler::read_and_execute()
{
  /*
    line can be allocated by:
    - batch_readline. Use my_free()
    - my_win_console_readline. Do not free, see tmpbuf.
    - readline. Use free()
  */
  char	*line= NULL;
  char	in_string=0;
  ulong line_number=0;
  bool ml_comment= 0;  
  COMMANDS *com;
  size_t line_length= 0;
  status.exit_status=1;
  
  for (;;)
  {

    /*
      batch_readline can return 0 on EOF or error.
      In that case, we need to double check that we have a valid
      line before actually setting line_length to read_length.
      */
    line= batch_readline(status.line_buff, real_binary_mode);
    if (line) 
    {
      line_length= status.line_buff->read_length;

      /*
        ASCII 0x00 is not allowed appearing in queries if it is not in binary
        mode.
      */
      if (!real_binary_mode && strlen(line) != line_length)
      {
        status.exit_status= 1;
        String msg;
        msg.append("ASCII '\\0' appeared in the statement, but this is not "
                    "allowed unless option --binary-mode is enabled and mysql is "
                    "run in non-interactive mode. Set --binary-mode to 1 if ASCII "
                    "'\\0' is expected. Query: '");
        msg.append(glob_buffer);
        msg.append(line);
        msg.append("'.");
        put_info(msg.c_ptr(), INFO_ERROR);
        break;
      }

      /*
        Skip UTF8 Byte Order Marker (BOM) 0xEFBBBF.
        Editors like "notepad" put this marker in
        the very beginning of a text file when
        you save the file using "Unicode UTF-8" format.
      */
      if (!line_number &&
            (uchar) line[0] == 0xEF &&
            (uchar) line[1] == 0xBB &&
            (uchar) line[2] == 0xBF)
      {
        line+= 3;
        // decrease the line length accordingly to the 3 bytes chopped
        line_length -=3;
      }
    }

    line_number++;
    if (!glob_buffer.length())
      status.query_start_line=line_number;

    // End of file or system error
    if (!line)
    {
      if (status.line_buff && status.line_buff->error)
        status.exit_status= 1;
      else
        status.exit_status= 0;
      break;
    }

    /*
      Check if line is a mysql command line
      (We want to allow help, print and clear anywhere at line start
    */
    if ((named_cmds || glob_buffer.is_empty())
	      && !ml_comment && !in_string && (com= find_command(line)))
    {
      if ((*com->func)(this, &glob_buffer, line) > 0)
      {
	      break;
      }
      if (glob_buffer.is_empty())		// If buffer was emptied
	      in_string=0;
      
      continue;
    }

    if (add_line(glob_buffer, line, line_length, &in_string, &ml_comment,
                 status.line_buff ? status.line_buff->truncated : 0))
      break;
  }

  /* if in batch mode, send last query even if it doesn't end with \g or go */
  if (!status.exit_status)
  {
    remove_cntrl(glob_buffer);
    if (!glob_buffer.is_empty())
    {
      status.exit_status=1;
      if (com_go(&glob_buffer,line) <= 0)
	      status.exit_status=0;
    }
  }

  /*
    If the function is called by 'source' command, it will return to interactive
    mode, so real_binary_mode should be FALSE. Otherwise, it will exit the
    program, it is safe to set real_binary_mode to FALSE.
  */
  real_binary_mode= false;
  return status.exit_status;
}

void Restore_Handler::mysql_end(int sig)
{
  mysql_close(&mysql);
  batch_readline_end(status.line_buff);

  if (sig >= 0)
    put_info(sig ? "Aborted" : "Bye", INFO_RESULT);

  glob_buffer.mem_free();
}

bool Restore_Handler::import_data_main(string &errmsg) noexcept
{
  char buffer[1024];

  DBUG_ENTER("Restore_Handler::import_data_main");
  errmsg.clear();

  charset_index= get_command_index('C');
  delimiter_index= get_command_index('d');

  // Open the input file
  if (!(sql_file = my_fopen(sql_file_name.c_str(), 
                            O_RDONLY | O_BINARY,MYF(0))))
  {
    errmsg= "Failed to open sql file: " + sql_file_name;
    DBUG_RETURN(true);
  }
  MY_FILE_GUARD sql_file_guard(sql_file);

  // Open the output file
  if (!(output_file= my_fopen(output_file_name.c_str(), 
                            O_WRONLY | FILE_BINARY | O_APPEND,
                            MYF(MY_WME)))) {
    errmsg= "Failed to open output file: " + output_file_name;
    DBUG_RETURN(true);
  }
  MY_FILE_GUARD out_file_guard(output_file);

  opt_silent= 1;
  status.exit_status = 1;
  char cur_file_name[FN_REFLEN];
  my_snprintf(cur_file_name, sizeof(cur_file_name), "%s", sql_file_name.c_str());
  status.file_name= cur_file_name;

  // Initialize batch_readline
  if (!status.line_buff &&
      !(status.line_buff = batch_readline_init(MAX_BATCH_BUFFER_SIZE, sql_file)))
  {
    put_info("Can't initialize batch_readline - may be the input source is "
             "a directory or a block device.", INFO_ERROR, 0);
    errmsg= "Can't initialize batch_readline";
    DBUG_RETURN(true);
  }
  glob_buffer.mem_realloc(512);

  // Connect to the server
  if (sql_real_connect(current_host.c_str(),
                       server_options.get_port(),
                       current_user.c_str(),
                       server_options.get_password(),
                       current_db.c_str()))
  {
    quick= 1;	
    my_snprintf(buffer, sizeof(buffer), "Failed to connect to %s#%d", 
                current_host.c_str(), server_options.get_port());
    errmsg = buffer;
    mysql_end(-1);
    DBUG_RETURN(true);
  }

  status.exit_status = read_and_execute();
  if (status.exit_status != 0)
  {
    my_snprintf(buffer, sizeof(buffer), "Failed to import data from %s", 
                sql_file_name.c_str());
    errmsg = buffer;
  }

  mysql_end(0);
  DBUG_RETURN(status.exit_status != 0);
}