#include "tc_dump.h"
#include "errmsg.h"
#include "my_sys.h"
#include "mysql/service_my_snprintf.h"
#include "sql_class.h"
#include "mysql.h"
#include "sql_servers.h"
#include "tc_base.h"
#include "my_user.h"
#include "log.h"
#include "mysql.h"
#include "sql_common.h"
#include <cstddef>
#include <memory>
#include <stdio.h>
#include <string>
#include <functional>
#include <sstream>

using std::string;


static const char *tcdump_progname = "tdbctl_dump";

#define QUERY_LENGTH 1536
#define COMMENT_LENGTH 2048
#define DUMP_VERSION "10.13"
#define FIRST_INFORMATION_SCHEMA_VERSION 50003
#define INFORMATION_SCHEMA_DB_NAME "information_schema"
#define FIRST_PERFORMANCE_SCHEMA_VERSION 50503
#define PERFORMANCE_SCHEMA_DB_NAME "performance_schema"
#define FIRST_SYS_SCHEMA_VERSION 50707
#define SYS_SCHEMA_DB_NAME "sys"

/* ignore table flags */
#define IGNORE_NONE 0x00 /* no ignore */
#define IGNORE_DATA 0x01 /* don't dump data for this table */

/* index into 'show fields from table' */

#define SHOW_FIELDNAME  0
#define SHOW_TYPE  1
#define SHOW_NULL  2
#define SHOW_DEFAULT  4
#define SHOW_EXTRA  5

#define MASK_ANSI_QUOTES \
(\
 (1<<2)  | /* POSTGRESQL */\
 (1<<3)  | /* ORACLE     */\
 (1<<4)  | /* MSSQL      */\
 (1<<5)  | /* DB2        */\
 (1<<6)  | /* MAXDB      */\
 (1<<10)   /* ANSI       */\
)


/* 
  Move to a specific row and column 
  This function was migrated from libmysql.c
  see libmysql.c: mysql_data_seek
*/
void STDCALL
client_mysql_data_seek(MYSQL_RES *result, my_ulonglong row)
{
  MYSQL_ROWS	*tmp=0;
  DBUG_PRINT("info",("client_mysql_data_seek(%ld)",(long) row));
  if (result->data)
    for (tmp=result->data->data; row-- && tmp ; tmp = tmp->next) ;
  result->current_row=0;
  result->data_cursor = tmp;
}

/*
  This function was migrated from libmysql.c
  see libmysql.c: mysql_field_seek
*/
MYSQL_FIELD_OFFSET STDCALL
client_mysql_field_seek(MYSQL_RES *result, MYSQL_FIELD_OFFSET field_offset)
{
  MYSQL_FIELD_OFFSET return_value=result->current_field;
  result->current_field=field_offset;
  return return_value;
}

/*
  Expand wildcard to a sql string
  This function was migrated from libmysql.c
  see libmysql.c: append_wild
*/

static void
client_append_wild(char *to, char *end, const char *wild)
{
  end-=5;					/* Some extra */
  if (wild && wild[0])
  {
    to=my_stpcpy(to," like '");
    while (*wild && to < end)
    {
      if (*wild == '\\' || *wild == '\'')
	*to++='\\';
      *to++= *wild++;
    }
    if (*wild)					/* Too small buffer */
      *to++='%';				/* Nicer this way */
    to[0]='\'';
    to[1]=0;
  }
}

/* 
  List all tables in a database
  If wild is given then only the tables matching wild is returned 
  This function was migrated from libmysql.c
  see libmysql.c: mysql_list_tables
*/
MYSQL_RES * STDCALL
client_mysql_list_tables(MYSQL *mysql, const char *wild)
{
  char buff[255];
  DBUG_ENTER("client_mysql_list_tables");

  client_append_wild(my_stpcpy(buff,"show tables"),buff+sizeof(buff),wild);
  if (mysql_real_query(mysql,buff, strlen(buff)))
    DBUG_RETURN(0);
  DBUG_RETURN (mysql_store_result(mysql));
}

/**
  Escapes special characters in a string for use in an SQL statement.
  This function was migrated from libmysql.c
  see libmysql.c: mysql_real_escape_string_quote
*/

ulong STDCALL
client_mysql_real_escape_string_quote(MYSQL *mysql, char *to, const char *from,
                               ulong length, char quote)
{
  if (quote == '`' || mysql->server_status & SERVER_STATUS_NO_BACKSLASH_ESCAPES)
    return (uint)escape_quotes_for_mysql(mysql->charset, to, 0,
                                         from, length, quote);
  return (uint)escape_string_for_mysql(mysql->charset, to, 0, from, length);
}

/*
  Return next field of the query results
  This function was migrated from libmysql.c
  see libmysql.c: mysql_fetch_field
*/
MYSQL_FIELD * STDCALL
client_mysql_fetch_field(MYSQL_RES *result)
{
  if (result->current_field >= result->field_count)
    return(NULL);
  return &result->fields[result->current_field++];
}

/*
  This function was migrated from libmysql.c
  see libmysql.c: mysql_fetch_field_direct
*/
MYSQL_FIELD * STDCALL client_mysql_fetch_field_direct(MYSQL_RES *res,uint fieldnr)
{
  return &(res)->fields[fieldnr];
}

/*
  This function was migrated from libmysql.c
  see libmysql.c: mysql_refresh
*/
int STDCALL
client_mysql_refresh(MYSQL *mysql,uint options)
{
  uchar bits[1];
  DBUG_ENTER("client_mysql_refresh");
  bits[0]= (uchar) options;
  DBUG_RETURN(simple_command(mysql, COM_REFRESH, bits, 1, 0));
}

/*
  This function was migrated from libmysql.c
  see libmysql.c: mysql_use_result
*/
MYSQL_RES * STDCALL client_mysql_use_result(MYSQL *mysql)
{
  return (*mysql->methods->use_result)(mysql);
}

/*
  This function was migrated from libmysql.c
  see libmysql.c: mysql_sqlstate
*/
const char *STDCALL client_mysql_sqlstate(MYSQL *mysql)
{
  return mysql ? mysql->net.sqlstate : cant_connect_sqlstate;
}


/**
 * Frees a MySQL result set and sets the pointer to NULL.
 * @param res Reference to a MYSQL_RES pointer. If the pointer is not NULL,
 *            the result set is freed and the pointer is set to NULL.
 */
void mysql_free_result_with_set_null(MYSQL_RES *&res)
{
  if (res)
  {
    mysql_free_result(res);
    res= NULL;
  }
}

/*
  Sends a query to server, optionally reads result, throws exception if error.

  SYNOPSIS
    mysql_query_with_exception()
    mysql_con       connection to use
    res             if non zero, result will be put there with
                    mysql_store_result()
    query           query to send to server

  RETURN VALUES
    0               query sending and (if res!=0) result reading went ok
    1               error
*/
static int mysql_query_with_exception(MYSQL *mysql_con, MYSQL_RES **res,
                                         const char *query) throw(Query_exception)
{
  if (mysql_real_query(mysql_con, query, strlen(query)) ||
      (res && !((*res)= mysql_store_result(mysql_con))))
  {
    throw Query_exception(query, mysql_con);
    return 1;
  }
  return 0;
}

/*
  Note: This function is copied from mysqldump.c

  Quote a table name so it can be used in "SHOW TABLES LIKE <tabname>"

  SYNOPSIS
    quote_for_like()
    name     name of the table
    buff     quoted name of the table

  DESCRIPTION
    Quote \, _, ' and % characters

    Note: Because MySQL uses the C escape syntax in strings
    (for example, '\n' to represent newline), you must double
    any '\' that you use in your LIKE  strings. For example, to
    search for '\n', specify it as '\\n'. To search for '\', specify
    it as '\\\\' (the backslashes are stripped once by the parser
    and another time when the pattern match is done, leaving a
    single backslash to be matched).

    Example: "t\1" => "t\\\\1"

*/
static char *quote_for_like(const char *name, char *buff)
{
  char *to= buff;
  *to++= '\'';
  while (*name)
  {
    if (*name == '\\')
    {
      *to++='\\';
      *to++='\\';
      *to++='\\';
    }
    else if (*name == '\'' || *name == '_'  || *name == '%')
      *to++= '\\';
    *to++= *name++;
  }
  to[0]= '\'';
  to[1]= 0;
  return buff;
}

/*
  Find the first occurrence of a quoted identifier in a given string. Returns
  the pointer to the opening quote, and stores the pointer to the closing quote
  to the memory location pointed to by the 'end' argument,

  If no quoted identifiers are found, returns NULL (and the value pointed to by
  'end' is undefined in this case).
*/
static const char *parse_quoted_identifier(const char *str,
                                            const char **end)
{
  const char *from;
  const char *to;

  if (!(from= strchr(str, '`')))
    return NULL;

  to= from;

  while ((to= strchr(to + 1, '`'))) {
    /*
      Double backticks represent a backtick in identifier, rather than a quote
      character.
    */
    if (to[1] == '`')
    {
      to++;
      continue;
    }

    break;
  }

  if (to <= from + 1)
    return NULL;                                /* Empty identifier */

  *end= to;

  return from;
}

static my_bool has_session_variables_like(MYSQL *mysql_con, const char *var_name)
{
  MYSQL_RES  *res;
  MYSQL_ROW  row;
  char       *val= 0;
  char       buf[32], query[256];
  my_bool    has_var= FALSE;
  my_bool    has_table= FALSE;

  my_snprintf(query, sizeof(query), "SELECT COUNT(*) FROM"
              " INFORMATION_SCHEMA.TABLES WHERE table_schema ="
              " 'performance_schema' AND table_name = 'session_variables'");
  if (mysql_query_with_exception(mysql_con, &res, query))
    return FALSE;

  row = mysql_fetch_row(res);
  val = row ? (char*)row[0] : NULL;
  has_table = val && strcmp(val, "0") != 0;
  mysql_free_result(res);

  if (has_table)
  {
    my_snprintf(query, sizeof(query), "SELECT COUNT(*) FROM"
                " performance_schema.session_variables WHERE VARIABLE_NAME LIKE"
                " %s", quote_for_like(var_name, buf));
    if (mysql_query_with_exception(mysql_con, &res, query))
      return FALSE;

    row = mysql_fetch_row(res);
    val = row ? (char*)row[0] : NULL;
    has_var = val && strcmp(val, "0") != 0;
    mysql_free_result(res);
  }

  return has_var;
}

/*
 Note: this function is copied from mysqldump.c

 This function accepts object names and prefixes -- wherever \n
 character is found.

 @param[in]     object_name

 @return
    @retval fixed object name.
*/

static const char* fix_identifier_with_newline(char* object_name)
{
  static char buff[COMMENT_LENGTH]= {0};
  char *ptr= buff;
  memset(buff, 0, 255);
  while(*object_name)
  {
    *ptr++ = *object_name;
    if (*object_name == '\n')
      ptr= my_stpcpy(ptr, "-- ");
    object_name++;
  }
  return buff;
}

static int do_flush_tables_read_lock(MYSQL *mysql_con)
{
  /*
    We do first a FLUSH TABLES. If a long update is running, the FLUSH TABLES
    will wait but will not stall the whole mysqld, and when the long update is
    done the FLUSH TABLES WITH READ LOCK will start and succeed quickly. So,
    FLUSH TABLES is to lower the probability of a stage where both mysqldump
    and most client connections are stalled. Of course, if a second long
    update starts between the two FLUSHes, we have that bad stall.
  */
  return
    ( mysql_query_with_exception(mysql_con, 0, "FLUSH TABLES") ||
      mysql_query_with_exception(mysql_con, 0,
                                    "FLUSH TABLES WITH READ LOCK") );
}

/**
   Execute LOCK TABLES FOR BACKUP if supported by the server.

   @note If LOCK TABLES FOR BACKUP is not supported by the server, then nothing
         is done and no error condition is returned.

   @returns  whether there was an error or not
*/
static int do_lock_tables_for_backup(MYSQL *mysql_con)
{
  return mysql_query_with_exception(mysql_con, 0,
                                       "LOCK TABLES FOR BACKUP");
}

static int do_unlock_tables(MYSQL *mysql_con)
{
  return mysql_query_with_exception(mysql_con, 0, "UNLOCK TABLES");
}

static void check_io(FILE *file) throw(IO_exception)
{
  if (ferror(file) || errno == 5)
    throw IO_exception();
}

uchar* get_table_key(const char *entry, size_t *length,
                     my_bool not_used MY_ATTRIBUTE((unused)))
{
  *length= strlen(entry);
  return (uchar*) entry;
}

/*
  The following functions are wrappers for the dynamic string functions
  and if they fail, the wrappers will throw an exception.
*/

#define DYNAMIC_STR_ERROR_MSG "Couldn't perform DYNAMIC_STRING operation"

/* RAII guard for DYNAMIC_STRING */
class DYNAMIC_STR_GUARD
{
public:
  DYNAMIC_STR_GUARD(DYNAMIC_STRING *str) : str(str) {}
  ~DYNAMIC_STR_GUARD() { dynstr_free(str); }
private:
  DYNAMIC_STRING *str;
};

static void init_dynamic_string_checked(DYNAMIC_STRING *str, const char *init_str,
			    size_t init_alloc, size_t alloc_increment) throw(DStr_exception)
{
  if (init_dynamic_string(str, init_str, init_alloc, alloc_increment))
    throw DStr_exception(DYNAMIC_STR_ERROR_MSG);
}

static void dynstr_append_checked(DYNAMIC_STRING* dest, const char* src) 
                                  throw(DStr_exception)
{
  if (dynstr_append(dest, src))
    throw DStr_exception(DYNAMIC_STR_ERROR_MSG);
}

static void dynstr_set_checked(DYNAMIC_STRING *str, const char *init_str)
{
  if (dynstr_set(str, init_str))
    throw DStr_exception(DYNAMIC_STR_ERROR_MSG);
}

static void dynstr_realloc_checked(DYNAMIC_STRING *str, size_t additional_size)
{
  if (dynstr_realloc(str, additional_size))
    throw DStr_exception(DYNAMIC_STR_ERROR_MSG);
}

static void dynstr_append_mem_checked(DYNAMIC_STRING *str, const char *append,
			  size_t length)
{
  if (dynstr_append_mem(str, append, length))
    throw DStr_exception(DYNAMIC_STR_ERROR_MSG);
}

/*
  Replace a substring

  SYNOPSIS
    replace
    ds_str      The string to search and perform the replace in
    search_str  The string to search for
    search_len  Length of the string to search for
    replace_str The string to replace with
    replace_len Length of the string to replace with

  RETURN
    0 String replaced
    1 Could not find search_str in str
*/
static int replace(DYNAMIC_STRING *ds_str,
                   const char *search_str, size_t search_len,
                   const char *replace_str, size_t replace_len)
{
  DYNAMIC_STRING ds_tmp;
  const char *start= strstr(ds_str->str, search_str);
  if (!start)
    return 1;
  init_dynamic_string_checked(&ds_tmp, "",
                      ds_str->length + replace_len, 256);

  /* make sure the dynamic string will be freed properly*/
  DYNAMIC_STR_GUARD ds_tmp_guard(&ds_tmp);

  dynstr_append_mem_checked(&ds_tmp, ds_str->str, start - ds_str->str);
  dynstr_append_mem_checked(&ds_tmp, replace_str, replace_len);
  dynstr_append_checked(&ds_tmp, start + search_len);
  dynstr_set_checked(ds_str, ds_tmp.str);
  dynstr_free(&ds_tmp);
  return 0;
}

static char *my_case_str(const char *str,
                         size_t str_len,
                         const char *token,
                         size_t token_len)
{
  my_match_t match;

  uint status= my_charset_latin1.coll->instr(&my_charset_latin1,
                                             str, str_len,
                                             token, token_len,
                                             &match, 1);

  return status ? (char *) str + match.end : NULL;
}

static char *alloc_query_str(size_t size)
{
  char *query;

  if (!(query= (char*) my_malloc(PSI_NOT_INSTRUMENTED,
                                 size, MYF(MY_WME))))
    throw TC_Dump_exception("Couldn't allocate a query string.");

  return query;
}

/**
  Rewrite statement, enclosing DEFINER clause in version-specific comment.

  This function parses any CREATE statement and encloses DEFINER-clause in
  version-specific comment:
    input query:     CREATE DEFINER=a@b FUNCTION ...
    rewritten query: CREATE * / / *!50020 DEFINER=a@b * / / *!50003 FUNCTION ...

  @note This function will go away when WL#3995 is implemented.

  @param[in] stmt_str                 CREATE statement string.
  @param[in] stmt_length              Length of the stmt_str.
  @param[in] definer_version_str      Minimal MySQL version number when
                                      DEFINER clause is supported in the
                                      given statement.
  @param[in] definer_version_length   Length of definer_version_str.
  @param[in] stmt_version_str         Minimal MySQL version number when the
                                      given statement is supported.
  @param[in] stmt_version_length      Length of stmt_version_str.
  @param[in] keyword_str              Keyword to look for after CREATE.
  @param[in] keyword_length           Length of keyword_str.

  @return pointer to the new allocated query string.
*/
static char *cover_definer_clause(const char *stmt_str,
                                  size_t stmt_length,
                                  const char *definer_version_str,
                                  size_t definer_version_length,
                                  const char *stmt_version_str,
                                  size_t stmt_version_length,
                                  const char *keyword_str,
                                  size_t keyword_length)
{
  char *definer_begin= my_case_str(stmt_str, stmt_length,
                                   C_STRING_WITH_LEN(" DEFINER"));
  char *definer_end= NULL;

  char *query_str= NULL;
  char *query_ptr;

  if (!definer_begin)
    return NULL;

  definer_end= my_case_str(definer_begin, strlen(definer_begin),
                           keyword_str, keyword_length);

  if (!definer_end)
    return NULL;

  /*
    Allocate memory for new query string: original string
    from SHOW statement and version-specific comments.
  */
  query_str= alloc_query_str(stmt_length + 23);

  query_ptr= my_stpncpy(query_str, stmt_str, definer_begin - stmt_str);
  query_ptr= my_stpncpy(query_ptr, C_STRING_WITH_LEN("*/ /*!"));
  query_ptr= my_stpncpy(query_ptr, definer_version_str, definer_version_length);
  query_ptr= my_stpncpy(query_ptr, definer_begin, definer_end - definer_begin);
  query_ptr= my_stpncpy(query_ptr, C_STRING_WITH_LEN("*/ /*!"));
  query_ptr= my_stpncpy(query_ptr, stmt_version_str, stmt_version_length);
  query_ptr= strxmov(query_ptr, definer_end, NullS);

  return query_str;
}

/*
 create_delimiter
 Generate a new (null-terminated) string that does not exist in  query 
 and is therefore suitable for use as a query delimiter.  Store this
 delimiter in  delimiter_buff .
 
 This is quite simple in that it doesn't even try to parse statements as an
 interpreter would.  It merely returns a string that is not in the query, which
 is much more than adequate for constructing a delimiter.

 RETURN
   ptr to the delimiter  on Success
   NULL                  on Failure
*/
static char *create_delimiter(char *query, char *delimiter_buff, 
                              int delimiter_max_size) 
{
  int proposed_length;
  char *presence;

  delimiter_buff[0]= ';';  /* start with one semicolon, and */

  for (proposed_length= 2; proposed_length < delimiter_max_size; 
      proposed_length++) {

    delimiter_buff[proposed_length-1]= ';';  /* add semicolons, until */
    delimiter_buff[proposed_length]= '\0';

    presence = strstr(query, delimiter_buff);
    if (presence == NULL) { /* the proposed delimiter is not in the query. */
       return delimiter_buff;
    }

  }
  return NULL;  /* but if we run out of space, return nothing at all. */
}

bool Dump_Handler::check_options(std::string &errmsg)
{
  errmsg = "";

  /* check target server */
  if (target_server == NULL)
  {
    errmsg = "Target server is a null pointer.\n";
    return true;
  }

  if (my_hash_init(&processed_compression_dictionaries, charset_info, 16, 0,
    0, (my_hash_get_key)get_table_key, my_free, 0, PSI_NOT_INSTRUMENTED))
  {
    errmsg = "Failed to initialize hash table for processed compression dictionaries.\n";
    return true;
  }

  if (opt_lock_for_backup && opt_lock_all_tables)
  {
    errmsg = "You can't use --lock-for-backup and --lock-all-tables at the same time.\n";
    return true;
  }

  /*
     Convert --lock-for-backup to --lock-all-tables if --single-transaction is
     not specified.
  */
  if (!opt_single_transaction && opt_lock_for_backup)
  {
    opt_lock_all_tables= 1;
    opt_lock_for_backup= 0;
  }

  if (opt_single_transaction && opt_lock_all_tables)
  {
    errmsg = "You can't use --single-transaction and --lock-all-tables at the same time.\n";
    return true;
  }

  if (mysql_options(NULL, MYSQL_OPT_MAX_ALLOWED_PACKET, &opt_max_allowed_packet) ||
      mysql_options(NULL, MYSQL_OPT_NET_BUFFER_LENGTH, &opt_net_buffer_length))
  {
    errmsg = "Failed to set max_allowed_packet or net_buffer_length.\n";
    return true;
  }

  if (opt_single_transaction || opt_lock_all_tables)
    opt_lock_tables= 0;

  if (strcmp(default_charset.c_str(), charset_info->csname) &&
      !(charset_info= get_charset_by_csname(default_charset.c_str(),
                                            MY_CS_PRIMARY, MYF(MY_WME)))) 
  {
    errmsg = "Unknown charset: " + default_charset;
    return true;
  }

  if(opt_alldbs == false) {
    errmsg = "The case of opt_alldbs=false is not supported.\n";
    return true;
  }

  if(opt_no_data == false) {
    errmsg = "The case of opt_no_data=false is not supported.\n";
    return true;
  }

  return false;
}

/**
   This function sets the session tc_admin=0 to disable tdbctl tc-admin mode, and 
   record 'set tc_admin=0' into the dump file.
*/
void Dump_Handler::set_session_tc_admin()
{
  if (has_session_variables_like(mysql, "tc_admin"))
  {
    mysql_query_with_exception(mysql, 0, 
                        "SET SESSION tc_admin=0");
    if(opt_print_tc_admin_info)
    {
      fprintf(result_file, "/*!50720 SET tc_admin=0 */;\n");
      check_io(result_file);
    }
  }
}

bool Dump_Handler::connect_to_server()
{
  char buff[20+FN_REFLEN];

  mysql_init(&mysql_connection);
  uint ssl_mode = SSL_MODE_DISABLED;
  mysql_options(&mysql_connection, MYSQL_OPT_SSL_MODE, &ssl_mode);
  mysql_options(&mysql_connection, MYSQL_SET_CHARSET_NAME, default_charset.c_str());
  mysql_options(&mysql_connection, MYSQL_OPT_CONNECT_ATTR_RESET, 0);
  mysql_options4(&mysql_connection, MYSQL_OPT_CONNECT_ATTR_ADD, "program_name", "TDBCTL:tc_dump");

  if (!(mysql= mysql_real_connect(&mysql_connection,
                                  target_server->host,
                                  target_server->username,
                                  target_server->password,
                                  NULL,
                                  target_server->port,
                                  0,
                                  0)))
  {
    throw DB_exception(&mysql_connection, "when trying to connect");
    return true;
  }

  if ((mysql_get_server_version(&mysql_connection) < 40100))
  {
    /* Don't dump SET NAMES with a pre-4.1 server (bug#7997).  */
    opt_set_charset= 0;

    /* Don't switch charsets for 4.1 and earlier.  (bug#34192). */
    server_supports_switching_charsets= FALSE;
  } 

  /*
    As we're going to set SQL_MODE, it would be lost on reconnect, so we
    cannot reconnect.
  */
  mysql->reconnect= 0;

  // Set tc_admin=0 to prevent forwarding of SET commands
  set_session_tc_admin();

  my_snprintf(buff, sizeof(buff), "/*!40100 SET @@SQL_MODE='%s' */",
              compatible_mode_normal_str);
  if (mysql_query_with_exception(mysql, 0, buff))
    return true;
  /*
    set time_zone to UTC to allow dumping date types between servers with
    different time zone settings
  */
  if (opt_tz_utc)
  {
    my_snprintf(buff, sizeof(buff), "/*!40103 SET TIME_ZONE='+00:00' */");
    if (mysql_query_with_exception(mysql, 0, buff))
      return true;
  }
  return false;
}

void Dump_Handler::print_comment(const char *format, ...)
{
  static char comment_buff[COMMENT_LENGTH];
  va_list args;

  va_start(args, format);
  my_vsnprintf(comment_buff, COMMENT_LENGTH, format, args);
  va_end(args);

  fputs(comment_buff, result_file);
  check_io(result_file);

  return;
}

/*
  Print the supplied message if in verbose mode

  SYNOPSIS
    verbose_msg()
    fmt   format specifier
    ...   variable number of parameters
*/
void Dump_Handler::verbose_msg(const char *fmt, ...)
{
  va_list args;

  if (!opt_verbose)
    return;

  va_start(args, fmt);
  vfprintf(error_file, fmt, args);
  va_end(args);

  fflush(error_file);

  return;
}

void Dump_Handler::write_header()
{
  print_comment("-- MySQL dump %s  Distrib %s, for %s (%s)\n--\n",
                DUMP_VERSION, MYSQL_SERVER_VERSION, SYSTEM_TYPE,
                MACHINE_TYPE);
  print_comment("-- Host: %s    Database: %s\n",
                target_server->host ? target_server->host : "localhost",
                "");
  print_comment("-- ------------------------------------------------------\n"
                );
  print_comment("-- Server version\t%s\n",
                mysql_connection.server_version);

  if (opt_set_charset)
    fprintf(result_file,
    "\n/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;"
            "\n/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;"
            "\n/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;"
            "\n/*!40101 SET NAMES %s */;\n", default_charset.c_str());

  if (opt_tz_utc)
  {
    fprintf(result_file, "/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;\n");
    fprintf(result_file, "/*!40103 SET TIME_ZONE='+00:00' */;\n");
  }

  fprintf(result_file,
          "/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;\n"
          "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n");
  
  fprintf(result_file,
            "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='%s%s%s' */;\n"
            "/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;\n",
            "NO_AUTO_VALUE_ON_ZERO", compatible_mode_normal_str[0] == 0 ? "" : ",",
            compatible_mode_normal_str);
  // This is specifically to allow MyRocks to bulk load a dump faster
  // We have no interest in anything earlier than 5.7 and 17 being the
  // current release. 5.7.8 and after can only use P_S for session_variables
  // and never I_S. So we first check that P_S is present and the
  // session_variables table exists. If no, we simply skip the optimization
  // assuming that MyRocks isn't present either. If it is, ohh well, bulk
  // loader will not be invoked.
  fprintf(result_file,
        "/*!50717 SELECT COUNT(*) INTO @rocksdb_has_p_s_session_variables"
        " FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA ="
        " 'performance_schema' AND TABLE_NAME = 'session_variables'"
        " */;\n"
        "/*!50717 SET @rocksdb_get_is_supported = IF"
        " (@rocksdb_has_p_s_session_variables, 'SELECT COUNT(*) INTO"
        " @rocksdb_is_supported FROM performance_schema.session_variables"
        " WHERE VARIABLE_NAME=\\'rocksdb_bulk_load\\'', 'SELECT 0') */;\n"
        "/*!50717 PREPARE s FROM @rocksdb_get_is_supported */;\n"
        "/*!50717 EXECUTE s */;\n"
        "/*!50717 DEALLOCATE PREPARE s */;\n"
        "/*!50717 SET @rocksdb_enable_bulk_load = IF"
        " (@rocksdb_is_supported, 'SET SESSION rocksdb_bulk_load = 1',"
        " 'SET @rocksdb_dummy_bulk_load = 0') */;\n"
        "/*!50717 PREPARE s FROM @rocksdb_enable_bulk_load */;\n"
        "/*!50717 EXECUTE s */;\n"
        "/*!50717 DEALLOCATE PREPARE s */;\n");
  
  check_io(result_file);
}

void Dump_Handler::write_footer()
{
  fprintf(result_file,
          "/*!50112 SET @disable_bulk_load = IF (@is_rocksdb_supported,"
          " 'SET SESSION rocksdb_bulk_load = @old_rocksdb_bulk_load',"
          " 'SET @dummy_rocksdb_bulk_load = 0') */;\n"
          "/*!50112 PREPARE s FROM @disable_bulk_load */;\n"
          "/*!50112 EXECUTE s */;\n"
          "/*!50112 DEALLOCATE PREPARE s */;\n");

  if (opt_tz_utc)
    fprintf(result_file,"/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;\n");

  fprintf(result_file,"\n/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;\n");

  fprintf(result_file,
          "/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n"
          "/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;\n");
  if (opt_set_charset)
    fprintf(result_file,
            "/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n"
            "/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n"
            "/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n");
  
  fprintf(result_file,
          "/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;\n");
  
  fputs("\n", result_file);


  char time_str[20];
  get_date(time_str, GETDATE_DATE_TIME, 0);
  print_comment("-- Dump completed on %s\n", time_str);

  check_io(result_file);
}

/**
   Check if the server supports LOCK TABLES FOR BACKUP.

   @returns  TRUE if there is support, FALSE otherwise.
*/
bool Dump_Handler::server_supports_backup_locks()
{
  MYSQL_RES *res;
  MYSQL_ROW row;
  bool rc;

  if (mysql_query_with_exception(mysql, &res,
                                    "SHOW VARIABLES LIKE 'have_backup_locks'"))
    return false;

  if ((row= mysql_fetch_row(res)) == NULL)
  {
    mysql_free_result(res);
    return false;
  }

  rc= mysql_num_fields(res) > 1 && !strcmp(row[1], "YES");

  mysql_free_result(res);

  return rc;
}

int Dump_Handler::start_transaction(MYSQL *mysql_con)
{
  verbose_msg("-- Starting transaction...\n");
  /*
    We want the first consistent read to be used for all tables to dump so we
    need the REPEATABLE READ level (not anything lower, for example READ
    COMMITTED would give one new consistent read per dumped table).
  */
  return (mysql_query_with_exception(mysql_con, 0,
                                        "SET SESSION TRANSACTION ISOLATION "
                                        "LEVEL REPEATABLE READ") ||
          mysql_query_with_exception(mysql_con, 0,
                                        "START TRANSACTION "
                                        "/*!40100 WITH CONSISTENT SNAPSHOT */"));
}

/**
  This function sets the session binlog in the dump file.
  When --set-gtid-purged is used, this function is called to
  disable the session binlog and at the end of the dump, to restore
  the session binlog.

  @param[in]      flag          If FALSE, disable binlog.
                                If TRUE and binlog disabled previously,
                                restore the session binlog.
*/
void Dump_Handler::set_session_binlog(my_bool flag)
{
  if (!flag && !is_binlog_disabled)
  {
    fprintf(result_file,
            "SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN;\n");
    fprintf(result_file, "SET @@SESSION.SQL_LOG_BIN= 0;\n");
    is_binlog_disabled= 1;
  }
  else if (flag && is_binlog_disabled)
  {
    fprintf(result_file,
            "SET @@SESSION.SQL_LOG_BIN = @MYSQLDUMP_TEMP_LOG_BIN;\n");
    is_binlog_disabled= 0;
  }
}

/**
  This function gets the GTID_EXECUTED sets from the
  server and assigns those sets to GTID_PURGED in the
  dump file.

  @param[in]  mysql_con     connection to the server

  @retval     FALSE         succesfully printed GTID_PURGED sets
                             in the dump file.
  @retval     TRUE          failed.

*/
bool Dump_Handler::add_set_gtid_purged(MYSQL *mysql_con)
{
  MYSQL_RES  *gtid_purged_res;
  MYSQL_ROW  gtid_set;
  ulonglong  num_sets, idx;

  /* query to get the GTID_EXECUTED */
  if (mysql_query_with_exception(mysql_con, &gtid_purged_res,
                  "SELECT @@GLOBAL.GTID_EXECUTED"))
    return true;
  /* make sure the mysql query result will be freed properly */
  MYSQL_RES_GUARD(gtid_purged_res);
  
  /* Proceed only if gtid_purged_res is non empty */
  if ((num_sets= mysql_num_rows(gtid_purged_res)) > 0)
  {
    fprintf(result_file,
          "\n--\n-- GTID state at the beginning of the backup \n--\n\n");

    fprintf(result_file,"SET @@GLOBAL.GTID_PURGED='");

    /* formatting is not required, even for multiple gtid sets */
    for (idx= 0; idx< num_sets-1; idx++)
    {
      gtid_set= mysql_fetch_row(gtid_purged_res);
      fprintf(result_file,"%s,", (char*)gtid_set[0]);
    }
    /* for the last set */
    gtid_set= mysql_fetch_row(gtid_purged_res);
    /* close the SET expression */
    fprintf(result_file,"%s';\n", (char*)gtid_set[0]);
  }

  return false;  /*success */
}

/**
  This function processes the opt_set_gtid_purged option.
  This function also calls set_session_binlog() function before
  setting the SET @@GLOBAL.GTID_PURGED in the output.

  @param[in]          mysql_con     the connection to the server

  @retval             FALSE         successful according to the value
                                    of opt_set_gtid_purged.
  @retval             TRUE          fail.
*/
bool Dump_Handler::process_set_gtid_purged(MYSQL* mysql_con)
{
  MYSQL_RES  *gtid_mode_res;
  MYSQL_ROW  gtid_mode_row;
  char       *gtid_mode_val= 0;
  char buf[32], query[64];

  if (opt_set_gtid_purged_mode == SET_GTID_PURGED_OFF)
    return false;  /* nothing to be done */

  /*
    Check if the server has the knowledge of GTIDs(pre mysql-5.6)
    or if the gtid_mode is ON or OFF.
  */
  my_snprintf(query, sizeof(query), "SHOW VARIABLES LIKE %s",
              quote_for_like("gtid_mode", buf));

  if (mysql_query_with_exception(mysql_con, &gtid_mode_res, query))
    return true;

  MYSQL_RES_GUARD(gtid_mode_res);

  gtid_mode_row = mysql_fetch_row(gtid_mode_res);

  /*
     gtid_mode_row is NULL for pre 5.6 versions. For versions >= 5.6,
     get the gtid_mode value from the second column.
  */
  gtid_mode_val = gtid_mode_row ? (char*)gtid_mode_row[1] : NULL;

  if (gtid_mode_val && strcmp(gtid_mode_val, "OFF"))
  {
    /*
       For any gtid_mode !=OFF and irrespective of --set-gtid-purged
       being AUTO or ON,  add GTID_PURGED in the output.
    */
    if (opt_databases || !opt_alldbs || !opt_dump_triggers
        || !opt_routines || !opt_events)
    {
      fprintf(error_file,
              "Warning: A partial dump from a server that has GTIDs will "
              "by default include the GTIDs of all transactions, even "
              "those that changed suppressed parts of the database. If "
              "you don't want to restore GTIDs, pass "
              "--set-gtid-purged=OFF. To make a complete dump, pass "
              "--all-databases --triggers --routines --events. \n");
    }

    set_session_binlog(FALSE);
    if (add_set_gtid_purged(mysql_con))
    {
      return true;
    }
  }
  else /* gtid_mode is off */
  {
    if (opt_set_gtid_purged_mode == SET_GTID_PURGED_ON)
    {
      fprintf(error_file, "Error: Server has GTIDs disabled.\n");
      return true;
    }
  }

  return false;
}

/*
** dbDisconnect -- disconnects from the host.
*/
void Dump_Handler::dbDisconnect()
{
  verbose_msg("-- Disconnecting from %s:%d...\n", 
              target_server->host ? target_server->host : "localhost",
              target_server->port);
  mysql_close(mysql);
}

int Dump_Handler::dump_tablespaces(char* ts_where)
{
  MYSQL_ROW row;
  MYSQL_RES *tableres;
  char buf[FN_REFLEN];
  DYNAMIC_STRING sqlbuf;
  int first= 0;
  /*
    The following are used for parsing the EXTRA field
  */
  char extra_format[]= "UNDO_BUFFER_SIZE=";
  char *ubs;
  char *endsemi;
  DBUG_ENTER("Dump_Handler::dump_tablespaces");

  { /* section 1 begin */
  init_dynamic_string_checked(&sqlbuf,
                      "SELECT LOGFILE_GROUP_NAME,"
                      " FILE_NAME,"
                      " TOTAL_EXTENTS,"
                      " INITIAL_SIZE,"
                      " ENGINE,"
                      " EXTRA"
                      " FROM INFORMATION_SCHEMA.FILES"
                      " WHERE FILE_TYPE = 'UNDO LOG'"
                      " AND FILE_NAME IS NOT NULL"
                      " AND LOGFILE_GROUP_NAME IS NOT NULL",
                      256, 1024);
  
  /* make sure the dynamic string will be freed properly */
  DYNAMIC_STR_GUARD dstr_guard(&sqlbuf);

  if(ts_where)
  {
    dynstr_append_checked(&sqlbuf,
                  " AND LOGFILE_GROUP_NAME IN ("
                  "SELECT DISTINCT LOGFILE_GROUP_NAME"
                  " FROM INFORMATION_SCHEMA.FILES"
                  " WHERE FILE_TYPE = 'DATAFILE'"
                  );
    dynstr_append_checked(&sqlbuf, ts_where);
    dynstr_append_checked(&sqlbuf, ")");
  }
  dynstr_append_checked(&sqlbuf,
                " GROUP BY LOGFILE_GROUP_NAME, FILE_NAME"
                ", ENGINE, TOTAL_EXTENTS, INITIAL_SIZE"
                " ORDER BY LOGFILE_GROUP_NAME");

  if (mysql_real_query(mysql, sqlbuf.str, sqlbuf.length) ||
      !(tableres = mysql_store_result(mysql)))
  {
    if (mysql_errno(mysql) == ER_BAD_TABLE_ERROR ||
        mysql_errno(mysql) == ER_BAD_DB_ERROR ||
        mysql_errno(mysql) == ER_UNKNOWN_TABLE)
    {
      fprintf(result_file,
              "\n--\n-- Not dumping tablespaces as no INFORMATION_SCHEMA.FILES"
              " table on this server\n--\n");
      check_io(result_file);
      DBUG_RETURN(0);
    }
    
    fprintf(error_file, "Error: '%s' when trying to dump tablespaces\n",
                    mysql_error(mysql));
    fflush(error_file);
    DBUG_RETURN(1);
  }
  
  /* make sure the result set will be freed properly */
  MYSQL_RES_GUARD(tableres);

  buf[0]= 0;
  while ((row= mysql_fetch_row(tableres)))
  {
    if (strcmp(buf, row[0]) != 0)
      first= 1;
    if (first)
    {
      print_comment("\n--\n-- Logfile group: %s\n--\n", row[0]);
      fprintf(result_file, "\nCREATE");
    }
    else
    {
      fprintf(result_file, "\nALTER");
    }
    fprintf(result_file,
            " LOGFILE GROUP %s\n"
            "  ADD UNDOFILE '%s'\n",
            row[0],
            row[1]);
    if (first)
    {
      ubs= strstr(row[5],extra_format);
      if(!ubs)
        break;
      ubs+= strlen(extra_format);
      endsemi= strstr(ubs,";");
      if(endsemi)
        endsemi[0]= '\0';
      fprintf(result_file,
              "  UNDO_BUFFER_SIZE %s\n",
              ubs);
    }
    fprintf(result_file,
            "  INITIAL_SIZE %s\n"
            "  ENGINE=%s;\n",
            row[3],
            row[4]);
    check_io(result_file);
    if (first)
    {
      first= 0;
      strxmov(buf, row[0], NullS);
    }
  }
  } /* section 1 end */

  { /* section 2 begin */
  init_dynamic_string_checked(&sqlbuf,
                      "SELECT DISTINCT TABLESPACE_NAME,"
                      " FILE_NAME,"
                      " LOGFILE_GROUP_NAME,"
                      " EXTENT_SIZE,"
                      " INITIAL_SIZE,"
                      " ENGINE"
                      " FROM INFORMATION_SCHEMA.FILES"
                      " WHERE FILE_TYPE = 'DATAFILE'",
                      256, 1024);
  
  /* make sure the dynamic string will be freed properly */
  DYNAMIC_STR_GUARD dstr_guard(&sqlbuf);

  if(ts_where)
    dynstr_append_checked(&sqlbuf, ts_where);

  dynstr_append_checked(&sqlbuf, " ORDER BY TABLESPACE_NAME, LOGFILE_GROUP_NAME");

  if (mysql_query_with_exception(mysql, &tableres, sqlbuf.str))
  {
    DBUG_RETURN(1);
  }
  
  /* make sure the result set will be freed properly */
  MYSQL_RES_GUARD(tableres);

  buf[0]= 0;
  while ((row= mysql_fetch_row(tableres)))
  {
    if (strcmp(buf, row[0]) != 0)
      first= 1;
    if (first)
    {
      print_comment("\n--\n-- Tablespace: %s\n--\n", row[0]);
      fprintf(result_file, "\nCREATE");
    }
    else
    {
      fprintf(result_file, "\nALTER");
    }
    fprintf(result_file,
            " TABLESPACE %s\n"
            "  ADD DATAFILE '%s'\n",
            row[0],
            row[1]);
    if (first)
    {
      fprintf(result_file,
              "  USE LOGFILE GROUP %s\n"
              "  EXTENT_SIZE %s\n",
              row[2],
              row[3]);
    }
    fprintf(result_file,
            "  INITIAL_SIZE %s\n"
            "  ENGINE=%s;\n",
            row[4],
            row[5]);
    check_io(result_file);
    if (first)
    {
      first= 0;
      strxmov(buf, row[0], NullS);
    }
  }
  } /* section 2 end */

  DBUG_RETURN(0);
}

/*
  dump all logfile groups and tablespaces
*/
int Dump_Handler::dump_all_tablespaces()
{
  return dump_tablespaces(NULL);
}

int Dump_Handler::is_ndbinfo(const char* dbname)
{
  if (!checked_ndbinfo)
  {
    MYSQL_RES *res;
    MYSQL_ROW row;
    char buf[32], query[64];

    my_snprintf(query, sizeof(query),
                "SHOW VARIABLES LIKE %s",
                quote_for_like("ndbinfo_version", buf));

    checked_ndbinfo= 1;

    if (mysql_query_with_exception(mysql, &res, query))
      return 0;

    if (!(row= mysql_fetch_row(res)))
    {
      mysql_free_result(res);
      return 0;
    }

    have_ndbinfo= 1;
    mysql_free_result(res);
  }

  if (!have_ndbinfo)
    return 0;

  if (my_strcasecmp(&my_charset_latin1, dbname, "ndbinfo") == 0)
    return 1;

  return 0;
}

/* Return true if we should dump the database */
bool Dump_Handler::include_database(const char *dbname, size_t dbname_len)
{
  return ignore_databases.find(string(dbname, dbname_len))
          == ignore_databases.end();
}

/* Return true if we should dump the table */
bool Dump_Handler::include_table(const char *tbname, size_t tbname_len)
{
  return ignore_tables.find(string(tbname, tbname_len))
          == ignore_tables.end();
}

my_bool Dump_Handler::test_if_special_chars(const char *str)
{
  for ( ; *str ; str++)
    if (!my_isvar(charset_info,*str) && *str != '$')
      return 1;
  return 0;
}

/*
  quote_name(name, buff, force)

  Quotes char string, taking into account compatible mode

  Args

  name                 Unquoted string containing that which will be quoted
  buff                 The buffer that contains the quoted value, also returned
  force                Flag to make it ignore 'test_if_special_chars'

  Returns

  buff                 quoted string

*/
char * Dump_Handler::quote_name(const char *name, char *buff, my_bool force)
{
  char *to= buff;
  char qtype= '`';

  if (!force && !opt_quoted && !test_if_special_chars(name))
    return (char*) name;
  *to++= qtype;
  while (*name)
  {
    if (*name == qtype)
      *to++= qtype;
    *to++= *name++;
  }
  to[0]= qtype;
  to[1]= 0;
  return buff;
}

int Dump_Handler::init_dumping(const char *database, 
                               const std::function<int(char *)> &init_func)
{
  if (is_ndbinfo(database))
  {
    verbose_msg("-- Skipping dump of ndbinfo database\n");
    return 0;
  }

  if (mysql_select_db(mysql, database))
  {
    throw DB_exception(mysql, "when selecting the database");
    return 1;                   /* If --force */
  }

  if (opt_databases || opt_alldbs)
  {
    /*
      length of table name * 2 (if name contains quotes), 2 quotes and 0
    */
    char quoted_database_buf[NAME_LEN*2+3];
    char *qdatabase= quote_name(database,quoted_database_buf,opt_quoted);

    print_comment("\n--\n-- Current Database: %s\n--\n",
                  fix_identifier_with_newline(qdatabase));

    /* Call the view or table specific function */
    init_func(qdatabase);

    fprintf(result_file,"\nUSE %s;\n", qdatabase);
    check_io(result_file);
  }

  return 0;
}

/*
Table Specific database initalization.

SYNOPSIS
  init_dumping_tables
  qdatabase      quoted name of the database

RETURN VALUES
  0        Success.
  1        Failure.
*/
int Dump_Handler::init_dumping_tables(char *qdatabase)
{
  DBUG_ENTER("Dump_Handler::init_dumping_tables");

  char qbuf[256];
  MYSQL_ROW row;
  MYSQL_RES *dbinfo;

  my_snprintf(qbuf, sizeof(qbuf),
              "SHOW CREATE DATABASE IF NOT EXISTS %s",
              qdatabase);

  if (mysql_real_query(mysql, qbuf, strlen(qbuf)) || 
      !(dbinfo = mysql_store_result(mysql)))
  {
    /* Old server version, dump generic CREATE DATABASE */
    if (opt_drop_database)
      fprintf(result_file,
              "\n/*!40000 DROP DATABASE IF EXISTS %s*/;\n",
              qdatabase);
    fprintf(result_file,
            "\nCREATE DATABASE /*!32312 IF NOT EXISTS*/ %s;\n",
            qdatabase);
  }
  else
  {
    if (opt_drop_database)
      fprintf(result_file,
              "\n/*!40000 DROP DATABASE IF EXISTS %s*/;\n",
              qdatabase);
    row = mysql_fetch_row(dbinfo);
    if (row[1])
    {
      fprintf(result_file,"\n%s;\n",row[1]);
    }
    mysql_free_result(dbinfo);
  }

  DBUG_RETURN(0);
}

char *Dump_Handler::getTableName(int reset)
{
  static MYSQL_RES *res= NULL;
  MYSQL_ROW    row;

  if (!res)
  {
    if (!(res= client_mysql_list_tables(mysql,NullS)))
      return(NULL);
  }
  if ((row= mysql_fetch_row(res)))
    return((char*) row[0]);

  if (reset)
    client_mysql_data_seek(res,0);      /* We want to read again */
  else
  {
    mysql_free_result(res);
    res= NULL;
  }
  return(NULL);
}

/*
  SYNOPSIS

  Check if the table is one of the table types that should be ignored:
  MRG_ISAM, MRG_MYISAM.

  If the table should be altogether ignored, it returns a TRUE, FALSE if it
  should not be ignored.

  ARGS

    check_if_ignore_table()
    table_name                  Table name to check
    table_type                  Type of table

  GLOBAL VARIABLES
    mysql                       MySQL connection
    verbose                     Write warning messages

  RETURN
    char (bit value)            See IGNORE_ values at top
*/
char Dump_Handler::check_if_ignore_table(const char *table_name, char *table_type)
{
  char result= IGNORE_NONE;
  char buff[FN_REFLEN+80], show_name_buff[FN_REFLEN];
  MYSQL_RES *res= NULL;
  MYSQL_ROW row;
  DBUG_ENTER("Dump_Handler::check_if_ignore_table");

  /* Check memory for quote_for_like() */
  DBUG_ASSERT(2*sizeof(table_name) < sizeof(show_name_buff));
  my_snprintf(buff, sizeof(buff), "show table status like %s",
              quote_for_like(table_name, show_name_buff));
  if (mysql_query_with_exception(mysql, &res, buff))
  {
    if (mysql_errno(mysql) != ER_PARSE_ERROR)
    {                                   /* If old MySQL version */
      verbose_msg("-- Warning: Couldn't get status information for "
                  "table %s (%s)\n", table_name, mysql_error(mysql));
      DBUG_RETURN(result);                       /* assume table is ok */
    }
  }
  if (!(row= mysql_fetch_row(res)))
  {
    fprintf(error_file,
            "Error: Couldn't read status information for table %s (%s)\n",
            table_name, mysql_error(mysql));
    mysql_free_result(res);
    DBUG_RETURN(result);                         /* assume table is ok */
  }
  if (!(row[1]))
    strmake(table_type, "VIEW", NAME_LEN-1);
  else
  {
    strmake(table_type, row[1], NAME_LEN-1);

    /*  If these two types, we want to skip dumping the table. */
    if (!opt_no_data &&
        (!my_strcasecmp(&my_charset_latin1, table_type, "MRG_MyISAM") ||
         !strcmp(table_type,"MRG_ISAM") ||
         !strcmp(table_type,"FEDERATED")))
      result= IGNORE_DATA;
  }
  mysql_free_result(res);
  DBUG_RETURN(result);
}

/*
  Check if the table has a primary key defined either explicitly or
  implicitly (i.e. a unique key on non-nullable columns).

  SYNOPSIS
    my_bool has_primary_key(const char *table_name)

    table_name  quoted table name

  RETURNS     TRUE if the table has a primary key

  DESCRIPTION
*/
my_bool Dump_Handler::has_primary_key(const char *table_name)
{
  MYSQL_RES  *res= NULL;
  MYSQL_ROW  row;
  char query_buff[QUERY_LENGTH];
  my_bool has_pk= TRUE;

  my_snprintf(query_buff, sizeof(query_buff),
              "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE "
              "TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND "
              "COLUMN_KEY='PRI'", table_name);
  if (mysql_real_query(mysql, query_buff, strlen(query_buff)) || 
      !(res= mysql_store_result(mysql)) ||
      !(row= mysql_fetch_row(res)))
  {
    fprintf(error_file, "%s: Warning: Couldn't determine if table %s has a "
            "primary key (%s). "
            "--innodb-optimize-keys may work inefficiently.\n",
            tcdump_progname, table_name, mysql_error(mysql));
    goto cleanup;
  }

  has_pk= atoi(row[0]) > 0;

cleanup:
  if (res)
    mysql_free_result(res);

  return has_pk;
}

/*
  Get string of comma-separated primary key field names

  SYNOPSIS
    char *primary_key_fields(const char *table_name)
    RETURNS     pointer to allocated buffer (must be freed by caller)
    table_name  quoted table name

  DESCRIPTION
    Use SHOW KEYS FROM table_name, allocate a buffer to hold the
    field names, and then build that string and return the pointer
    to that buffer.

    Returns NULL if there is no PRIMARY or UNIQUE key on the table,
    or if there is some failure.  It is better to continue to dump
    the table unsorted, rather than exit without dumping the data.
*/
char * Dump_Handler::primary_key_fields(const char *table_name, const my_bool desc)
{
  MYSQL_RES  *res= NULL;
  MYSQL_ROW  row;
  /* SHOW KEYS FROM + table name * 2 (escaped) + 2 quotes + \0 */
  char show_keys_buff[15 + NAME_LEN * 2 + 3];
  size_t result_length= 0;
  char *result= 0;
  char buff[NAME_LEN * 2 + 3];
  char *quoted_field;
  static const char *desc_index= " DESC";

  my_snprintf(show_keys_buff, sizeof(show_keys_buff),
              "SHOW KEYS FROM %s", table_name);
  if (mysql_real_query(mysql, show_keys_buff, strlen(show_keys_buff)) ||
      !(res= mysql_store_result(mysql)))
  {
    fprintf(error_file, "Warning: Couldn't read keys from table %s;"
            " records are NOT sorted (%s)\n",
            table_name, mysql_error(mysql));
    /* Don't exit, because it's better to print out unsorted records */
    goto cleanup;
  }

  /*
   * Figure out the length of the ORDER BY clause result.
   * Note that SHOW KEYS is ordered:  a PRIMARY key is always the first
   * row, and UNIQUE keys come before others.  So we only need to check
   * the first key, not all keys.
   */
  if ((row= mysql_fetch_row(res)) && atoi(row[1]) == 0)
  {
    /* Key is unique */
    do
    {
      quoted_field= quote_name(row[4], buff, 0);
      result_length+= strlen(quoted_field) + 1; /* + 1 for ',' or \0 */
      if (desc)
      {
        result_length+= strlen(desc_index);
      }
    } while ((row= mysql_fetch_row(res)) && atoi(row[3]) > 1);
  }

  /* Build the ORDER BY clause result */
  if (result_length)
  {
    char *end;
    /* result (terminating \0 is already in result_length) */
    result= (char *)my_malloc(PSI_NOT_INSTRUMENTED,
                        result_length + 10, MYF(MY_WME));
    if (!result)
    {
      fprintf(error_file, "Error: Not enough memory to store ORDER BY clause\n");
      goto cleanup;
    }
    client_mysql_data_seek(res, 0);
    row= mysql_fetch_row(res);
    quoted_field= quote_name(row[4], buff, 0);
    end= my_stpcpy(result, quoted_field);
    while ((row= mysql_fetch_row(res)) && atoi(row[3]) > 1)
    {
      quoted_field= quote_name(row[4], buff, 0);
      end= strxmov(end, desc ? " DESC," : ",", quoted_field, NullS);
    }
    if (desc)
    {
      end= my_stpmov(end, " DESC");
    }
  }

cleanup:
  if (res)
    mysql_free_result(res);

  return result;
}

/**
  Switch charset for results to some specified charset.  If the server does not
  support character_set_results variable, nothing can be done here.  As for
  whether something should be done here, future new callers of this function
  should be aware that the server lacking the facility of switching charsets is
  treated as success.

  @note  If the server lacks support, then nothing is changed and no error
         condition is returned.

  @returns  whether there was an error or not
*/
int Dump_Handler::switch_character_set_results(const char *cs_name)
{
  char query_buffer[QUERY_LENGTH];
  size_t query_length;

  /* Server lacks facility.  This is not an error, by arbitrary decision . */
  if (!server_supports_switching_charsets)
    return FALSE;

  query_length= my_snprintf(query_buffer,
                            sizeof (query_buffer),
                            "SET SESSION character_set_results = '%s'",
                            (const char *) cs_name);

  return mysql_real_query(mysql, query_buffer, (ulong)query_length);
}

/**
  Checks if --add-drop-table option is enabled and prints
  "DROP TABLE IF EXISTS ..." if the specified table is not a log table.

  @param sq_file            output file
  @param db                 db name
  @param table              table name
  @param opt_quoted_table   optionally quoted table name
*/
void Dump_Handler::print_optional_drop_table(FILE *sql_file, const char* db,
                                            const char *table,
                                            const char *opt_quoted_table)
{
  DBUG_ENTER("Dump_Handler::print_optional_drop_table");
  DBUG_PRINT("enter", ("db: %s  table: %s", db, table));
  if (opt_drop_table)
  {
    if (!(general_log_or_slow_log_tables(db, table) ||
          replication_metadata_tables(db, table)))
    {
      fprintf(sql_file, "DROP TABLE IF EXISTS %s;\n", opt_quoted_table);
      check_io(sql_file);
    }
  }
  DBUG_VOID_RETURN;
}

/*
  Find a node in the skipped keys list whose name matches a quoted
  identifier specified as 'id_from' and 'id_to' arguments.
*/

LIST * Dump_Handler::find_matching_skipped_key(const char *id_from,
                                              const char *id_to)
{
  LIST *list;
  size_t id_len;

  id_len= id_to - id_from + 1;
  DBUG_ASSERT(id_len > 2);

  for (list= skipped_keys_list; list; list= list_rest(list))
  {
    const char *keydef;
    const char *keyname_from;
    const char *keyname_to;
    size_t keyname_len;

    keydef= (char *)(list->data);

    if ((keyname_from= parse_quoted_identifier(keydef, &keyname_to)))
    {
      keyname_len= keyname_to - keyname_from + 1;

      if (id_len == keyname_len &&
          !strncmp(keyname_from, id_from, id_len))
        return list;
    }
  }

  return NULL;
}

/*
  Parse the specified key definition string and check if the key contains an
  AUTO_INCREMENT column as the first key part. We only check for the first key
  part, because unlike MyISAM, InnoDB does not allow the AUTO_INCREMENT column
  as a secondary key column, i.e. the AUTO_INCREMENT column would not be
  considered indexed for such key specification.
*/
my_bool Dump_Handler::contains_autoinc_column(const char *autoinc_column,
                                              const char *keydef,
                                              key_type_t type)
{
  const char *from, *to;
  uint idnum;

  DBUG_ASSERT(type != KEY_TYPE_NONE);

  if (autoinc_column == NULL)
    return FALSE;

  idnum= 0;

  /*
    There is only 1 iteration of the following loop for type == KEY_TYPE_PRIMARY
    and 2 iterations for type == KEY_TYPE_UNIQUE / KEY_TYPE_NON_UNIQUE.
  */
  while ((from= parse_quoted_identifier(keydef, &to)))
  {
    idnum++;

    /*
      Skip the check if it's the first identifier and we are processing a
      secondary key.
    */
    if ((type == KEY_TYPE_PRIMARY || idnum != 1) &&
        !strncmp(autoinc_column, from + 1, to - from - 1))
      return TRUE;

    /*
      Check only the first (for PRIMARY KEY) or the second (for secondary keys)
      quoted identifier.
    */
    if (idnum == 1 + MY_TEST(type != KEY_TYPE_PRIMARY))
      break;

    keydef= to + 1;
  }

  return FALSE;
}

/*
  Remove secondary/foreign key definitions from a given SHOW CREATE TABLE string
  and store them into a temporary list to be used later.

  SYNOPSIS
    skip_secondary_keys()
    create_str                SHOW CREATE TABLE output
    has_pk                    TRUE, if the table has PRIMARY KEY
                              (or UNIQUE key on non-nullable columns)


  DESCRIPTION

    Stores all lines starting with "KEY" or "UNIQUE KEY"
    into skipped_keys_list and removes them from the input string.
    Ignoring FOREIGN KEYS constraints when creating the table is ok, because
    mysqldump sets foreign_key_checks to 0 anyway.
*/
void Dump_Handler::skip_secondary_keys(char *create_str, my_bool has_pk)
{
  char *ptr, *strend;
  char *last_comma= NULL;
  my_bool pk_processed= FALSE;
  char *autoinc_column= NULL;
  my_bool has_autoinc= FALSE;
  key_type_t type;
  const char *constr_from;
  const char *constr_to;
  LIST *keydef_node;
  my_bool keys_processed= FALSE;

  strend= create_str + strlen(create_str);

  ptr= create_str;
  while (*ptr && !keys_processed)
  {
    char *tmp, *orig_ptr, c;

    orig_ptr= ptr;
    /* Skip leading whitespace */
    while (*ptr && my_isspace(charset_info, *ptr))
      ptr++;

    /* Read the next line */
    for (tmp= ptr; *tmp != '\n' && *tmp != '\0'; tmp++);

    c= *tmp;
    *tmp= '\0'; /* so strstr() only processes the current line */

    if (!strncmp(ptr, "CONSTRAINT ", sizeof("CONSTRAINT ") - 1) &&
        (constr_from= parse_quoted_identifier(ptr, &constr_to)) &&
        (keydef_node= find_matching_skipped_key(constr_from, constr_to)))
    {
      char *keydef;
      size_t keydef_len;

      /*
        There's a skipped key with the same name as the constraint name.  Let's
        put it back before the current constraint definition and remove from the
        skipped keys list.
      */
      keydef= (char *)(keydef_node->data);
      /* 
        The original key definition had the following format
        "  <keydef>,\n"
        ("  " (two spaces) followed by key definition, followed by ",\n").
        For instance "  KEY `a` (`a`),\n".
        Therefore, when the definition was removed, the data was shifted by
        strlen(keydef) + 4 characters.
      */
      keydef_len= strlen(keydef) + 4;

      memmove(orig_ptr + keydef_len, orig_ptr, strend - orig_ptr + 1);
      memcpy(orig_ptr, "  ", 2);
      memcpy(orig_ptr + 2, keydef, keydef_len - 4);
      memcpy(orig_ptr + keydef_len - 2, ",\n", 2);

      skipped_keys_list= list_delete(skipped_keys_list, keydef_node);
      my_free(keydef);
      my_free(keydef_node);

      strend+= keydef_len;
      orig_ptr+= keydef_len;
      ptr+= keydef_len;
      tmp+= keydef_len;

      type= KEY_TYPE_NONE;
    }
    else if (!strncmp(ptr, "UNIQUE KEY ", sizeof("UNIQUE KEY ") - 1))
      type= KEY_TYPE_UNIQUE;
    else if (!strncmp(ptr, "KEY ", sizeof("KEY ") - 1))
      type= KEY_TYPE_NON_UNIQUE;
    else if (!strncmp(ptr, "PRIMARY KEY ", sizeof("PRIMARY KEY ") - 1))
      type= KEY_TYPE_PRIMARY;
    else
      type= KEY_TYPE_NONE;

    has_autoinc= (type != KEY_TYPE_NONE) ?
      contains_autoinc_column(autoinc_column, ptr, type) : FALSE;

    /* Is it a secondary index definition? */
    if (c == '\n' &&
        ((type == KEY_TYPE_UNIQUE && (pk_processed || !has_pk)) ||
         type == KEY_TYPE_NON_UNIQUE) && !has_autoinc)
    {
      char *data, *end= tmp - 1;

      /* Remove the trailing comma */
      if (*end == ',')
        end--;
      data= my_strndup(PSI_NOT_INSTRUMENTED, ptr, end - ptr + 1, MYF(MY_FAE));
      skipped_keys_list= list_cons(data, skipped_keys_list);

      memmove(orig_ptr, tmp + 1, strend - tmp);
      ptr= orig_ptr;
      strend-= tmp + 1 - ptr;

      /* Remove the comma on the previos line */
      if (last_comma != NULL)
      {
        *last_comma= ' ';
      }
    }
    else
    {
      char *end;

      if (last_comma != NULL && *ptr == ')')
      {
        keys_processed= TRUE;
      }
      else if (last_comma != NULL && !keys_processed)
      {
        /*
          It's not the last line of CREATE TABLE, so we have skipped a key
          definition. We have to restore the last removed comma.
        */
        *last_comma= ',';
      }

      /*
        If we are skipping a key which indexes an AUTO_INCREMENT column, it is
        safe to optimize all subsequent keys, i.e. we should not be checking for
        that column anymore.
      */
      if (type != KEY_TYPE_NONE && has_autoinc)
      {
          DBUG_ASSERT(autoinc_column != NULL);

          my_free(autoinc_column);
          autoinc_column= NULL;
      }

      if ((has_pk && type == KEY_TYPE_UNIQUE && !pk_processed) ||
          type == KEY_TYPE_PRIMARY)
        pk_processed= TRUE;

      if (strstr(ptr, "AUTO_INCREMENT") && *ptr == '`')
      {
        /*
          The first secondary key defined on this column later cannot be
          skipped, as CREATE TABLE would fail on import. Unless there is a
          PRIMARY KEY and it indexes that column.
        */
        for (end= ptr + 1;
             /* Skip double backticks as they are a part of identifier */
             *end != '\0' && (*end != '`' || end[1] == '`');
             end++)
          /* empty */;

        if (*end == '`' && end > ptr + 1)
        {
          DBUG_ASSERT(autoinc_column == NULL);

          autoinc_column= my_strndup(PSI_NOT_INSTRUMENTED, ptr + 1,
                                     end - ptr - 1, MYF(MY_FAE));
        }
      }

      *tmp= c;

      if (tmp[-1] == ',')
        last_comma= tmp - 1;
      ptr= (*tmp == '\0') ? tmp : tmp + 1;
    }
  }

  my_free(autoinc_column);
}

/**
  Unquotes char string, taking into account compatible mode

  @param opt_quoted_name   Optionally quoted string
  @param buff              The buffer that will contain the unquoted value,
                           may be returned

  @return Pointer to unquoted string (either original opt_quoted_name or
          buff).
*/
char * Dump_Handler::unquote_name(const char *opt_quoted_name, char *buff)
{
  char *to= buff;
  const char qtype= '`';

  if (!opt_quoted)
    return (char*)opt_quoted_name;
  if (*opt_quoted_name != qtype)
  {
    DBUG_ASSERT(strchr(opt_quoted_name, qtype) == 0);
    return (char*)opt_quoted_name;
  }

  ++opt_quoted_name;
  while (*opt_quoted_name)
  {
    if (*opt_quoted_name == qtype)
    {
      ++opt_quoted_name;
      if (*opt_quoted_name == qtype)
        *to++= qtype;
      else
      {
        DBUG_ASSERT(*opt_quoted_name == '\0');
      }
    }
    else
    {
      *to++= *opt_quoted_name++;
    }
  }
  to[0]= 0;
  return buff;
}

void Dump_Handler::unescape(FILE *file,char *pos, size_t length)
{
  char *tmp;
  DBUG_ENTER("Dump_Handler::unescape");
  if (!(tmp=(char*) my_malloc(PSI_NOT_INSTRUMENTED,
                              length*2+1, MYF(MY_WME))))
    throw TC_Dump_exception("Couldn't allocate memory");


  client_mysql_real_escape_string_quote(&mysql_connection, tmp, pos, (ulong)length, '\'');
  fputc('\'', file);
  fputs(tmp, file);
  fputc('\'', file);
  my_free(tmp);
  check_io(file);
  DBUG_VOID_RETURN;
}

/**
  Removes some compressed columns extensions from the create table
  definition (a string produced by SHOW CREATE TABLE) depending on
  opt_compressed_columns and opt_compressed_columns_with_dictionaries flags.
  If opt_compressed_columns_with_dictionaries flags is true, in addition
  dictionaries list will be filled with referenced compression
  dictionaries.

  @param create_str     SHOW CREATE TABLE output
  @param dictionaries   the list of dictionary names found in the
                        create table definition
*/
void Dump_Handler::skip_compressed_columns(char *create_str, LIST **dictionaries)
{
  static const char prefix[]=
    " /*!"
    STRINGIFY_ARG(FIRST_SUPPORTED_COMPRESSED_COLUMNS_VERSION)
    " COLUMN_FORMAT COMPRESSED";
  static const size_t prefix_length= sizeof(prefix) - 1;
  static const char suffix[]= " */";
  static const size_t suffix_length= sizeof(suffix) - 1;
  static const char dictionary_keyword[]=" WITH COMPRESSION_DICTIONARY ";
  static const size_t dictionary_keyword_length=
    sizeof(dictionary_keyword) - 1;

  char *ptr, *end_ptr, *prefix_ptr, *suffix_ptr, *dictionary_keyword_ptr;

  DBUG_ENTER("Dump_Handler::skip_compressed_columns");

  ptr= create_str;
  end_ptr= ptr + strlen(create_str);

  bool opt_compressed_columns_with_dictionaries = false;
  bool opt_compressed_columns = false;

  if (opt_compressed_columns_with_dictionaries && dictionaries != 0)
    *dictionaries= 0;

  while ((prefix_ptr= strstr(ptr, prefix)) != 0)
  {
    suffix_ptr= strstr(prefix_ptr + prefix_length, suffix);
    DBUG_ASSERT(suffix_ptr != 0);
    if (!opt_compressed_columns_with_dictionaries)
    {
      if (!opt_compressed_columns)
      {
        /* Strip out all compressed columns extensions. */
        memmove(prefix_ptr, suffix_ptr + suffix_length,
                end_ptr - (suffix_ptr + suffix_length) + 1);
        end_ptr-= suffix_ptr + suffix_length - prefix_ptr;
        ptr= prefix_ptr;
      }
      else
      {
        /* Strip out only compression dictionary references. */
        memmove(prefix_ptr + prefix_length, suffix_ptr,
          end_ptr - suffix_ptr + 1);
        end_ptr-= suffix_ptr - (prefix_ptr + prefix_length);
        ptr= prefix_ptr + prefix_length + suffix_length;
      }
    }
    else
    {
      /* Do not strip out anything. Leave full column definition as is. */
      if (dictionaries !=0 && prefix_ptr + prefix_length != suffix_ptr)
      {
        char *dictionary_name_ptr;
        size_t dictionary_name_length;
        char opt_quoted_buff[NAME_LEN * 2 + 3],
             unquoted_buff[NAME_LEN * 2 + 3];

        dictionary_keyword_ptr= strstr(prefix_ptr + prefix_length,
                                       dictionary_keyword);
        DBUG_ASSERT(dictionary_keyword_ptr < suffix_ptr);
        dictionary_name_length= suffix_ptr -
          (dictionary_keyword_ptr + dictionary_keyword_length);

        strncpy(opt_quoted_buff,
          dictionary_keyword_ptr + dictionary_keyword_length,
          dictionary_name_length);
        opt_quoted_buff[dictionary_name_length]= '\0';

        dictionary_name_ptr=
          my_strdup(PSI_NOT_INSTRUMENTED,
                    unquote_name(opt_quoted_buff, unquoted_buff), MYF(0));
        if (dictionary_name_ptr == 0)
          throw TC_Dump_exception("Couldn't allocate memory");

        list_push(*dictionaries, dictionary_name_ptr);
      }
      ptr= suffix_ptr + suffix_length;
    }
  }
  DBUG_VOID_RETURN;
}

/**
  Prints "CREATE COMPRESSION_DICTIONARY ..." statement for the specified
  dictionary name if this is the first time this dictionary is referenced.

  @param sql_file          output file
  @param dictionary_name   dictionary name
*/
void Dump_Handler::print_optional_create_compression_dictionary(FILE* sql_file,
  const char *dictionary_name)
{
  DBUG_ENTER("Dump_Handler::print_optional_create_compression_dictionary");
  DBUG_PRINT("enter", ("dictionary: %s", dictionary_name));

  /*
    We skip this compression dictionary if it has already been processed
  */
  if (my_hash_search(&processed_compression_dictionaries,
    (const uchar *)dictionary_name, strlen(dictionary_name)) == 0)
  {
    static const char get_zip_dict_data_stmt[] =
      "SELECT `ZIP_DICT` "
      "FROM `INFORMATION_SCHEMA`.`XTRADB_ZIP_DICT` "
      "WHERE `NAME` = '%s'";

    char quoted_buff[NAME_LEN * 2 + 3];
    char *quoted_dictionary_name;
    char query_buff[QUERY_LENGTH];
    MYSQL_RES *result= 0;
    MYSQL_ROW row;
    ulong *lengths;

    if (my_hash_insert(&processed_compression_dictionaries,
      (uchar*)my_strdup(PSI_NOT_INSTRUMENTED, dictionary_name, MYF(0))))
      throw TC_Dump_exception("Couldn't allocate memory");

    my_snprintf(query_buff, sizeof(query_buff), get_zip_dict_data_stmt,
                dictionary_name);

    if (mysql_query_with_exception(mysql, &result, query_buff))
    {
      DBUG_VOID_RETURN;
    }

    /* make sure the MYSQL_RES object will be freed properly */
    MYSQL_RES_GUARD(result);

    row= mysql_fetch_row(result);
    if (row == 0)
    {
      char err_msg[1024];
      my_snprintf(err_msg, sizeof(err_msg),
                "Couldn't read data for compresion dictionary %s (%s)\n",
                dictionary_name, mysql_error(mysql));
      throw TC_Dump_exception(err_msg);
      DBUG_VOID_RETURN;
    }
    lengths= mysql_fetch_lengths(result);
    DBUG_ASSERT(lengths != 0);

    quoted_dictionary_name= quote_name(dictionary_name, quoted_buff, 0);

    /*
      We print DROP COMPRESSION_DICTIONARY only if no --tab
      (file per table option) and no --skip-add-drop-compression-dictionary
      were specified
    */
    bool opt_drop_compression_dictionary= true;
    if (opt_drop_compression_dictionary)
    {
      fprintf(sql_file,
        "/*!"  STRINGIFY_ARG(FIRST_SUPPORTED_COMPRESSED_COLUMNS_VERSION)
        " DROP COMPRESSION_DICTIONARY IF EXISTS %s */;\n",
        quoted_dictionary_name);
      check_io(sql_file);
    }

    /*
      Whether IF NOT EXISTS is added to CREATE COMPRESSION_DICTIONARY
      depends on the --add-drop-compression-dictionary /
      --skip-add-drop-compression-dictionary options.
    */
    fprintf(sql_file,
      "/*!"  STRINGIFY_ARG(FIRST_SUPPORTED_COMPRESSED_COLUMNS_VERSION)
      " CREATE COMPRESSION_DICTIONARY %s%s (", "",
      quoted_dictionary_name);
    check_io(sql_file);

    unescape(sql_file, row[0], lengths[0]);
    fputs(") */;\n", sql_file);
    check_io(sql_file);
  }
  DBUG_VOID_RETURN;
}

/* Print a value with a prefix on file */
void Dump_Handler::print_value(FILE *file, MYSQL_RES  *result, MYSQL_ROW row,
                              const char *prefix, const char *name,
                              int string_value)
{
  MYSQL_FIELD   *field;
  client_mysql_field_seek(result, 0);

  for ( ; (field= client_mysql_fetch_field(result)) ; row++)
  {
    if (!strcmp(field->name,name))
    {
      if (row[0] && row[0][0] && strcmp(row[0],"0")) /* Skip default */
      {
        fputc(' ',file);
        fputs(prefix, file);
        if (string_value)
          unescape(file,row[0], strlen(row[0]));
        else
          fputs(row[0], file);
        check_io(file);
        return;
      }
    }
  }
  return;                                       /* This shouldn't happen */
}

/*
  get_table_structure -- retrievs database structure, prints out corresponding
  CREATE statement and fills out insert_pat if the table is the type we will
  be dumping.

  ARGS
    table       - table name
    db          - db name
    table_type  - table type, e.g. "MyISAM" or "InnoDB", but also "VIEW"
    ignore_flag - what we must particularly ignore - see IGNORE_ defines above
    real_columns- Contains one byte per column, 0 means unused, 1 is used
                  Generated columns are marked as unused
  RETURN
    number of fields in table, 0 if error
*/
uint Dump_Handler::get_table_structure(char *table, char *db, char *table_type,
                                char *ignore_flag, my_bool real_columns[])
{
  my_bool    init=0, write_data, complete_insert;
  my_ulonglong num_fields;
  char       *result_table, *opt_quoted_table;
  const char *insert_option;
  char	     name_buff[NAME_LEN+3],table_buff[NAME_LEN*2+3];
  char       table_buff2[NAME_LEN*2+3], query_buff[QUERY_LENGTH];
  /* For SPIDER, this SQL is Open_frm_only */
  const char *show_fields_stmt= "SELECT `COLUMN_NAME` AS `Field`, "
                                "`COLUMN_TYPE` AS `Type`, "
                                "`IS_NULLABLE` AS `Null`, "
                                "`COLUMN_KEY` AS `Key`, "
                                "`COLUMN_DEFAULT` AS `Default`, "
                                "`EXTRA` AS `Extra`, "
                                "`COLUMN_COMMENT` AS `Comment` "
                                "FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE "
                                "TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";
  FILE       *sql_file= result_file;
  size_t     len;
  my_bool    is_log_table;
  my_bool    is_replication_metadata_table;
  unsigned int colno;
  my_bool    is_innodb_table;
  MYSQL_RES  *result = NULL;
  MYSQL_ROW  row;
  my_bool    has_pk= FALSE;
  bool opt_replace_into = false;
  DBUG_ENTER("Dump_Handler::get_table_structure");
  DBUG_PRINT("enter", ("db: %s  table: %s", db, table));

  /* make sure the mysql query result will be freed properly */
  MYSQL_RES_Mgr mysql_res_mgr(result);

  *ignore_flag= check_if_ignore_table(table, table_type);

  complete_insert= 0;
  my_bool opt_complete_insert = FALSE;
  if ((write_data= !(*ignore_flag & IGNORE_DATA)))
  {
    complete_insert= opt_complete_insert;
    if (!insert_pat_inited)
    {
      insert_pat_inited= 1;
      init_dynamic_string_checked(&insert_pat, "", 1024, 1024);
    }
    else
      dynstr_set_checked(&insert_pat, "");
  }

  bool opt_ignore = false;
  insert_option= (opt_ignore ? " IGNORE " : "");

  verbose_msg("-- Retrieving table structure for table %s...\n", table);

  len= my_snprintf(query_buff, sizeof(query_buff),
                   "SET SQL_QUOTE_SHOW_CREATE=%d",
                   (opt_quoted));
  if (!opt_create_options)
    my_stpcpy(query_buff+len,
           "/*!40102 ,SQL_MODE=concat(@@sql_mode, _utf8 ',NO_KEY_OPTIONS,NO_TABLE_OPTIONS,NO_FIELD_OPTIONS') */");

  result_table=     quote_name(table, table_buff, 1);
  opt_quoted_table= quote_name(table, table_buff2, 0);

  bool opt_innodb_optimize_keys = false;
  if (opt_innodb_optimize_keys && !strcmp(table_type, "InnoDB"))
    has_pk= has_primary_key(table);

  bool opt_order_by_primary = false, opt_order_by_primary_desc = false;
  if (opt_order_by_primary || opt_order_by_primary_desc)
    order_by= primary_key_fields(result_table,
                                 opt_order_by_primary_desc ? TRUE : FALSE);

  if (!mysql_query_with_exception(mysql, 0, query_buff))
  {
    /* using SHOW CREATE statement */
    if (!opt_no_create_info)
    {
      /* Make an sql-file, if path was given iow. option -T was given */
      char buff[20+FN_REFLEN];
      MYSQL_FIELD *field;

      my_snprintf(buff, sizeof(buff), "show create table %s", result_table);

      if (switch_character_set_results("binary") ||
          mysql_query_with_exception(mysql, &result, buff) ||
          switch_character_set_results(default_charset.c_str()))
        DBUG_RETURN(0);

      if (strcmp (table_type, "VIEW") == 0)         /* view */
        print_comment("\n--\n-- Temporary table structure for view %s\n--\n\n",
                      fix_identifier_with_newline(result_table));
      else
        print_comment("\n--\n-- Table structure for table %s\n--\n\n",
                      fix_identifier_with_newline(result_table));

      field= client_mysql_fetch_field_direct(result, 0); 
      if (strcmp(field->name, "View") == 0)
      {
        char *scv_buff= NULL;
        my_ulonglong n_cols;

        /*
          Even if the "table" is a view, we do a DROP TABLE here.  The
          view-specific code below fills in the DROP VIEW.
          We will skip the DROP TABLE for general_log and slow_log, since
          those stmts will fail, in case we apply dump by enabling logging.
          We will skip this for replication metadata tables as well.
        */
        print_optional_drop_table(sql_file, db, table, opt_quoted_table);

        verbose_msg("-- It's a view, create dummy view\n");

        /* save "show create" statement for later */
        if ((row= mysql_fetch_row(result)) && (scv_buff=row[1]))
          scv_buff= my_strdup(PSI_NOT_INSTRUMENTED,
                              scv_buff, MYF(0));
        
        /* make sure scv_buff will be freed properly */
        std::unique_ptr<char, void(*)(void*)> scv_buff_ptr(scv_buff, my_free);

        mysql_free_result_with_set_null(result);

        /*
          Create a table with the same name as the view and with columns of
          the same name in order to satisfy views that depend on this view.
          The table will be removed when the actual view is created.

          The properties of each column, are not preserved in this temporary
          table, because they are not necessary.

          This will not be necessary once we can determine dependencies
          between views and can simply dump them in the appropriate order.
        */
        my_snprintf(query_buff, sizeof(query_buff),
                    "SHOW FIELDS FROM %s", result_table);
        if (switch_character_set_results("binary") ||
            mysql_query_with_exception(mysql, &result, query_buff) ||
            switch_character_set_results(default_charset.c_str()))
        {
          /*
            View references invalid or privileged table/col/fun (err 1356),
            so we cannot create a stand-in table.  Be defensive and dump
            a comment with the view's 'show create' statement. (Bug #17371)
          */

          if (mysql_errno(mysql) == ER_VIEW_INVALID)
            fprintf(sql_file, "\n-- failed on view %s: %s\n\n", result_table, scv_buff ? scv_buff : "");

          DBUG_RETURN(0);
        }

        n_cols= mysql_num_rows(result);
        if (0 != n_cols)
        {

          /*
            The actual formula is based on the column names and how the .FRM
            files are stored and is too volatile to be repeated here.
            Thus we simply warn the user if the columns exceed a limit we
            know works most of the time.
          */
          if (n_cols >= 1000)
            fprintf(error_file,
                    "-- Warning: Creating a stand-in table for view %s may"
                    " fail when replaying the dump file produced because "
                    "of the number of columns exceeding 1000. Exercise "
                    "caution when replaying the produced dump file.\n", 
                    table);
          if (opt_drop_table)
          {
            /*
              We have already dropped any table of the same name above, so
              here we just drop the view.
            */

            fprintf(sql_file, "/*!50001 DROP VIEW IF EXISTS %s*/;\n",
                    opt_quoted_table);
            check_io(sql_file);
          }

          fprintf(sql_file,
                  "SET @saved_cs_client     = @@character_set_client;\n"
                  "SET character_set_client = utf8;\n"
                  "/*!50001 CREATE VIEW %s AS SELECT \n",
                  result_table);

          /*
            Get first row, following loop will prepend comma - keeps from
            having to know if the row being printed is last to determine if
            there should be a _trailing_ comma.
          */

          row= mysql_fetch_row(result);

          /*
            A temporary view is created to resolve the view interdependencies.
            This temporary view is dropped when the actual view is created.
          */

          fprintf(sql_file, " 1 AS %s",
                  quote_name(row[0], name_buff, 0));

          while((row= mysql_fetch_row(result)))
          {
            fprintf(sql_file, ",\n 1 AS %s",
                    quote_name(row[0], name_buff, 0));
          }

          fprintf(sql_file,"*/;\n"
                  "SET character_set_client = @saved_cs_client;\n");

          check_io(sql_file);
        }

        mysql_free_result_with_set_null(result);

        seen_views= 1;
        DBUG_RETURN(0);
      }

      row= mysql_fetch_row(result);

      is_innodb_table= (strcmp(table_type, "InnoDB") == 0);
      if (opt_innodb_optimize_keys && is_innodb_table)
        skip_secondary_keys(row[1], has_pk);
      if (is_innodb_table)
      {
        /*
          Search for compressed columns attributes and remove them if
          necessary.
        */

        LIST *referenced_dictionaries= 0, *current_dictionary;

        try
        {
          skip_compressed_columns(row[1], &referenced_dictionaries);
          referenced_dictionaries= list_reverse(referenced_dictionaries);
          for (current_dictionary= referenced_dictionaries;
              current_dictionary != 0;
              current_dictionary= list_rest(current_dictionary))
          {
            print_optional_create_compression_dictionary(
              sql_file, (const char*)current_dictionary->data);
          }
        }
        catch (...)
        {
          list_free(referenced_dictionaries, TRUE);
          throw;
        }
        
        list_free(referenced_dictionaries, TRUE);
      }

      print_optional_drop_table(sql_file, db, table, opt_quoted_table);

      is_log_table= general_log_or_slow_log_tables(db, table);
      is_replication_metadata_table= replication_metadata_tables(db, table);
      if (is_log_table || is_replication_metadata_table || opt_add_not_exists)
        row[1]+= 13; /* strlen("CREATE TABLE ")= 13 */
      
      fprintf(sql_file,
              "/*!40101 SET @saved_cs_client     = @@character_set_client */;\n"
              "/*!40101 SET character_set_client = utf8 */;\n"
              "%s%s;\n"
              "/*!40101 SET character_set_client = @saved_cs_client */;\n",
              (is_log_table || is_replication_metadata_table || opt_add_not_exists) ?
              "CREATE TABLE IF NOT EXISTS " : "", row[1]);

      check_io(sql_file);
      mysql_free_result_with_set_null(result);
    }
  
    my_snprintf(query_buff, sizeof(query_buff), "show fields from %s",
                result_table);
    if (mysql_query_with_exception(mysql, &result, query_buff))
    {
      DBUG_RETURN(0);
    }

    if (write_data && !complete_insert)
    {
      /*
        If data contents of table are to be written and complete_insert
        is false (column list not required in INSERT statement), scan the
        column list for generated columns, as presence of any generated column
        will require that an explicit list of columns is printed.
      */
      while ((row= mysql_fetch_row(result)))
      {
        complete_insert|=
          strcmp(row[SHOW_EXTRA], "STORED GENERATED") == 0 ||
          strcmp(row[SHOW_EXTRA], "VIRTUAL GENERATED") == 0;
      }
      mysql_free_result_with_set_null(result);

      if (mysql_query_with_exception(mysql, &result, query_buff))
      {
        DBUG_RETURN(0);
      }
    }
    /*
      If write_data is true, then we build up insert statements for
      the table's data. Note: in subsequent lines of code, this test
      will have to be performed each time we are appending to
      insert_pat.
    */
    if (write_data)
    {
      if (opt_replace_into)
        dynstr_append_checked(&insert_pat, "REPLACE ");
      else
        dynstr_append_checked(&insert_pat, "INSERT ");
      dynstr_append_checked(&insert_pat, insert_option);
      dynstr_append_checked(&insert_pat, "INTO ");
      dynstr_append_checked(&insert_pat, opt_quoted_table);
      if (complete_insert)
      {
        dynstr_append_checked(&insert_pat, " (");
      }
      else
      {
        dynstr_append_checked(&insert_pat, " VALUES ");
        if (!opt_extended_insert)
          dynstr_append_checked(&insert_pat, "(");
      }
    }

    colno= 0;
    while ((row= mysql_fetch_row(result)))
    {
      real_columns[colno]=
        strcmp(row[SHOW_EXTRA], "STORED GENERATED") != 0 &&
        strcmp(row[SHOW_EXTRA], "VIRTUAL GENERATED") != 0;

      if (real_columns[colno++] && complete_insert)
      {
        if (init)
        {
          dynstr_append_checked(&insert_pat, ", ");
        }
        init=1;
        dynstr_append_checked(&insert_pat,
                      quote_name(row[SHOW_FIELDNAME], name_buff, 0));
      }
    }
    num_fields= mysql_num_rows(result);
    mysql_free_result_with_set_null(result);
  }
  else
  {
    verbose_msg("%s: Warning: Can't set SQL_QUOTE_SHOW_CREATE option (%s)\n",
                tcdump_progname, mysql_error(mysql));

    my_snprintf(query_buff, sizeof(query_buff), show_fields_stmt, db, table);

    if (mysql_query_with_exception(mysql, &result, query_buff))
      DBUG_RETURN(0);

    if (write_data && !complete_insert)
    {
      /*
        If data contents of table are to be written and complete_insert
        is false (column list not required in INSERT statement), scan the
        column list for generated columns, as presence of any generated column
        will require that an explicit list of columns is printed.
      */
      while ((row= mysql_fetch_row(result)))
      {
        complete_insert|=
          strcmp(row[SHOW_EXTRA], "STORED GENERATED") == 0 ||
          strcmp(row[SHOW_EXTRA], "VIRTUAL GENERATED") == 0;
      }
      mysql_free_result_with_set_null(result);

      if (mysql_query_with_exception(mysql, &result, query_buff))
      {
        DBUG_RETURN(0);
      }
    }
    /* Make an sql-file, if path was given iow. option -T was given */
    if (!opt_no_create_info)
    {

      print_comment("\n--\n-- Table structure for table %s\n--\n\n",
                    fix_identifier_with_newline(result_table));
      if (opt_drop_table)
        fprintf(sql_file, "DROP TABLE IF EXISTS %s;\n", result_table);

      fprintf(sql_file, "CREATE TABLE %s (\n", result_table);
    
      check_io(sql_file);
    }

    if (write_data)
    {
      if (opt_replace_into)
        dynstr_append_checked(&insert_pat, "REPLACE ");
      else
        dynstr_append_checked(&insert_pat, "INSERT ");
      dynstr_append_checked(&insert_pat, insert_option);
      dynstr_append_checked(&insert_pat, "INTO ");
      dynstr_append_checked(&insert_pat, result_table);
      if (complete_insert)
        dynstr_append_checked(&insert_pat, " (");
      else
      {
        dynstr_append_checked(&insert_pat, " VALUES ");
        if (!opt_extended_insert)
          dynstr_append_checked(&insert_pat, "(");
      }
    }

    colno= 0;
    while ((row= mysql_fetch_row(result)))
    {
      ulong *lengths= mysql_fetch_lengths(result);

      real_columns[colno]=
        strcmp(row[SHOW_EXTRA], "STORED GENERATED") != 0 &&
        strcmp(row[SHOW_EXTRA], "VIRTUAL GENERATED") != 0;

      if (!real_columns[colno++])
        continue;

      if (init)
      {
        if (!opt_no_create_info)
        {
          fputs(",\n",sql_file);
          check_io(sql_file);
        }
        if (complete_insert)
          dynstr_append_checked(&insert_pat, ", ");
      }
      init=1;
      if (complete_insert)
        dynstr_append_checked(&insert_pat,
                      quote_name(row[SHOW_FIELDNAME], name_buff, 0));
      if (!opt_no_create_info)
      {

        fprintf(sql_file, "  %s %s", quote_name(row[SHOW_FIELDNAME],
                                                name_buff, 0),
                row[SHOW_TYPE]);
        if (row[SHOW_DEFAULT])
        {
          fputs(" DEFAULT ", sql_file);
          unescape(sql_file, row[SHOW_DEFAULT], lengths[SHOW_DEFAULT]);
        }
        if (!row[SHOW_NULL][0])
          fputs(" NOT NULL", sql_file);
        if (row[SHOW_EXTRA][0])
          fprintf(sql_file, " %s",row[SHOW_EXTRA]);
        check_io(sql_file);
      }
    }
    num_fields= mysql_num_rows(result);
    mysql_free_result_with_set_null(result);
    if (!opt_no_create_info)
    {
      /* Make an sql-file, if path was given iow. option -T was given */
      char buff[20+FN_REFLEN];
      uint keynr,primary_key;
      my_snprintf(buff, sizeof(buff), "show keys from %s", result_table);
      if (mysql_query_with_exception(mysql, &result, buff))
      {
        if (mysql_errno(mysql) == ER_WRONG_OBJECT)
        {
          /* it is VIEW */
          fputs("\t\t<options Comment=\"view\" />\n", sql_file);
          goto continue_xml;
        }
        fprintf(error_file, "%s: Can't get keys for table %s (%s)\n",
                tcdump_progname, result_table, mysql_error(mysql));
        DBUG_RETURN(0);
      }

      /* Find first which key is primary key */
      keynr=0;
      primary_key=INT_MAX;
      while ((row= mysql_fetch_row(result)))
      {
        if (atoi(row[3]) == 1)
        {
          keynr++;
          if (!strcmp(row[2],"PRIMARY"))
          {
            primary_key=keynr;
            break;
          }
        }
      }
      client_mysql_data_seek(result,0);
      keynr=0;
      while ((row= mysql_fetch_row(result)))
      {
        if (atoi(row[3]) == 1)
        {
          if (keynr++)
            putc(')', sql_file);
          if (atoi(row[1]))       /* Test if duplicate key */
            /* Duplicate allowed */
            fprintf(sql_file, ",\n  KEY %s (",quote_name(row[2],name_buff,0));
          else if (keynr == primary_key)
            fputs(",\n  PRIMARY KEY (",sql_file); /* First UNIQUE is primary */
          else
            fprintf(sql_file, ",\n  UNIQUE %s (",quote_name(row[2],name_buff,
                                                            0));
        }
        else
          putc(',', sql_file);
        fputs(quote_name(row[4], name_buff, 0), sql_file);
        if (row[7])
          fprintf(sql_file, " (%s)",row[7]);      /* Sub key */
        check_io(sql_file);
      }
      mysql_free_result_with_set_null(result);

      if (keynr)
        putc(')', sql_file);
      fputs("\n)",sql_file);
      check_io(sql_file);

      /* Get MySQL specific create options */
      if (opt_create_options)
      {
        char show_name_buff[NAME_LEN*2+2+24];

        /* Check memory for quote_for_like() */
        my_snprintf(buff, sizeof(buff), "show table status like %s",
                    quote_for_like(table, show_name_buff));

        if (mysql_query_with_exception(mysql, &result, buff))
        {
          if (mysql_errno(mysql) != ER_PARSE_ERROR)
          {                                     /* If old MySQL version */
            verbose_msg("-- Warning: Couldn't get status information for " \
                        "table %s (%s)\n", result_table,mysql_error(mysql));
          }
        }
        else if (!(row= mysql_fetch_row(result)))
        {
          fprintf(error_file,
                  "Error: Couldn't read status information for table %s (%s)\n",
                  result_table,mysql_error(mysql));
        }
        else
        {
          fputs("/*!",sql_file);
          print_value(sql_file,result,row,"engine=","Engine",0);
          print_value(sql_file,result,row,"","Create_options",0);
          print_value(sql_file,result,row,"comment=","Comment",1);
          fputs(" */",sql_file);
          check_io(sql_file);
        }
        mysql_free_result_with_set_null(result);              /* Is always safe to free */
      }
continue_xml:
      fputs(";\n", sql_file);
      check_io(sql_file);
    }
  }
  if (complete_insert)
  {
    dynstr_append_checked(&insert_pat, ") VALUES ");
    if (!opt_extended_insert)
      dynstr_append_checked(&insert_pat, "(");
  }
  DBUG_RETURN((uint) num_fields);
}

/*
  Dump delayed secondary index definitions when --innodb-optimize-keys is used.
*/
void Dump_Handler::dump_skipped_keys(const char *table)
{
  uint keys;

  if (!skipped_keys_list)
    return;

  verbose_msg("-- Dumping delayed secondary index definitions for table %s\n",
              table);

  skipped_keys_list= list_reverse(skipped_keys_list);
  fprintf(result_file, "ALTER TABLE %s ", table);
  for (keys= list_length(skipped_keys_list); keys > 0; keys--)
  {
    LIST *node= skipped_keys_list;
    char *def= (char *)(node->data);

    fprintf(result_file, "ADD %s%s", def, (keys > 1) ? ", " : ";\n");

    skipped_keys_list= list_delete(skipped_keys_list, node);
    my_free(def);
    my_free(node);
  }

  DBUG_ASSERT(skipped_keys_list == NULL);
}

/*
  Perform delayed secondary index creation for --innodb-optimize-keys.
*/
void Dump_Handler::restore_secondary_keys(char *table)
{
    if (skipped_keys_list)
    {
      uint keys;
      skipped_keys_list= list_reverse(skipped_keys_list);
      fprintf(result_file, "ALTER TABLE %s ", table);
      for (keys= list_length(skipped_keys_list); keys > 0; keys--)
      {
        LIST *node= skipped_keys_list;
        char *def= (char *)(node->data);

        fprintf(result_file, "ADD %s%s", def, (keys > 1) ? ", " : ";\n");

        skipped_keys_list= list_delete(skipped_keys_list, node);
        my_free(def);
        my_free(node);
      }

      DBUG_ASSERT(skipped_keys_list == NULL);
    }
}

/*
  Print hex value for blob data.

  SYNOPSIS
    print_blob_as_hex()
    output_file         - output file
    str                 - string to print
    len                 - its length

  DESCRIPTION
    Print hex value for blob data.
*/
void Dump_Handler::print_blob_as_hex(FILE *output_file, const char *str, ulong len)
{
    /* sakaik got the idea to to provide blob's in hex notation. */
    const char *ptr= str, *end= ptr + len;
    for (; ptr < end ; ptr++)
      fprintf(output_file, "%02X", *((uchar *)ptr));
    check_io(output_file);
}

/*

 SYNOPSIS
  dump_table()

  dump_table saves database contents as a series of INSERT statements.

  ARGS
   table - table name
   db    - db name

   RETURNS
    void
*/
void Dump_Handler::dump_table(char *table, char *db)
{
  char ignore_flag;
  char table_buff[NAME_LEN+3];
  DYNAMIC_STRING query_string;
  DYNAMIC_STRING extended_row;
  char table_type[NAME_LEN];
  char *result_table, table_buff2[NAME_LEN*2+3], *opt_quoted_table;
  char error_msg[1024];
  ulong         rownr, row_break;
  size_t        total_length, init_length;
  uint num_fields;
  MYSQL_RES     *res = NULL;
  MYSQL_FIELD   *field;
  MYSQL_ROW     row;
  my_bool real_columns[MAX_FIELDS];
  bool opt_hex_blob = false;
  DBUG_ENTER("Dump_Handler::dump_table");

  /* make sure the mysql query result will be freed properly */
  MYSQL_RES_Mgr res_mgr(res);

  /*
    Make sure you get the create table info before the following check for
    --no-data flag below. Otherwise, the create table info won't be printed.
  */
  num_fields= get_table_structure(table, db, table_type, &ignore_flag,
                                  real_columns);

  /*
    The "table" could be a view.  If so, we don't do anything here.
  */
  if (strcmp(table_type, "VIEW") == 0)
    DBUG_VOID_RETURN;

  /*
    We don't dump data fo`r replication metadata tables.
  */
  if (replication_metadata_tables(db, table))
    DBUG_VOID_RETURN;

  result_table= quote_name(table,table_buff, 1);
  opt_quoted_table= quote_name(table, table_buff2, 0);

  /* Check --no-data flag */
  if (opt_no_data)
  {
    dump_skipped_keys(opt_quoted_table);

    verbose_msg("-- Skipping dump data for table '%s', --no-data was used\n",
                table);
    restore_secondary_keys(opt_quoted_table);
    DBUG_VOID_RETURN;
  }

  DBUG_PRINT("info",
             ("ignore_flag: %x  num_fields: %d", (int) ignore_flag,
              num_fields));
  /*
    If the table type is a merge table or any type that has to be
     _completely_ ignored and no data dumped
  */
  if (ignore_flag & IGNORE_DATA)
  {
    verbose_msg("-- Warning: Skipping data for table '%s' because " \
                "it's of type %s\n", table, table_type);
    DBUG_VOID_RETURN;
  }
  /* Check that there are any fields in the table */
  if (num_fields == 0)
  {
    verbose_msg("-- Skipping dump data for table '%s', it has no fields\n",
                table);
    DBUG_VOID_RETURN;
  }

  verbose_msg("-- Sending SELECT query...\n");

  init_dynamic_string_checked(&query_string, "", 1024, 1024);
  DYNAMIC_STR_GUARD query_string_guard(&query_string);
  extended_row.str= NULL;
  if (opt_extended_insert)
    init_dynamic_string_checked(&extended_row, "", 1024, 1024);
  DYNAMIC_STR_GUARD extended_row_guard(&extended_row);

  print_comment("\n--\n-- Dumping data for table %s\n--\n",
                fix_identifier_with_newline(result_table));
  
  dynstr_append_checked(&query_string, "SELECT /*!40001 SQL_NO_CACHE */ * FROM ");
  dynstr_append_checked(&query_string, result_table);

  /*
    If table is mysql.proc then do not dump routines which belong
    to sys schema
  */
  if ((!my_strcasecmp(charset_info, db, "mysql")) &&
        (!my_strcasecmp(charset_info, table, "proc")) &&
        opt_alldbs)
  {
    dynstr_append_checked(&query_string, " WHERE db != 'sys'");
  }
  if (order_by)
  {
    print_comment("-- ORDER BY:  %s\n",
      fix_identifier_with_newline(order_by));

    dynstr_append_checked(&query_string, " ORDER BY ");
    dynstr_append_checked(&query_string, order_by);
  }

  fputs("\n", result_file);
  check_io(result_file);

  if (mysql_query_with_exception(mysql, 0, query_string.str))
  {
    throw DB_exception(mysql, "when retrieving data from server");
    goto err;
  }
  if (opt_quick)
    res=client_mysql_use_result(mysql);
  else
    res=mysql_store_result(mysql);
  if (!res)
  {
    throw DB_exception(mysql, "when retrieving data from server");
    goto err;
  }

  verbose_msg("-- Retrieving rows...\n");
  if (mysql_num_fields(res) != num_fields)
  {
    my_snprintf(error_msg, sizeof(error_msg), 
                "%s: Error in field count for table: %s !  Aborting.\n",
                tcdump_progname, result_table);
    goto err;
  }

  if (opt_lock)
  {
    fprintf(result_file,"LOCK TABLES %s WRITE;\n", opt_quoted_table);
    check_io(result_file);
  }
  /* Moved disable keys to after lock per bug 15977 */
  if (opt_disable_keys)
  {
    fprintf(result_file, "/*!40000 ALTER TABLE %s DISABLE KEYS */;\n",
      opt_quoted_table);
    check_io(result_file);
  }

  total_length= opt_net_buffer_length;                /* Force row break */
  row_break=0;
  rownr=0;
  init_length=(uint) insert_pat.length+4;
  if (opt_autocommit)
  {
    fprintf(result_file, "set autocommit=0;\n");
    check_io(result_file);
  }

  while ((row= mysql_fetch_row(res)))
  {
    uint i;
    ulong *lengths= mysql_fetch_lengths(res);
    rownr++;
    if (!opt_extended_insert)
    {
      fputs(insert_pat.str,result_file);
      check_io(result_file);
    }
    client_mysql_field_seek(res,0);

    for (i= 0; i < mysql_num_fields(res); i++)
    {
      int is_blob;
      ulong length= lengths[i];

      if (!(field= client_mysql_fetch_field(res))) {
        my_snprintf(error_msg, sizeof(error_msg),
            "Not enough fields from table %s! Aborting.\n", result_table);
        throw TC_Dump_exception(error_msg);
      }

      if (!real_columns[i])
        continue;
      /*
          63 is my_charset_bin. If charsetnr is not 63,
          we have not a BLOB but a TEXT column.
          we'll dump in hex only BLOB columns.
      */
      is_blob= (opt_hex_blob && field->charsetnr == 63 &&
                (field->type == MYSQL_TYPE_BIT ||
                  field->type == MYSQL_TYPE_STRING ||
                  field->type == MYSQL_TYPE_VAR_STRING ||
                  field->type == MYSQL_TYPE_VARCHAR ||
                  field->type == MYSQL_TYPE_BLOB ||
                  field->type == MYSQL_TYPE_LONG_BLOB ||
                  field->type == MYSQL_TYPE_MEDIUM_BLOB ||
                  field->type == MYSQL_TYPE_TINY_BLOB ||
                  field->type == MYSQL_TYPE_GEOMETRY)) ? 1 : 0;
      if (opt_extended_insert)
      {
        if (i == 0)
          dynstr_set_checked(&extended_row,"(");
        else
          dynstr_append_checked(&extended_row,",");

        if (row[i])
        {
          if (length)
          {
            if (!(field->flags & NUM_FLAG))
            {
              /*
                "length * 2 + 2" is OK for both HEX and non-HEX modes:
                - In HEX mode we need exactly 2 bytes per character
                plus 2 bytes for '0x' prefix.
                - In non-HEX mode we need up to 2 bytes per character,
                plus 2 bytes for leading and trailing '\'' characters.
                Also we need to reserve 1 byte for terminating '\0'.
              */
              dynstr_realloc_checked(&extended_row,length * 2 + 2 + 1);
              if (opt_hex_blob && is_blob)
              {
                dynstr_append_checked(&extended_row, "0x");
                extended_row.length+= mysql_hex_string(extended_row.str +
                                                        extended_row.length,
                                                        row[i], length);
                DBUG_ASSERT(extended_row.length+1 <= extended_row.max_length);
                /* mysql_hex_string() already terminated string by '\0' */
                DBUG_ASSERT(extended_row.str[extended_row.length] == '\0');
              }
              else
              {
                dynstr_append_checked(&extended_row,"'");
                extended_row.length +=
                client_mysql_real_escape_string_quote(&mysql_connection,
                                        &extended_row.str[extended_row.length],
                                        row[i],length,
                                        '\'');
                extended_row.str[extended_row.length]='\0';
                dynstr_append_checked(&extended_row,"'");
              }
            }
            else
            {
              /* change any strings ("inf", "-inf", "nan") into NULL */
              char *ptr= row[i];
              if (my_isalpha(charset_info, *ptr) || (*ptr == '-' &&
                  my_isalpha(charset_info, ptr[1])))
                dynstr_append_checked(&extended_row, "NULL");
              else
              {
                if (field->type == MYSQL_TYPE_DECIMAL)
                {
                  /* add " signs around */
                  dynstr_append_checked(&extended_row, "'");
                  dynstr_append_checked(&extended_row, ptr);
                  dynstr_append_checked(&extended_row, "'");
                }
                else
                  dynstr_append_checked(&extended_row, ptr);
              }
            }
          }
          else
            dynstr_append_checked(&extended_row,"''");
        }
        else
          dynstr_append_checked(&extended_row,"NULL");
      }
      else
      {
        if (i)
        {
          fputc(',', result_file);
          check_io(result_file);
        }
        if (row[i])
        {
          if (!(field->flags & NUM_FLAG))
          {
            if (opt_hex_blob && is_blob && length)
            {
              fputs("0x", result_file);
              print_blob_as_hex(result_file, row[i], length);
            }
            else
              unescape(result_file, row[i], length);
          }
          else
          {
            /* change any strings ("inf", "-inf", "nan") into NULL */
            char *ptr= row[i];
            if (my_isalpha(charset_info, *ptr) ||
                      (*ptr == '-' && my_isalpha(charset_info, ptr[1])))
              fputs("NULL", result_file);
            else if (field->type == MYSQL_TYPE_DECIMAL)
            {
              /* add " signs around */
              fputc('\'', result_file);
              fputs(ptr, result_file);
              fputc('\'', result_file);
            }
            else
              fputs(ptr, result_file);
          }
        }
        else
        {
          /* The field value is NULL */
          fputs("NULL", result_file);
        }
        check_io(result_file);
      }
    }

    if (opt_extended_insert)
    {
      size_t row_length;
      dynstr_append_checked(&extended_row,")");
      row_length= 2 + extended_row.length;
      if (total_length + row_length < opt_net_buffer_length)
      {
        total_length+= row_length;
        fputc(',',result_file);            /* Always row break */
        fputs(extended_row.str,result_file);
      }
      else
      {
        if (row_break)
          fputs(";\n", result_file);
        row_break=1;                          /* This is first row */

        fputs(insert_pat.str,result_file);
        fputs(extended_row.str,result_file);
        total_length= row_length+init_length;
      }
      check_io(result_file);
    }
    else
    {
      fputs(");\n", result_file);
      check_io(result_file);
    }
  }

  /* XML - close table tag and supress regular output */
  if (opt_extended_insert && row_break)
    fputs(";\n", result_file);             /* If not empty table */
  fflush(result_file);
  check_io(result_file);
  if (mysql_errno(mysql))
  {
    my_snprintf(error_msg, sizeof(error_msg),
                "%s: Error %d: %s when dumping table %s at row: %ld\n",
                tcdump_progname,
                mysql_errno(mysql),
                mysql_error(mysql),
                result_table,
                rownr);
    goto err;
  }

  dump_skipped_keys(opt_quoted_table);

  /* Moved enable keys to before unlock per bug 15977 */
  if (opt_disable_keys)
  {
    fprintf(result_file,"/*!40000 ALTER TABLE %s ENABLE KEYS */;\n",
            opt_quoted_table);
    check_io(result_file);
  }
  if (opt_lock)
  {
    fputs("UNLOCK TABLES;\n", result_file);
    check_io(result_file);
  }
  if (opt_autocommit)
  {
    fprintf(result_file, "commit;\n");
    check_io(result_file);
  }
  mysql_free_result_with_set_null(res);
  
  dynstr_free(&query_string);
  if (opt_extended_insert)
    dynstr_free(&extended_row);
  DBUG_VOID_RETURN;

err:
  dynstr_free(&query_string);
  if (opt_extended_insert)
    dynstr_free(&extended_row);
  throw TC_Dump_exception(error_msg);
  DBUG_VOID_RETURN;
}

int Dump_Handler::fetch_db_collation(const char *db_name,
                                     char *db_cl_name,
                                     int db_cl_size)
{
  my_bool err_status= FALSE;
  char query[QUERY_LENGTH];
  MYSQL_RES *db_cl_res;
  MYSQL_ROW db_cl_row;
  char quoted_database_buf[NAME_LEN*2+3];
  char *qdatabase= quote_name(db_name, quoted_database_buf, 1);

  my_snprintf(query, sizeof (query), "use %s", qdatabase);

  if (mysql_query_with_exception(mysql, NULL, query))
    return 1;

  if (mysql_query_with_exception(mysql, &db_cl_res,
                                 "select @@collation_database"))
    return 1;

  do
  {
    if (mysql_num_rows(db_cl_res) != 1)
    {
      err_status= TRUE;
      break;
    }

    if (!(db_cl_row= mysql_fetch_row(db_cl_res)))
    {
      err_status= TRUE;
      break;
    }

    strncpy(db_cl_name, db_cl_row[0], db_cl_size-1);
    db_cl_name[db_cl_size - 1]= 0;

  } while (FALSE);

  mysql_free_result(db_cl_res);

  return err_status ? 1 : 0;
}

void Dump_Handler::dump_trigger_old(FILE *sql_file, MYSQL_RES *show_triggers_rs,
                                    MYSQL_ROW *show_trigger_row,
                                    const char *table_name)
{
  char quoted_table_name_buf[NAME_LEN * 2 + 3];
  char *quoted_table_name= quote_name(table_name, quoted_table_name_buf, 1);

  char name_buff[NAME_LEN * 4 + 3];

  DBUG_ENTER("Dump_Handler::dump_trigger_old");

  fprintf(sql_file,
          "--\n"
          "-- WARNING: old server version. "
            "The following dump may be incomplete.\n"
          "--\n");

  if (opt_drop_trigger)
    fprintf(sql_file, "/*!50032 DROP TRIGGER IF EXISTS %s */;\n", (*show_trigger_row)[0]);

  fprintf(sql_file,
          "DELIMITER ;;\n"
          "/*!50003 SET SESSION SQL_MODE=\"%s\" */;;\n"
          "/*!50003 CREATE */ ",
          (*show_trigger_row)[6]);

  if (mysql_num_fields(show_triggers_rs) > 7)
  {
    /*
      mysqldump can be run against the server, that does not support
      definer in triggers (there is no DEFINER column in SHOW TRIGGERS
      output). So, we should check if we have this column before
      accessing it.
    */

    size_t user_name_len;
    char user_name_str[USERNAME_LENGTH + 1];
    char quoted_user_name_str[USERNAME_LENGTH * 2 + 3];
    size_t host_name_len;
    char host_name_str[HOSTNAME_LENGTH + 1];
    char quoted_host_name_str[HOSTNAME_LENGTH * 2 + 3];

    parse_user((*show_trigger_row)[7],
               strlen((*show_trigger_row)[7]),
               user_name_str, &user_name_len,
               host_name_str, &host_name_len);

    fprintf(sql_file,
            "/*!50017 DEFINER=%s@%s */ ",
            quote_name(user_name_str, quoted_user_name_str, FALSE),
            quote_name(host_name_str, quoted_host_name_str, FALSE));
  }

  fprintf(sql_file,
          "/*!50003 TRIGGER %s %s %s ON %s FOR EACH ROW%s%s */;;\n"
          "DELIMITER ;\n",
          quote_name((*show_trigger_row)[0], name_buff, 0), /* Trigger */
          (*show_trigger_row)[4], /* Timing */
          (*show_trigger_row)[1], /* Event */
          quoted_table_name,
          (strchr(" \t\n\r", *((*show_trigger_row)[3]))) ? "" : " ",
          (*show_trigger_row)[3] /* Statement */);
  
  DBUG_VOID_RETURN;
}

int Dump_Handler::switch_db_collation(FILE *sql_file,
                                      const char *db_name,
                                      const char *delimiter,
                                      const char *current_db_cl_name,
                                      const char *required_db_cl_name,
                                      int *db_cl_altered)
{
  if (strcmp(current_db_cl_name, required_db_cl_name) != 0)
  {
    char quoted_db_buf[NAME_LEN * 2 + 3];
    char *quoted_db_name= quote_name(db_name, quoted_db_buf, FALSE);

    CHARSET_INFO *db_cl= get_charset_by_name(required_db_cl_name, MYF(0));

    if (!db_cl)
      return 1;

    fprintf(sql_file,
            "ALTER DATABASE %s CHARACTER SET %s COLLATE %s %s\n",
            (const char *) quoted_db_name,
            (const char *) db_cl->csname,
            (const char *) db_cl->name,
            (const char *) delimiter);

    *db_cl_altered= 1;

    return 0;
  }

  *db_cl_altered= 0;

  return 0;
}

int Dump_Handler::restore_db_collation(FILE *sql_file,
                                      const char *db_name,
                                      const char *delimiter,
                                      const char *db_cl_name)
{
  char quoted_db_buf[NAME_LEN * 2 + 3];
  char *quoted_db_name= quote_name(db_name, quoted_db_buf, FALSE);

  CHARSET_INFO *db_cl= get_charset_by_name(db_cl_name, MYF(0));

  if (!db_cl)
    return 1;

  fprintf(sql_file,
          "ALTER DATABASE %s CHARACTER SET %s COLLATE %s %s\n",
          (const char *) quoted_db_name,
          (const char *) db_cl->csname,
          (const char *) db_cl->name,
          (const char *) delimiter);

  return 0;
}

void Dump_Handler::switch_cs_variables(FILE *sql_file,
                                      const char *delimiter,
                                      const char *character_set_client,
                                      const char *character_set_results,
                                      const char *collation_connection)
{
  fprintf(sql_file,
          "/*!50003 SET @saved_cs_client      = @@character_set_client */ %s\n"
          "/*!50003 SET @saved_cs_results     = @@character_set_results */ %s\n"
          "/*!50003 SET @saved_col_connection = @@collation_connection */ %s\n"
          "/*!50003 SET character_set_client  = %s */ %s\n"
          "/*!50003 SET character_set_results = %s */ %s\n"
          "/*!50003 SET collation_connection  = %s */ %s\n",
          (const char *) delimiter,
          (const char *) delimiter,
          (const char *) delimiter,

          (const char *) character_set_client,
          (const char *) delimiter,

          (const char *) character_set_results,
          (const char *) delimiter,

          (const char *) collation_connection,
          (const char *) delimiter);
}

void Dump_Handler::restore_cs_variables(FILE *sql_file, const char *delimiter)
{
  fprintf(sql_file,
          "/*!50003 SET character_set_client  = @saved_cs_client */ %s\n"
          "/*!50003 SET character_set_results = @saved_cs_results */ %s\n"
          "/*!50003 SET collation_connection  = @saved_col_connection */ %s\n",
          (const char *) delimiter,
          (const char *) delimiter,
          (const char *) delimiter);
}

void Dump_Handler::switch_sql_mode(FILE *sql_file,
                                  const char *delimiter,
                                  const char *sql_mode)
{
  fprintf(sql_file,
          "/*!50003 SET @saved_sql_mode       = @@sql_mode */ %s\n"
          "/*!50003 SET sql_mode              = '%s' */ %s\n",
          (const char *) delimiter,

          (const char *) sql_mode,
          (const char *) delimiter);
}

void Dump_Handler::restore_sql_mode(FILE *sql_file, const char *delimiter)
{
  fprintf(sql_file,
          "/*!50003 SET sql_mode              = @saved_sql_mode */ %s\n",
          (const char *) delimiter);
}

void Dump_Handler::switch_time_zone(FILE *sql_file,
                                    const char *delimiter,
                                    const char *time_zone)
{
  fprintf(sql_file,
          "/*!50003 SET @saved_time_zone      = @@time_zone */ %s\n"
          "/*!50003 SET time_zone             = '%s' */ %s\n",
          (const char *) delimiter,

          (const char *) time_zone,
          (const char *) delimiter);
}

void Dump_Handler::restore_time_zone(FILE *sql_file, const char *delimiter)
{
  fprintf(sql_file,
          "/*!50003 SET time_zone             = @saved_time_zone */ %s\n",
          (const char *) delimiter);
}

int Dump_Handler::dump_trigger(FILE *sql_file, MYSQL_RES *show_create_trigger_rs,
                              const char *db_name,
                              const char *db_cl_name)
{
  MYSQL_ROW row;
  char *query_str;
  int db_cl_altered= FALSE;

  DBUG_ENTER("Dump_Handler::dump_trigger");

  while ((row= mysql_fetch_row(show_create_trigger_rs)))
  {
    query_str= cover_definer_clause(row[2], strlen(row[2]),
                                    C_STRING_WITH_LEN("50017"),
                                    C_STRING_WITH_LEN("50003"),
                                    C_STRING_WITH_LEN(" TRIGGER"));
    
    /* make sure query_str will be freed properly */
    std::unique_ptr<char, decltype(my_free)*> query_str_ptr(query_str, my_free);

    if (switch_db_collation(sql_file, db_name, ";",
                            db_cl_name, row[5], &db_cl_altered))
      DBUG_RETURN(TRUE);

    switch_cs_variables(sql_file, ";",
                        row[3],   /* character_set_client */
                        row[3],   /* character_set_results */
                        row[4]);  /* collation_connection */

    switch_sql_mode(sql_file, ";", row[1]);

    if (opt_drop_trigger)
      fprintf(sql_file, "/*!50032 DROP TRIGGER IF EXISTS %s */;\n", row[0]);

    fprintf(sql_file,
            "DELIMITER ;;\n"
            "/*!50003 %s */;;\n"
            "DELIMITER ;\n",
            (const char *) (query_str != NULL ? query_str : row[2]));

    restore_sql_mode(sql_file, ";");
    restore_cs_variables(sql_file, ";");

    if (db_cl_altered)
    {
      if (restore_db_collation(sql_file, db_name, ";", db_cl_name))
        DBUG_RETURN(TRUE);
    }
  }

  DBUG_RETURN(FALSE);
}

/**
  Dump the triggers for a given table.

  This should be called after the tables have been dumped in case a trigger
  depends on the existence of a table.

  @param[in] table_name
  @param[in] db_name

  @return Error status.
    @retval TRUE error has occurred.
    @retval FALSE operation succeed.
*/
int Dump_Handler::dump_triggers_for_table(char *table_name, char *db_name)
{
  char       name_buff[NAME_LEN*4+3];
  char       query_buff[QUERY_LENGTH];
  uint       opt_compatible_mode = 0;
  uint       old_opt_compatible_mode= opt_compatible_mode;
  MYSQL_RES  *show_triggers_rs = NULL;
  MYSQL_ROW  row;
  FILE      *sql_file= result_file;

  char       db_cl_name[MY_CS_NAME_SIZE];
  int        ret= TRUE;

  DBUG_ENTER("Dump_Handler::dump_triggers_for_table");
  DBUG_PRINT("enter", ("db: %s, table_name: %s", db_name, table_name));

  /* make sure the mysql query result will be freed properly */
  MYSQL_RES_Mgr res_mgr(show_triggers_rs);

  /* Do not use ANSI_QUOTES on triggers in dump */
  opt_compatible_mode&= ~MASK_ANSI_QUOTES;

  /* Get database collation. */

  if (switch_character_set_results("binary"))
    goto done;

  if (fetch_db_collation(db_name, db_cl_name, sizeof (db_cl_name)))
    goto done;

  /* Get list of triggers. */

  my_snprintf(query_buff, sizeof(query_buff),
              "SHOW TRIGGERS LIKE %s",
              quote_for_like(table_name, name_buff));

  if (mysql_query_with_exception(mysql, &show_triggers_rs, query_buff))
    goto done;

  /* Dump triggers. */

  if (! mysql_num_rows(show_triggers_rs))
    goto skip;

  while ((row= mysql_fetch_row(show_triggers_rs)))
  {

    my_snprintf(query_buff, sizeof (query_buff),
                "SHOW CREATE TRIGGER %s",
                quote_name(row[0], name_buff, TRUE));

    if (mysql_real_query(mysql, query_buff, strlen(query_buff)))
    {
      /*
        mysqldump is being run against old server, that does not support
        SHOW CREATE TRIGGER statement. We should use SHOW TRIGGERS output.

        NOTE: the dump may be incorrect, as old SHOW TRIGGERS does not
        provide all the necessary information to restore trigger properly.
      */

      dump_trigger_old(sql_file, show_triggers_rs, &row, table_name);
    }
    else
    {
      MYSQL_RES *show_create_trigger_rs= mysql_store_result(mysql);

      /* make sure mysql query result will be freed properly */
      MYSQL_RES_GUARD(show_create_trigger_rs);

      if (!show_create_trigger_rs ||
          dump_trigger(sql_file, show_create_trigger_rs, db_name, db_cl_name))
        goto done;
    }

  }

skip:
  mysql_free_result_with_set_null(show_triggers_rs);

  if (switch_character_set_results(default_charset.c_str()))
    goto done;

  /*
    make sure to set back opt_compatible mode to
    original value
  */
  opt_compatible_mode=old_opt_compatible_mode;

  ret= FALSE;

done:

  DBUG_RETURN(ret);
}

/*
  dump_events_for_db
  -- retrieves list of events for a given db, and prints out
  the CREATE EVENT statement into the output (the dump).

  RETURN
    0  Success
    1  Error
*/
uint Dump_Handler::dump_events_for_db(char *db)
{
  char       query_buff[QUERY_LENGTH];
  char       db_name_buff[NAME_LEN*2+3], name_buff[NAME_LEN*2+3];
  char       *event_name;
  char       delimiter[QUERY_LENGTH];
  FILE       *sql_file= result_file;
  MYSQL_RES  *event_res, *event_list_res;
  MYSQL_ROW  row, event_list_row;

  char       db_cl_name[MY_CS_NAME_SIZE];
  int        db_cl_altered= FALSE;

  DBUG_ENTER("Dump_Handler::dump_events_for_db");
  DBUG_PRINT("enter", ("db: '%s'", db));

  client_mysql_real_escape_string_quote(mysql, db_name_buff,
                                 db, (ulong)strlen(db), '\'');
  /* nice comments */
  print_comment("\n--\n-- Dumping events for database '%s'\n--\n",
                fix_identifier_with_newline(db));

  /*
    not using "mysql_query_with_error_report" because we may have not
    enough privileges to lock mysql.events.
  */
  const char *lock_event_query= "LOCK TABLES mysql.event READ";
  if (opt_lock_tables)
    mysql_real_query(mysql, lock_event_query, strlen(lock_event_query));

  if (mysql_query_with_exception(mysql, &event_list_res, "show events"))
    DBUG_RETURN(0);

  /* make sure the mysql query result will be freed properly */
  MYSQL_RES_GUARD(event_list_res);

  strcpy(delimiter, ";");
  if (mysql_num_rows(event_list_res) > 0)
  {
    fprintf(sql_file, "/*!50106 SET @save_time_zone= @@TIME_ZONE */ ;\n");

    /* Get database collation. */
    if (fetch_db_collation(db_name_buff, db_cl_name, sizeof (db_cl_name)))
      DBUG_RETURN(1);

    if (switch_character_set_results("binary"))
      DBUG_RETURN(1);

    while ((event_list_row= mysql_fetch_row(event_list_res)) != NULL)
    {
      event_name= quote_name(event_list_row[1], name_buff, 0);
      DBUG_PRINT("info", ("retrieving CREATE EVENT for %s", name_buff));
      my_snprintf(query_buff, sizeof(query_buff), "SHOW CREATE EVENT %s", 
          event_name);

      if (mysql_query_with_exception(mysql, &event_res, query_buff))
        DBUG_RETURN(1);

      /* make sure the mysql query result will be freed properly */
      MYSQL_RES_GUARD(event_res);

      while ((row= mysql_fetch_row(event_res)) != NULL)
      {
        /*
          if the user has EXECUTE privilege he can see event names, but not the
          event body!
        */
        if (strlen(row[3]) != 0)
        {
          char *query_str;

          if (opt_drop_table)
            fprintf(sql_file, "/*!50106 DROP EVENT IF EXISTS %s */%s\n", 
                event_name, delimiter);

          if (create_delimiter(row[3], delimiter, sizeof(delimiter)) == NULL)
          {
            fprintf(error_file, "%s: Warning: Can't create delimiter for event '%s'\n",
                    my_progname, event_name);
            DBUG_RETURN(1);
          }

          fprintf(sql_file, "DELIMITER %s\n", delimiter);

          if (mysql_num_fields(event_res) >= 7)
          {
            if (switch_db_collation(sql_file, db_name_buff, delimiter,
                                    db_cl_name, row[6], &db_cl_altered))
            {
              DBUG_RETURN(1);
            }

            switch_cs_variables(sql_file, delimiter,
                                row[4],   /* character_set_client */
                                row[4],   /* character_set_results */
                                row[5]);  /* collation_connection */
          }
          else
          {
            /*
              mysqldump is being run against the server, that does not
              provide character set information in SHOW CREATE
              statements.

              NOTE: the dump may be incorrect, since character set
              information is required in order to restore event properly.
            */

            fprintf(sql_file,
                    "--\n"
                    "-- WARNING: old server version. "
                      "The following dump may be incomplete.\n"
                    "--\n");
          }

          switch_sql_mode(sql_file, delimiter, row[1]);

          switch_time_zone(sql_file, delimiter, row[2]);

          query_str= cover_definer_clause(row[3], strlen(row[3]),
                                          C_STRING_WITH_LEN("50117"),
                                          C_STRING_WITH_LEN("50106"),
                                          C_STRING_WITH_LEN(" EVENT"));

          fprintf(sql_file,
                  "/*!50106 %s */ %s\n",
                  (const char *) (query_str != NULL ? query_str : row[3]),
                  (const char *) delimiter);

          my_free(query_str);
          restore_time_zone(sql_file, delimiter);
          restore_sql_mode(sql_file, delimiter);

          if (mysql_num_fields(event_res) >= 7)
          {
            restore_cs_variables(sql_file, delimiter);

            if (db_cl_altered)
            {
              if (restore_db_collation(sql_file, db_name_buff, delimiter,
                                       db_cl_name))
                DBUG_RETURN(1);
            }
          }
        }
      } /* end of event printing */

    } /* end of list of events */

    fprintf(sql_file, "DELIMITER ;\n");
    fprintf(sql_file, "/*!50106 SET TIME_ZONE= @save_time_zone */ ;\n");

    if (switch_character_set_results(default_charset.c_str()))
      DBUG_RETURN(1);
  }

  if (opt_lock_tables)
    (void) mysql_query_with_exception(mysql, 0, "UNLOCK TABLES");
  DBUG_RETURN(0);
}

/*
  dump_routines_for_db
  -- retrieves list of routines for a given db, and prints out
  the CREATE PROCEDURE definition into the output (the dump).

  This function has logic to print the appropriate syntax depending on whether
  this is a procedure or functions

  RETURN
    0  Success
    1  Error
*/

uint Dump_Handler::dump_routines_for_db(char *db)
{
  char       query_buff[QUERY_LENGTH];
  const char *routine_type[]= {"FUNCTION", "PROCEDURE"};
  char       db_name_buff[NAME_LEN*2+3], name_buff[NAME_LEN*2+3];
  char       *routine_name;
  int        i;
  FILE       *sql_file= result_file;
  MYSQL_RES  *routine_res, *routine_list_res;
  MYSQL_ROW  row, routine_list_row;

  char       db_cl_name[MY_CS_NAME_SIZE];
  int        db_cl_altered= FALSE;

  char       error_buff[512];

  DBUG_ENTER("Dump_Handler::dump_routines_for_db");
  DBUG_PRINT("enter", ("db: '%s'", db));

  client_mysql_real_escape_string_quote(mysql, db_name_buff,
                                 db, (ulong)strlen(db), '\'');
  /* nice comments */
  print_comment("\n--\n-- Dumping routines for database '%s'\n--\n",
                fix_identifier_with_newline(db));

  /*
    not using "mysql_query_with_error_report" because we may have not
    enough privileges to lock mysql.proc.
  */
  const char *lock_proc_query= "LOCK TABLES mysql.proc READ";
  if (opt_lock_tables)
    mysql_real_query(mysql, lock_proc_query, strlen(lock_proc_query));

  /* Get database collation. */

  if (fetch_db_collation(db_name_buff, db_cl_name, sizeof (db_cl_name)))
    DBUG_RETURN(1);

  if (switch_character_set_results("binary"))
    DBUG_RETURN(1);

  /* 0, retrieve and dump functions, 1, procedures */
  for (i= 0; i <= 1; i++)
  {
    my_snprintf(query_buff, sizeof(query_buff),
                "SHOW %s STATUS WHERE Db = '%s'",
                routine_type[i], db_name_buff);

    if (mysql_query_with_exception(mysql, &routine_list_res, query_buff))
      DBUG_RETURN(1);

    /* make sure the mysql query result will be freed properly */
    MYSQL_RES_GUARD(routine_list_res);

    if (mysql_num_rows(routine_list_res))
    {

      while ((routine_list_row= mysql_fetch_row(routine_list_res)))
      {
        routine_name= quote_name(routine_list_row[1], name_buff, 0);
        DBUG_PRINT("info", ("retrieving CREATE %s for %s", routine_type[i],
                            name_buff));
        my_snprintf(query_buff, sizeof(query_buff), "SHOW CREATE %s %s",
                    routine_type[i], routine_name);

        if (mysql_query_with_exception(mysql, &routine_res, query_buff))
          DBUG_RETURN(1);

        /* make sure the mysql query result will be freed properly */
        MYSQL_RES_GUARD(routine_res);

        while ((row= mysql_fetch_row(routine_res)))
        {
          /*
            if the user has EXECUTE privilege he see routine names, but NOT the
            routine body of other routines that are not the creator of!
          */
          DBUG_PRINT("info",("length of body for %s row[2] '%s' is %zu",
                             routine_name, row[2] ? row[2] : "(null)",
                             row[2] ? strlen(row[2]) : 0));
          if (row[2] == NULL)
          {
            print_comment("\n-- insufficient privileges to %s\n",
                          query_buff);
            
            char current_user[256];
            if((target_server == NULL) || (target_server->username == NULL))
              my_snprintf(current_user, sizeof(current_user), "%s", "<NULL user>");
            else
              my_snprintf(current_user, sizeof(current_user), "%s", 
                          target_server->username);
            print_comment("-- does %s have permissions on mysql.proc?\n\n",
                          fix_identifier_with_newline(current_user));
            my_snprintf(error_buff, sizeof(error_buff), 
              "%s has insufficent privileges to %s!", current_user, query_buff);
            throw TC_Dump_exception(error_buff);
          }
          else if (strlen(row[2]))
          {
            if (opt_drop_table)
              fprintf(sql_file, "/*!50003 DROP %s IF EXISTS %s */;\n",
                      routine_type[i], routine_name);

            if (mysql_num_fields(routine_res) >= 6)
            {
              if (switch_db_collation(sql_file, db_name_buff, ";",
                                      db_cl_name, row[5], &db_cl_altered))
              {
                DBUG_RETURN(1);
              }

              switch_cs_variables(sql_file, ";",
                                  row[3],   /* character_set_client */
                                  row[3],   /* character_set_results */
                                  row[4]);  /* collation_connection */
            }
            else
            {
              /*
                mysqldump is being run against the server, that does not
                provide character set information in SHOW CREATE
                statements.

                NOTE: the dump may be incorrect, since character set
                information is required in order to restore stored
                procedure/function properly.
              */

              fprintf(sql_file,
                      "--\n"
                      "-- WARNING: old server version. "
                        "The following dump may be incomplete.\n"
                      "--\n");
            }


            switch_sql_mode(sql_file, ";", row[1]);

            fprintf(sql_file,
                    "DELIMITER ;;\n"
                    "%s ;;\n"
                    "DELIMITER ;\n",
                    (const char *) row[2]);

            restore_sql_mode(sql_file, ";");

            if (mysql_num_fields(routine_res) >= 6)
            {
              restore_cs_variables(sql_file, ";");

              if (db_cl_altered)
              {
                if (restore_db_collation(sql_file, db_name_buff, ";", db_cl_name))
                  DBUG_RETURN(1);
              }
            }

          }
        } /* end of routine printing */

      } /* end of list of routines */
    }
  } /* end of for i (0 .. 1)  */

  if (switch_character_set_results(default_charset.c_str()))
    DBUG_RETURN(1);

  if (opt_lock_tables)
    (void) mysql_query_with_exception(mysql, 0, "UNLOCK TABLES");
  DBUG_RETURN(0);
}

int Dump_Handler::dump_all_tables_in_db(char *database)
{
  char *table;
  uint numrows;
  char table_buff[NAME_LEN*2+3];
  char hash_key[2*NAME_LEN+2];  /* "db.tablename" */
  char *afterdot;
  my_bool general_log_table_exists= 0, slow_log_table_exists=0;
  int using_mysql_db= !my_strcasecmp(charset_info, database, "mysql");
  my_bool real_columns[MAX_FIELDS];
  char err_buff[512];

  DBUG_ENTER("Dump_Handler::dump_all_tables_in_db");

  afterdot= my_stpcpy(hash_key, database);
  *afterdot++= '.';

  std::function<int(char *)> init_func= std::bind(&Dump_Handler::init_dumping_tables, 
                                                  this, std::placeholders::_1);
  if (init_dumping(database, init_func))
    DBUG_RETURN(1);

  if (opt_lock_tables)
  {
    DYNAMIC_STRING query;
    init_dynamic_string_checked(&query, "LOCK TABLES ", 256, 1024);

    /* make sure the dynamic string will be freed properly */
    DYNAMIC_STR_GUARD dstr_guard(&query);

    for (numrows= 0 ; (table= getTableName(1)) ; )
    {
      char *end= my_stpcpy(afterdot, table);
      if (include_table(hash_key, end - hash_key))
      {
        numrows++;
        dynstr_append_checked(&query, quote_name(table, table_buff, 1));
        dynstr_append_checked(&query, " READ /*!32311 LOCAL */,");
      }
    }
    if (numrows && mysql_real_query(mysql, query.str, (ulong)(query.length-1)))
      throw DB_exception(mysql, "when using LOCK TABLES");
            /* We shall continue here, if --force was given */
  }

  if (opt_flush_logs)
  {
    if (client_mysql_refresh(mysql, REFRESH_LOG))
      throw DB_exception(mysql, "when doing refresh");
           /* We shall continue here, if --force was given */
    else
      verbose_msg("-- dump_all_tables_in_db : logs flushed successfully!\n");
  }

  if (opt_single_transaction && mysql_get_server_version(mysql) >= 50500)
  {
    verbose_msg("-- Setting savepoint...\n");
    if (mysql_query_with_exception(mysql, 0, "SAVEPOINT sp"))
      DBUG_RETURN(1);
  }

  while ((table= getTableName(0)))
  {
    char *end= my_stpcpy(afterdot, table);
    if (include_table(hash_key, end - hash_key))
    {
      try 
      {
        dump_table(table, database);
      } 
      catch (...) {
        if (order_by) {
          my_free(order_by);
          order_by= 0;
        }
        throw;  // rethrow
      }
      my_free(order_by);
      order_by= 0;
      
      if (opt_dump_triggers && mysql_get_server_version(mysql) >= 50009)
      {
        if (dump_triggers_for_table(table, database))
        {
          my_snprintf(err_buff, sizeof(err_buff), 
                "Error dumping triggers for table %s", table);
          throw TC_Dump_exception(err_buff);
        }
      }

      /**
        ROLLBACK TO SAVEPOINT in --single-transaction mode to release metadata
        lock on table which was already dumped. This allows to avoid blocking
        concurrent DDL on this table without sacrificing correctness, as we
        won't access table second time and dumps created by --single-transaction
        mode have validity point at the start of transaction anyway.
        Note that this doesn't make --single-transaction mode with concurrent
        DDL safe in general case. It just improves situation for people for whom
        it might be working.
      */
      if (opt_single_transaction && mysql_get_server_version(mysql) >= 50500)
      {
        verbose_msg("-- Rolling back to savepoint sp...\n");
        if (mysql_query_with_exception(mysql, 0, "ROLLBACK TO SAVEPOINT sp"))
          DBUG_RETURN(1);
      }
    }
    else
    {
      /*
        If general_log and slow_log exists in the 'mysql' database,
         we should dump the table structure. But we cannot
         call get_table_structure() here as 'LOCK TABLES' query got executed
         above on the session and that 'LOCK TABLES' query does not contain
         'general_log' and 'slow_log' tables. (you cannot acquire lock
         on log tables). Hence mark the existence of these log tables here and
         after 'UNLOCK TABLES' query is executed on the session, get the table
         structure from server and dump it in the file.
      */
      if (using_mysql_db)
      {
        if (!my_strcasecmp(charset_info, table, "general_log"))
          general_log_table_exists= 1;
        else if (!my_strcasecmp(charset_info, table, "slow_log"))
          slow_log_table_exists= 1;
      }
    }
  }

  if (opt_single_transaction && mysql_get_server_version(mysql) >= 50500)
  {
    verbose_msg("-- Releasing savepoint...\n");
    if (mysql_query_with_exception(mysql, 0, "RELEASE SAVEPOINT sp"))
      DBUG_RETURN(1);
  }

  if (opt_events && mysql_get_server_version(mysql) >= 50106)
  {
    DBUG_PRINT("info", ("Dumping events for database %s", database));
    dump_events_for_db(database);
  }
  if (opt_routines && mysql_get_server_version(mysql) >= 50009)
  {
    DBUG_PRINT("info", ("Dumping routines for database %s", database));
    dump_routines_for_db(database);
  }

  if (opt_lock_tables)
    (void) mysql_query_with_exception(mysql, 0, "UNLOCK TABLES");

  if (using_mysql_db)
  {
    char table_type[NAME_LEN];
    char ignore_flag;
    if (general_log_table_exists)
    {
      if (!get_table_structure((char *) "general_log",
                               database, table_type, &ignore_flag,
                               real_columns))
        verbose_msg("-- Warning: get_table_structure() failed with some internal "
                    "error for 'general_log' table\n");
    }
    if (slow_log_table_exists)
    {
      if (!get_table_structure((char *) "slow_log",
                               database, table_type, &ignore_flag,
                               real_columns))
        verbose_msg("-- Warning: get_table_structure() failed with some internal "
                    "error for 'slow_log' table\n");
    }
  }
  if (opt_flush_privileges && using_mysql_db)
  {
    fprintf(result_file,"\n--\n-- Flush Grant Tables \n--\n");
    fprintf(result_file,"\n/*! FLUSH PRIVILEGES */;\n");
  }
  DBUG_RETURN(0);
}

/*
View Specific database initalization.

SYNOPSIS
  init_dumping_views
  qdatabase      quoted name of the database

RETURN VALUES
  0        Success.
  1        Failure.
*/
int Dump_Handler::init_dumping_views(char *qdatabase MY_ATTRIBUTE((unused)))
{
    return 0;
} /* init_dumping_views */

/*
  Getting VIEW structure

  SYNOPSIS
    get_view_structure()
    table   view name
    db      db name

  RETURN
    0 OK
    1 ERROR
*/
my_bool Dump_Handler::get_view_structure(char *table, char* db)
{
  MYSQL_RES  *table_res = NULL;
  MYSQL_ROW  row;
  MYSQL_FIELD *field;
  char       *result_table, *opt_quoted_table;
  char       table_buff[NAME_LEN*2+3];
  char       table_buff2[NAME_LEN*2+3];
  char       query[QUERY_LENGTH];
  FILE       *sql_file= result_file;
  DBUG_ENTER("get_view_structure");

  if (opt_no_create_info) /* Don't write table creation info */
    DBUG_RETURN(0);

  /* make sure the mysql query result will be freed properly */
  MYSQL_RES_Mgr mysql_res_mgr(table_res);

  verbose_msg("-- Retrieving view structure for table %s...\n", table);

  result_table=     quote_name(table, table_buff, 1);
  opt_quoted_table= quote_name(table, table_buff2, 0);

  if (switch_character_set_results("binary"))
    DBUG_RETURN(1);

  my_snprintf(query, sizeof(query), "SHOW CREATE TABLE %s", result_table);

  if (mysql_query_with_exception(mysql, &table_res, query))
  {
    switch_character_set_results(default_charset.c_str());
    DBUG_RETURN(0);
  }

  /* Check if this is a view */
  field= client_mysql_fetch_field_direct(table_res, 0);
  if (strcmp(field->name, "View") != 0)
  {
    switch_character_set_results(default_charset.c_str());
    verbose_msg("-- It's base table, skipped\n");
    mysql_free_result_with_set_null(table_res);
    DBUG_RETURN(0);
  }

  print_comment("\n--\n-- Final view structure for view %s\n--\n\n",
                fix_identifier_with_newline(result_table));

  verbose_msg("-- Dropping the temporary view structure created\n");
  fprintf(sql_file, "/*!50001 DROP VIEW IF EXISTS %s*/;\n", opt_quoted_table);

  my_snprintf(query, sizeof(query),
              "SELECT CHECK_OPTION, DEFINER, SECURITY_TYPE, "
              "       CHARACTER_SET_CLIENT, COLLATION_CONNECTION "
              "FROM information_schema.views "
              "WHERE table_name=\"%s\" AND table_schema=\"%s\"", table, db);

  if (mysql_real_query(mysql, query, strlen(query)))
  {
    /*
      Use the raw output from SHOW CREATE TABLE if
       information_schema query fails.
     */
    row= mysql_fetch_row(table_res);
    fprintf(sql_file, "/*!50001 %s */;\n", row[1]);
    check_io(sql_file);
    mysql_free_result_with_set_null(table_res);
  }
  else
  {
    char *ptr;
    ulong *lengths;
    char search_buf[256], replace_buf[256];
    ulong search_len, replace_len;
    DYNAMIC_STRING ds_view;

    /* Save the result of SHOW CREATE TABLE in ds_view */
    row= mysql_fetch_row(table_res);
    lengths= mysql_fetch_lengths(table_res);
    init_dynamic_string_checked(&ds_view, row[1], lengths[1] + 1, 1024);

    /* make sure the dynamic string will be freed properly */
    DYNAMIC_STR_GUARD ds_view_guard(&ds_view);

    mysql_free_result_with_set_null(table_res);

    /* Get the result from "select ... information_schema" */
    if (!(table_res= mysql_store_result(mysql)) ||
        !(row= mysql_fetch_row(table_res)))
    {
      if (table_res)
        mysql_free_result_with_set_null(table_res);
      dynstr_free(&ds_view);
      throw DB_exception(mysql, "when trying to save the result of SHOW CREATE TABLE in ds_view.");
      DBUG_RETURN(1);
    }

    lengths= mysql_fetch_lengths(table_res);

    /*
      "WITH %s CHECK OPTION" is available from 5.0.2
      Surround it with !50002 comments
    */
    if (strcmp(row[0], "NONE"))
    {

      ptr= search_buf;
      search_len= (ulong)(strxmov(ptr, "WITH ", row[0],
                                  " CHECK OPTION", NullS) - ptr);
      ptr= replace_buf;
      replace_len=(ulong)(strxmov(ptr, "*/\n/*!50002 WITH ", row[0],
                                  " CHECK OPTION", NullS) - ptr);
      replace(&ds_view, search_buf, search_len, replace_buf, replace_len);
    }

    /*
      "DEFINER=%s SQL SECURITY %s" is available from 5.0.13
      Surround it with !50013 comments
    */
    {
      size_t     user_name_len;
      char       user_name_str[USERNAME_LENGTH + 1];
      char       quoted_user_name_str[USERNAME_LENGTH * 2 + 3];
      size_t     host_name_len;
      char       host_name_str[HOSTNAME_LENGTH + 1];
      char       quoted_host_name_str[HOSTNAME_LENGTH * 2 + 3];

      parse_user(row[1], lengths[1], user_name_str, &user_name_len,
                 host_name_str, &host_name_len);

      ptr= search_buf;
      search_len=
        (ulong)(strxmov(ptr, "DEFINER=",
                        quote_name(user_name_str, quoted_user_name_str, FALSE),
                        "@",
                        quote_name(host_name_str, quoted_host_name_str, FALSE),
                        " SQL SECURITY ", row[2], NullS) - ptr);
      ptr= replace_buf;
      replace_len=
        (ulong)(strxmov(ptr, "*/\n/*!50013 DEFINER=",
                        quote_name(user_name_str, quoted_user_name_str, FALSE),
                        "@",
                        quote_name(host_name_str, quoted_host_name_str, FALSE),
                        " SQL SECURITY ", row[2],
                        " */\n/*!50001", NullS) - ptr);
      replace(&ds_view, search_buf, search_len, replace_buf, replace_len);
    }

    /* Dump view structure to file */

    fprintf(sql_file,
            "/*!50001 SET @saved_cs_client          = @@character_set_client */;\n"
            "/*!50001 SET @saved_cs_results         = @@character_set_results */;\n"
            "/*!50001 SET @saved_col_connection     = @@collation_connection */;\n"
            "/*!50001 SET character_set_client      = %s */;\n"
            "/*!50001 SET character_set_results     = %s */;\n"
            "/*!50001 SET collation_connection      = %s */;\n"
            "/*!50001 %s */;\n"
            "/*!50001 SET character_set_client      = @saved_cs_client */;\n"
            "/*!50001 SET character_set_results     = @saved_cs_results */;\n"
            "/*!50001 SET collation_connection      = @saved_col_connection */;\n",
            (const char *) row[3],
            (const char *) row[3],
            (const char *) row[4],
            (const char *) ds_view.str);

    check_io(sql_file);
    mysql_free_result_with_set_null(table_res);
    dynstr_free(&ds_view);
  }

  if (switch_character_set_results(default_charset.c_str()))
    DBUG_RETURN(1);

  DBUG_RETURN(0);
}

/*
   dump structure of views of database

   SYNOPSIS
     dump_all_views_in_db()
     database  database name

  RETURN
    0 OK
    1 ERROR
*/
my_bool Dump_Handler::dump_all_views_in_db(char *database)
{
  char *table;
  uint numrows;
  char table_buff[NAME_LEN*2+3];
  char hash_key[2*NAME_LEN+2];  /* "db.tablename" */
  char *afterdot;

  afterdot= my_stpcpy(hash_key, database);
  *afterdot++= '.';

  std::function<int(char*)> init_func = std::bind(&Dump_Handler::init_dumping_views, 
                                                  this, std::placeholders::_1);
  if (init_dumping(database, init_func))
    return 1;

  if (opt_lock_tables)
  {
    DYNAMIC_STRING query;
    init_dynamic_string_checked(&query, "LOCK TABLES ", 256, 1024);

    /* make sure the dynamic string will be freed properly */
    DYNAMIC_STR_GUARD dstr_guard(&query);

    for (numrows= 0 ; (table= getTableName(1)); )
    {
      char *end= my_stpcpy(afterdot, table);
      if (include_table(hash_key, end - hash_key))
      {
        numrows++;
        dynstr_append_checked(&query, quote_name(table, table_buff, 1));
        dynstr_append_checked(&query, " READ /*!32311 LOCAL */,");
      }
    }
    if (numrows && mysql_real_query(mysql, query.str, (ulong)(query.length-1)))
      throw DB_exception(mysql, "when using LOCK TABLES");
            /* We shall continue here, if --force was given */
  }
  if (opt_flush_logs)
  {
    if (client_mysql_refresh(mysql, REFRESH_LOG))
      throw DB_exception(mysql, "when doing refresh");
           /* We shall continue here, if --force was given */
    else
      verbose_msg("-- dump_all_views_in_db : logs flushed successfully!\n");
  }
  while ((table= getTableName(0)))
  {
    char *end= my_stpcpy(afterdot, table);
    if (include_table(hash_key, end - hash_key))
      get_view_structure(table, database);
  }

  if (opt_lock_tables)
    (void) mysql_query_with_exception(mysql, 0, "UNLOCK TABLES");
  return 0;
}

int Dump_Handler::dump_all_databases()
{
  MYSQL_ROW row;
  MYSQL_RES *tableres;
  int result=0;

  { /* dump tables begin */
  if (mysql_query_with_exception(mysql, &tableres, "SHOW DATABASES"))
    return 1;

  /* make sure the result set will be freed properly */
  MYSQL_RES_GUARD(tableres);

  while ((row= mysql_fetch_row(tableres)))
  {
    if (mysql_get_server_version(mysql) >= FIRST_INFORMATION_SCHEMA_VERSION &&
        !my_strcasecmp(&my_charset_latin1, row[0], INFORMATION_SCHEMA_DB_NAME))
      continue;

    if (mysql_get_server_version(mysql) >= FIRST_PERFORMANCE_SCHEMA_VERSION &&
        !my_strcasecmp(&my_charset_latin1, row[0], PERFORMANCE_SCHEMA_DB_NAME))
      continue;

    if (mysql_get_server_version(mysql) >= FIRST_SYS_SCHEMA_VERSION &&
        !my_strcasecmp(&my_charset_latin1, row[0], SYS_SCHEMA_DB_NAME))
      continue;

    if (is_ndbinfo(row[0]))
      continue;

    if (include_database(row[0], strlen(row[0])))
      if (dump_all_tables_in_db(row[0]))
        result = 1;
  }
  } /* dump tables end */

  { /* dump views begin */
  if (seen_views)
  {
    const char *show_db_query = "SHOW DATABASES";
    if (mysql_real_query(mysql, show_db_query, strlen(show_db_query)) ||
        !(tableres= mysql_store_result(mysql)))
    {
      fprintf(error_file, "Error: Couldn't execute 'SHOW DATABASES': %s\n",
                      mysql_error(mysql));
      fflush(error_file);
      return 1;
    }

    /* make sure the result set will be freed properly */
    MYSQL_RES_GUARD(tableres);

    while ((row= mysql_fetch_row(tableres)))
    {
      if (mysql_get_server_version(mysql) >= FIRST_INFORMATION_SCHEMA_VERSION &&
          !my_strcasecmp(&my_charset_latin1, row[0], INFORMATION_SCHEMA_DB_NAME))
        continue;

      if (mysql_get_server_version(mysql) >= FIRST_PERFORMANCE_SCHEMA_VERSION &&
          !my_strcasecmp(&my_charset_latin1, row[0], PERFORMANCE_SCHEMA_DB_NAME))
        continue;

      if (mysql_get_server_version(mysql) >= FIRST_SYS_SCHEMA_VERSION &&
          !my_strcasecmp(&my_charset_latin1, row[0], SYS_SCHEMA_DB_NAME))
        continue;

      if (is_ndbinfo(row[0]))
        continue;

      if (include_database(row[0], strlen(row[0])))
        if (dump_all_views_in_db(row[0]))
          result = 1;
    }
  }
  } /* dump views end */

  return result;
}

void Dump_Handler::free_resources()
{
  if (insert_pat_inited)
    dynstr_free(&insert_pat);

  if (skipped_keys_list)
  {
    uint keys;
    for (keys= list_length(skipped_keys_list); keys > 0; keys--)
    {
      LIST *node= skipped_keys_list;
      char *def= (char *)(node->data);

      skipped_keys_list= list_delete(skipped_keys_list, node);
      my_free(def);
      my_free(node);
    }
  }

  if (my_hash_inited(&processed_compression_dictionaries))
    my_hash_free(&processed_compression_dictionaries);
}

bool Dump_Handler::dump_schema_main(string &errmsg) noexcept
{
  bool error_occurred= false;
  errmsg.clear();

  if(check_options(errmsg)) {
    return true;
  }

  if(!(result_file= my_fopen(result_file_name.c_str(), 
                          O_WRONLY | FILE_BINARY,
                          MYF(MY_WME)))) {
    errmsg= "Failed to open result file: " + result_file_name;
    return true;
  }
  /* make sure the result file will be closed properly */
  DUMP_FILE_GUARD result_file_guard(result_file);

  if (!(error_file= my_fopen(log_error_file_name.c_str(), 
                          O_WRONLY | FILE_BINARY | O_APPEND,
                          MYF(MY_WME)))) {
    errmsg= "Failed to open error log file: " + log_error_file_name;
    return true;
  }
  /* make sure the error file will be closed properly */
  DUMP_FILE_GUARD error_file_guard(error_file);

  try
  {
    if(connect_to_server()) {
      errmsg= "Failed to connect to server: " + 
              string(target_server->host ? target_server->host : "")
              + ":" + std::to_string(target_server->port);
      fprintf(error_file, "%s\n", errmsg.c_str());
      fflush(error_file);
      return true;
    }

    /* make sure the connection will be closed properly */
    DUMP_MYSQL_GUARD mysql_guard(this);

    write_header();

    if (opt_lock_for_backup && !server_supports_backup_locks())
    {
      errmsg= "Error: --lock-for-backup was specified with "
            "--single-transaction, but the server does not support "
            "LOCK TABLES FOR BACKUP.";
      fprintf(error_file, "%s\n", errmsg.c_str());
      fflush(error_file);
      goto err;
    }

    if (opt_lock_all_tables)
    {
      if (do_flush_tables_read_lock(mysql))
        goto err;
    }
    else if (opt_lock_for_backup && do_lock_tables_for_backup(mysql))
      goto err;

    /*
      Flush logs before starting transaction since
      this causes implicit commit starting mysql-5.5.
    */
    if (opt_lock_all_tables ||
        (opt_single_transaction && opt_flush_logs))
    {
      if (opt_flush_logs)
      {
        if (client_mysql_refresh(mysql, REFRESH_LOG))
        {
          throw DB_exception(mysql, "when doing refresh");
          goto err;
        }
        verbose_msg("-- main : logs flushed successfully!\n");
      }

      /* Not anymore! That would not be sensible. */
      opt_flush_logs= 0;
    }

    if (has_session_variables_like(mysql, "rocksdb_skip_fill_cache"))
      mysql_query_with_exception(mysql, 0,
                                    "SET SESSION rocksdb_skip_fill_cache=1");

    if (opt_single_transaction && start_transaction(mysql))
      goto err;

    /* Process opt_set_gtid_purged and add SET @@GLOBAL.GTID_PURGED if required. */
    if (process_set_gtid_purged(mysql))
      goto err;

    if (opt_single_transaction && (!opt_lock_for_backup) &&
        do_unlock_tables(mysql))                  /* unlock but no commit! */
      goto err;

    if (opt_alltspcs)
      dump_all_tablespaces();

    if (opt_alldbs)
    {
      if (!opt_alltspcs && !opt_notspcs)
        dump_all_tablespaces();
      dump_all_databases();
    }
    else
    {
      /*
        TODO:
          The case when opt_alldbs is false is not implemented yet.
      */
    }

    /*
      if --set-gtid-purged, restore binlog at the end of the session
      if required.
    */
    set_session_binlog(TRUE);

    int result_fd;
    if(result_file) {
      result_fd = my_fileno(result_file);
    } else {
      errmsg = "result_file is unexpectedly NULL";
      fprintf(error_file, "%s\n", errmsg.c_str());
      fflush(error_file);
      goto err;
    }

    /* 
      Ensure dumped data flushed.
      First we will flush the file stream data to kernel buffers with fflush().
      Second we will flush the kernel buffers data to physical disk file with
      my_sync(), this will make sure the data succeessfully dumped to disk file.
      fsync() fails with EINVAL if stdout is not redirected to any file, hence
      MY_IGNORE_BADFD is passed to ingnore that error.
    */
    if (result_file &&
        (fflush(result_file) || my_sync(result_fd, MYF(MY_IGNORE_BADFD))))
    {
      goto err;
    }

    goto footer;

    /*
      No reason to explicitely COMMIT the transaction, neither to explicitely
      UNLOCK TABLES: these will be automatically be done by the server when we
      disconnect now. Saves some code here, some network trips, adds nothing to
      server.
    */
  err:
    error_occurred= true;
  footer:
    write_footer();

  }
  catch (const std::exception& e) { 
    errmsg= string(e.what());
    fprintf(error_file, "%s\n", errmsg.c_str());
    fflush(error_file);
    error_occurred= true;
  }
  catch (...) {
    errmsg= "Unknown exception";
    fprintf(error_file, "%s\n", errmsg.c_str());
    fflush(error_file);
    error_occurred= true;
  }

  free_resources();

  return error_occurred;
}

/**
 * @brief Dumps the schema of a target server to a result file.
 * @param thd The thread handler for the current session.
 * @param target_server The foreign server to dump schema from.
 * @param result_file The file path to store the dumped schema.
 * @param log_error_file The file path to log errors during the dump process.
 * @return true if an error occurs, false otherwise.
 */
bool Dump_Handler::tc_dump_node_schema(THD *thd, 
                                  const FOREIGN_SERVER * const target_server,
                                  const std::string &result_file,
                                  const std::string &log_error_file)
{
  // Validate target_server is not NULL
  if(target_server == NULL) {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), 
              "target_server is NULL when dump schema");
    return true;
  }

  // Connect to the target server
  MYSQL *conn = tc_conn_connect(target_server->host, 
                                target_server->port, 
                                target_server->username, 
                                target_server->password, 
                                target_server->scheme);
  if(conn == NULL)
  {
    std::string ipport = std::string(target_server->host) + "#" 
                          + std::to_string(target_server->port);
    my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
    return true;
  }
  MYSQL_GUARD(conn);

  // Get the character set of the target server
  string charset;
  if(tc_get_variable_value(conn, "@@character_set_server", charset)) {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), 
              "Failed to get value of variable \'character_set_server\' "
              "when dump schema");
    return true;
  }

  // Initialize the Dump_Handler with the provided parameters
  Dump_Handler tc_dump_hdl(thd, target_server, result_file, log_error_file);

  // Configure dump options
  tc_dump_hdl.set_opt_single_transaction(true);                         // --single-transaction
  tc_dump_hdl.set_opt_autocommit(false);                                // --no-autocommit
  tc_dump_hdl.set_skip_optimization();                                  // --skip-opt
  tc_dump_hdl.set_opt_create_options(true);                             // --create-options
  tc_dump_hdl.set_opt_routines(true);                                   // --routines
  tc_dump_hdl.set_opt_quick(true);                                      // --quick
  tc_dump_hdl.set_default_charset(charset);                             //--default-character-set=xxx

  // Skip specified databases if any
  if (tc_skip_dump_db_list)
  {
    std::stringstream ignore_db_list(tc_skip_dump_db_list);
    std::string db_name;
    while (std::getline(ignore_db_list, db_name, ',')) {
      if(!db_name.empty()) {
        tc_dump_hdl.set_ignore_database(db_name);  // --ignore-database=xxx
      }
    }
  }

  // Handle TDBCTL-specific GTID mode
  if (strcasecmp(target_server->scheme, TDBCTL_WRAPPER) == 0)
  {
    tc_dump_hdl.set_opt_set_gtid_purged_mode(
                        Dump_Handler::SET_GTID_PURGED_AUTO);      // --set-gtid-purged=auto 
    tc_dump_hdl.set_opt_print_tc_admin_info(true);  // --print-tc-admin-info
  }

  // Execute the schema dump and handle errors
  string errmsg;
  if (tc_dump_hdl.dump_schema_main(errmsg))  // do dump
  {
    sql_print_error("TDBCTL Dump Handler: %s", errmsg.c_str());

    my_error(ER_TCADMIN_DUMP_NODE_ERROR, MYF(0), 
            result_file.c_str(), target_server->host, target_server->port, 
            log_error_file.c_str());
    return true;
  }

  // Log success message
  sql_print_information("Successfully dump schema from %s#%ld to file %s.", 
                        target_server->host, target_server->port, result_file.c_str());
  return false;
}