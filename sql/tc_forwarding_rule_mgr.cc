#include "tc_forwarding_rule_mgr.h"

#include "sql_class.h"             // THD
#include "json_dom.h"              // Json_dom::parse
#include "tc_base.h"               // TC_SPIDER_NEED_EXECUTE
#include "log.h"                   // sql_print_error


/* Removes whitespace at the beginning and end of the string. */
std::string string_strip(const std::string &str) {
  std::string result = str;
  int lpos = 0, rpos = result.size() - 1;
  while(lpos < result.size() && isspace(result[lpos]))
    ++lpos;
  while(rpos >= 0 && isspace(result[rpos]))
    --rpos;
  if(lpos > rpos)
    return "";
  result = result.substr(lpos, rpos - lpos + 1);
  return result;
}

/* Removes whitespace at the beginning and end of the string and converts English letters to uppercase. */
std::string trim_and_uppercase(const std::string &str) {
    std::string result = string_strip(str);
    for(int i = 0; i < result.size(); ++i) {
      result[i] = toupper(result[i]);
    }
    return result;
}


Supported_SQL_Command supported_sql_commands[] = {
  {"SQLCOM_ALTER_DB", SQLCOM_ALTER_DB, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_SPIDER_EXECUTE_FIRST | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_SPIDER_EXECUTE_FIRST | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ALTER_DB_UPGRADE", SQLCOM_ALTER_DB_UPGRADE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ALTER_EVENT", SQLCOM_ALTER_EVENT, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ALTER_FUNCTION", SQLCOM_ALTER_FUNCTION, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ALTER_INSTANCE", SQLCOM_ALTER_INSTANCE, 0, 0, 0, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ALTER_PROCEDURE", SQLCOM_ALTER_PROCEDURE, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ALTER_SERVER", SQLCOM_ALTER_SERVER, 0, 0, 0, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ALTER_TABLE", SQLCOM_ALTER_TABLE, TC_SPIDER_EXECUTE_FIRST | TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_EXECUTE_FIRST | TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ALTER_TABLESPACE", SQLCOM_ALTER_TABLESPACE, 0, 0, 0, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ALTER_USER", SQLCOM_ALTER_USER, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_ANALYZE", SQLCOM_ANALYZE, 0, 0, 0, false, ""},
  {"SQLCOM_ASSIGN_TO_KEYCACHE", SQLCOM_ASSIGN_TO_KEYCACHE, 0, 0, 0, false, ""},
  {"SQLCOM_BEGIN", SQLCOM_BEGIN, TC_TDBCTL_NEED_EXECUTE | TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE | TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_BINLOG_BASE64_EVENT", SQLCOM_BINLOG_BASE64_EVENT, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_CALL", SQLCOM_CALL, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_CHANGE_DB", SQLCOM_CHANGE_DB, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_CHANGE_MASTER", SQLCOM_CHANGE_MASTER, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_CHANGE_REPLICATION_FILTER", SQLCOM_CHANGE_REPLICATION_FILTER, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_CHECK", SQLCOM_CHECK, 0, 0, 0, false, ""},
  {"SQLCOM_CHECKSUM", SQLCOM_CHECKSUM, 0, 0, 0, false, ""},
  {"SQLCOM_COMMIT", SQLCOM_COMMIT, TC_TDBCTL_NEED_EXECUTE | TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE | TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_CREATE_COMPRESSION_DICTIONARY", SQLCOM_CREATE_COMPRESSION_DICTIONARY, 0, 0, 0, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_DB", SQLCOM_CREATE_DB, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_EVENT", SQLCOM_CREATE_EVENT, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_FUNCTION", SQLCOM_CREATE_FUNCTION, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_INDEX", SQLCOM_CREATE_INDEX, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_PROCEDURE", SQLCOM_CREATE_PROCEDURE, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_SERVER", SQLCOM_CREATE_SERVER, 0, 0, 0, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_SPFUNCTION", SQLCOM_CREATE_SPFUNCTION, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_TABLE", SQLCOM_CREATE_TABLE, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_TRIGGER", SQLCOM_CREATE_TRIGGER, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_USER", SQLCOM_CREATE_USER, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_CREATE_VIEW", SQLCOM_CREATE_VIEW, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DEALLOCATE_PREPARE", SQLCOM_DEALLOCATE_PREPARE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_DELETE", SQLCOM_DELETE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_DELETE_MULTI", SQLCOM_DELETE_MULTI, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_DO", SQLCOM_DO, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_DROP_COMPRESSION_DICTIONARY", SQLCOM_DROP_COMPRESSION_DICTIONARY, 0, 0, 0, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_DB", SQLCOM_DROP_DB, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_SPIDER_EXECUTE_FIRST | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_SPIDER_EXECUTE_FIRST | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_EVENT", SQLCOM_DROP_EVENT, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_FUNCTION", SQLCOM_DROP_FUNCTION, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_INDEX", SQLCOM_DROP_INDEX, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_PROCEDURE", SQLCOM_DROP_PROCEDURE, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_SERVER", SQLCOM_DROP_SERVER, 0, 0, 0, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_TABLE", SQLCOM_DROP_TABLE, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_SPIDER_EXECUTE_FIRST | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_SPIDER_EXECUTE_FIRST | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_TRIGGER", SQLCOM_DROP_TRIGGER, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_USER", SQLCOM_DROP_USER, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_DROP_VIEW", SQLCOM_DROP_VIEW, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_EMPTY_QUERY", SQLCOM_EMPTY_QUERY, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_EXECUTE", SQLCOM_EXECUTE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_EXPLAIN_OTHER", SQLCOM_EXPLAIN_OTHER, 0, 0, 0, false, ""},
  {"SQLCOM_FLUSH", SQLCOM_FLUSH, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_GET_DIAGNOSTICS", SQLCOM_GET_DIAGNOSTICS, 0, 0, 0, false, ""},
  {"SQLCOM_GRANT", SQLCOM_GRANT, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_HA_CLOSE", SQLCOM_HA_CLOSE, 0, 0, 0, false, ""},
  {"SQLCOM_HA_OPEN", SQLCOM_HA_OPEN, 0, 0, 0, false, ""},
  {"SQLCOM_HA_READ", SQLCOM_HA_READ, 0, 0, 0, false, ""},
  {"SQLCOM_HELP", SQLCOM_HELP, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_INSERT", SQLCOM_INSERT, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_INSERT_SELECT", SQLCOM_INSERT_SELECT, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_INSTALL_PLUGIN", SQLCOM_INSTALL_PLUGIN, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_KILL", SQLCOM_KILL, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_LOAD", SQLCOM_LOAD, 0, 0, 0, false, ""},
  {"SQLCOM_LOCK_BINLOG_FOR_BACKUP", SQLCOM_LOCK_BINLOG_FOR_BACKUP, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_LOCK_TABLES", SQLCOM_LOCK_TABLES, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE, false, ""},
  {"SQLCOM_LOCK_TABLES_FOR_BACKUP", SQLCOM_LOCK_TABLES_FOR_BACKUP, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_OPTIMIZE", SQLCOM_OPTIMIZE, 0, 0, 0, false, ""},
  {"SQLCOM_PRELOAD_KEYS", SQLCOM_PRELOAD_KEYS, 0, 0, 0, false, ""},
  {"SQLCOM_PREPARE", SQLCOM_PREPARE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_PURGE", SQLCOM_PURGE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_PURGE_BEFORE", SQLCOM_PURGE_BEFORE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_RELEASE_SAVEPOINT", SQLCOM_RELEASE_SAVEPOINT, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_RENAME_TABLE", SQLCOM_RENAME_TABLE, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_RENAME_USER", SQLCOM_RENAME_USER, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_REPAIR", SQLCOM_REPAIR, 0, 0, 0, false, ""},
  {"SQLCOM_REPLACE", SQLCOM_REPLACE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_REPLACE_SELECT", SQLCOM_REPLACE_SELECT, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_RESET", SQLCOM_RESET, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_RESIGNAL", SQLCOM_RESIGNAL, 0, 0, 0, false, ""},
  {"SQLCOM_REVOKE", SQLCOM_REVOKE, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_REVOKE_ALL", SQLCOM_REVOKE_ALL, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_ROLLBACK", SQLCOM_ROLLBACK, TC_TDBCTL_NEED_EXECUTE | TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE | TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_ROLLBACK_TO_SAVEPOINT", SQLCOM_ROLLBACK_TO_SAVEPOINT, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SAVEPOINT", SQLCOM_SAVEPOINT, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SELECT", SQLCOM_SELECT, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SET_OPTION", SQLCOM_SET_OPTION, TC_TDBCTL_NEED_EXECUTE | TC_SPIDER_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE | TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_BINLOGS", SQLCOM_SHOW_BINLOGS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_BINLOG_EVENTS", SQLCOM_SHOW_BINLOG_EVENTS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_CHARSETS", SQLCOM_SHOW_CHARSETS, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_CLIENT_STATS", SQLCOM_SHOW_CLIENT_STATS, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_COLLATIONS", SQLCOM_SHOW_COLLATIONS, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_CREATE", SQLCOM_SHOW_CREATE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_CREATE_DB", SQLCOM_SHOW_CREATE_DB, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_CREATE_EVENT", SQLCOM_SHOW_CREATE_EVENT, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_CREATE_FUNC", SQLCOM_SHOW_CREATE_FUNC, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_CREATE_PROC", SQLCOM_SHOW_CREATE_PROC, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_CREATE_TRIGGER", SQLCOM_SHOW_CREATE_TRIGGER, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_CREATE_USER", SQLCOM_SHOW_CREATE_USER, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_DATABASES", SQLCOM_SHOW_DATABASES, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_ENGINE_LOGS", SQLCOM_SHOW_ENGINE_LOGS, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_ENGINE_MUTEX", SQLCOM_SHOW_ENGINE_MUTEX, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_ENGINE_STATUS", SQLCOM_SHOW_ENGINE_STATUS, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_ERRORS", SQLCOM_SHOW_ERRORS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_EVENTS", SQLCOM_SHOW_EVENTS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_FIELDS", SQLCOM_SHOW_FIELDS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_FUNC_CODE", SQLCOM_SHOW_FUNC_CODE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_GRANTS", SQLCOM_SHOW_GRANTS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_INDEX_STATS", SQLCOM_SHOW_INDEX_STATS, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_KEYS", SQLCOM_SHOW_KEYS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_MASTER_STAT", SQLCOM_SHOW_MASTER_STAT, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_OPEN_TABLES", SQLCOM_SHOW_OPEN_TABLES, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_PLUGINS", SQLCOM_SHOW_PLUGINS, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_PRIVILEGES", SQLCOM_SHOW_PRIVILEGES, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_PROCESSLIST", SQLCOM_SHOW_PROCESSLIST, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_PROC_CODE", SQLCOM_SHOW_PROC_CODE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_PROFILE", SQLCOM_SHOW_PROFILE, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_PROFILES", SQLCOM_SHOW_PROFILES, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_RELAYLOG_EVENTS", SQLCOM_SHOW_RELAYLOG_EVENTS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_SLAVE_HOSTS", SQLCOM_SHOW_SLAVE_HOSTS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_SLAVE_STAT", SQLCOM_SHOW_SLAVE_STAT, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_STATUS", SQLCOM_SHOW_STATUS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_STATUS_FUNC", SQLCOM_SHOW_STATUS_FUNC, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_STATUS_PROC", SQLCOM_SHOW_STATUS_PROC, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_STORAGE_ENGINES", SQLCOM_SHOW_STORAGE_ENGINES, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_TABLES", SQLCOM_SHOW_TABLES, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_TABLE_STATS", SQLCOM_SHOW_TABLE_STATS, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_TABLE_STATUS", SQLCOM_SHOW_TABLE_STATUS, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_THREAD_STATS", SQLCOM_SHOW_THREAD_STATS, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_TRIGGERS", SQLCOM_SHOW_TRIGGERS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_USER_STATS", SQLCOM_SHOW_USER_STATS, 0, 0, 0, false, ""},
  {"SQLCOM_SHOW_VARIABLES", SQLCOM_SHOW_VARIABLES, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHOW_WARNS", SQLCOM_SHOW_WARNS, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SHUTDOWN", SQLCOM_SHUTDOWN, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SIGNAL", SQLCOM_SIGNAL, 0, 0, 0, false, ""},
  {"SQLCOM_SLAVE_START", SQLCOM_SLAVE_START, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_SLAVE_STOP", SQLCOM_SLAVE_STOP, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_START_GROUP_REPLICATION", SQLCOM_START_GROUP_REPLICATION, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_STOP_GROUP_REPLICATION", SQLCOM_STOP_GROUP_REPLICATION, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_TRUNCATE", SQLCOM_TRUNCATE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, true, UNCHANGEABLE_DDL},
  {"SQLCOM_UNINSTALL_PLUGIN", SQLCOM_UNINSTALL_PLUGIN, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_UNLOCK_BINLOG", SQLCOM_UNLOCK_BINLOG, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, false, ""},
  {"SQLCOM_UNLOCK_TABLES", SQLCOM_UNLOCK_TABLES, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE, false, ""},
  {"SQLCOM_UPDATE", SQLCOM_UPDATE, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_UPDATE_MULTI", SQLCOM_UPDATE_MULTI, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, 0, TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false, ""},
  {"SQLCOM_XA_COMMIT", SQLCOM_XA_COMMIT, 0, 0, 0, false, ""},
  {"SQLCOM_XA_END", SQLCOM_XA_END, 0, 0, 0, false, ""},
  {"SQLCOM_XA_PREPARE", SQLCOM_XA_PREPARE, 0, 0, 0, false, ""},
  {"SQLCOM_XA_RECOVER", SQLCOM_XA_RECOVER, 0, 0, 0, false, ""},
  {"SQLCOM_XA_ROLLBACK", SQLCOM_XA_ROLLBACK, 0, 0, 0, false, ""},
  {"SQLCOM_XA_START", SQLCOM_XA_START, 0, 0, 0, false, ""},
  {"TC_SQLCOM_ALTER_NODE", TC_SQLCOM_ALTER_NODE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_ALTER_SERVER", TC_SQLCOM_ALTER_SERVER, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_ALTER_TABLE_UNSUPPORT", TC_SQLCOM_ALTER_TABLE_UNSUPPORT, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CHECK_ROUTING", TC_SQLCOM_CHECK_ROUTING, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CHECK_TABLE", TC_SQLCOM_CHECK_TABLE, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CHECK_TABLES", TC_SQLCOM_CHECK_TABLES, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CONN_NODE_EXECUTE_SQL", TC_SQLCOM_CONN_NODE_EXECUTE_SQL, TC_DESIGNATED_NODE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, 0, TC_DESIGNATED_NODE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CREATE_NODE", TC_SQLCOM_CREATE_NODE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CREATE_OR_DROP_UNIQUE_KEY", TC_SQLCOM_CREATE_OR_DROP_UNIQUE_KEY, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CREATE_SERVER", TC_SQLCOM_CREATE_SERVER, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CREATE_TABLE_WITH_CONNECT_STRING", TC_SQLCOM_CREATE_TABLE_WITH_CONNECT_STRING, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CREATE_TABLE_WITH_FIELD_CHARSET", TC_SQLCOM_CREATE_TABLE_WITH_FIELD_CHARSET, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CREATE_TABLE_WITH_SELECT", TC_SQLCOM_CREATE_TABLE_WITH_SELECT, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_CREATE_TABLE_WITH_TABLE_COMMENT", TC_SQLCOM_CREATE_TABLE_WITH_TABLE_COMMENT, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_DISABLE_PRIMARY", TC_SQLCOM_DISABLE_PRIMARY, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_DROP_NODE", TC_SQLCOM_DROP_NODE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_DROP_SERVER", TC_SQLCOM_DROP_SERVER, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_ENABLE_PRIMARY", TC_SQLCOM_ENABLE_PRIMARY, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_FLUSH_ROUTING", TC_SQLCOM_FLUSH_ROUTING, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_GET_PRIMARY", TC_SQLCOM_GET_PRIMARY, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_MONITOR_INIT", TC_SQLCOM_MONITOR_INIT, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_SHOW_PROCESSLIST", TC_SQLCOM_SHOW_PROCESSLIST, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL},
  {"TC_SQLCOM_SHOW_VARIABLES", TC_SQLCOM_SHOW_VARIABLES, TC_TDBCTL_NEED_EXECUTE, 0, TC_TDBCTL_NEED_EXECUTE, true, UNCHANGEABLE_TDBCTL_SQL}
};
const unsigned int supported_sql_count = sizeof(supported_sql_commands) / sizeof(Supported_SQL_Command);

Supported_Exec_Flag supported_execute_flag[] = {
  {"NO_EXEC", 0, false}, 
  {"SPIDER_ALL", TC_SPIDER_NEED_EXECUTE, false}, 
  {"REMOTE_ALL", TC_REMOTE_NEED_EXECUTE, false}, 
  {"TDBCTL", TC_TDBCTL_NEED_EXECUTE, false}, 
  {"SPIDER_FIRST", TC_SPIDER_EXECUTE_FIRST, false}, 
  {"SPIDER_ONE", TC_ONLY_ONE_SPIDER_NEED_EXECUTE, false}, 
  {"DESIGNATED_ONE", TC_DESIGNATED_NODE_NEED_EXECUTE, true}
};
const unsigned int settable_flag_count = sizeof(supported_execute_flag) / sizeof(Supported_Exec_Flag);

bool get_sql_command_by_name(const char *sql_name, Supported_SQL_Command &sql_command) {
  for(Supported_SQL_Command supported_sql: supported_sql_commands) {
    if(strcmp(sql_name, supported_sql.name) == 0) {
      sql_command = supported_sql;
      return true;
    }
  }
  return false;
}

bool get_execute_flag_by_name(const char *flag_name, Supported_Exec_Flag &exec_flag_item) {
  for(Supported_Exec_Flag flag_item: supported_execute_flag) {
    if(strcmp(flag_name, flag_item.name) == 0) {
      exec_flag_item = flag_item;
      return true;
    }
  }
  return false;
}

bool check_conflict_exec_flag(std::vector<Exec_Flag> &exec_flag_list) {
  bool spider_all = false;
  bool spider_one = false;
  bool spider_first = false;
  for(Exec_Flag x: exec_flag_list) {
    if(x == TC_SPIDER_NEED_EXECUTE) {
      spider_all = true;
    }
    if(x == TC_ONLY_ONE_SPIDER_NEED_EXECUTE) {
      spider_one = true;
    }
    if(x == TC_SPIDER_EXECUTE_FIRST) {
      spider_first = true;
    }
  }
  /* SPIDER_ALL and SPIDER_ONE cannot be set at the same time */
  if(spider_all && spider_one)
    return false;
  /* SPIDER_FIRST should be set with SPIDER_ALL or SPIDER_ONE */
  if(spider_first && (!spider_all && !spider_one))
    return false;
  return true;
}

bool check_exec_flag_range(std::vector<Exec_Flag> &exec_flag_list, Exec_Flag max_supported_rule) {
  for(Exec_Flag x: exec_flag_list) {
    if(~max_supported_rule & x) {
      if((x == TC_ONLY_ONE_SPIDER_NEED_EXECUTE) && (max_supported_rule & TC_SPIDER_NEED_EXECUTE)) {
        continue;
      }
      return false;
    }
  }
  return true;
}

bool is_sys_var_unchangeable(sys_var *sysvar, std::string *tips) {
  if(sysvar->is_tdbctl()) {
    if(tips) {
      *tips = "system variable \'" + std::string(sysvar->name.str) + "\' is a tdbctl variable";
    }
    return true;
  }
  return false;
}

std::string to_sql_command_name(enum_sql_command sql) {
  for(Supported_SQL_Command supported_sql: supported_sql_commands) {
    if(sql == supported_sql.sql_type) {
      return supported_sql.name;
    }
  }
  return "";
}

std::string to_single_exec_flag_name(Exec_Flag flag) {
  for(Supported_Exec_Flag flag_item: supported_execute_flag) {
    if(flag_item.single_flag == flag) {
      return flag_item.name;
    }
  }
  return "";
}

std::string exec_flag_to_string(Exec_Flag flag, const char *separater) {
  std::string str = "";
  for(Supported_Exec_Flag flag_item: supported_execute_flag) {
    if(flag_item.single_flag & flag) {
      str += std::string(flag_item.name) + separater;
    }
  }
  if(str.size() > 0) {
    str = str.substr(0, str.size() - strlen(separater));
  }
  return str;
}

Forwarding_rule_mgr::SQL_Rule_Cache Forwarding_rule_mgr::m_global_rules_cache;
Forwarding_rule_mgr::SQL_Rule_Cache Forwarding_rule_mgr::m_global_rules_valid_cache;
Forwarding_rule_mgr::Var_Rule_Cache Forwarding_rule_mgr::m_global_var_rules_cache;
Forwarding_rule_mgr::Var_Rule_Cache Forwarding_rule_mgr::m_global_var_rules_valid_cache;

Forwarding_rule_mgr::Forwarding_rule_mgr():m_primary_rules(SQLCOM_END, 0), m_secondary_rules(SQLCOM_END, 0) {
  m_session_rules_cache.reserve(supported_sql_count);
  Forwarding_rule_mgr::m_global_rules_cache.reserve(supported_sql_count);
  reset_primary_rules();
  for(Supported_SQL_Command supported_sql: supported_sql_commands) {
    m_secondary_rules[supported_sql.sql_type] = supported_sql.secondary_default_rule;
  }
  reset_primary_var_rules();
}

/**
  @brief get execution flag of a variable being set using the SET command.  

  @param var        [IN]        The variable being set using the SET command.

  @return
    the execution flag.
*/
Exec_Flag Forwarding_rule_mgr::get_var_execute_flag(set_var_base *var) {

  if((!var) || var->check_tdbctl_var()) {
    return TC_TDBCTL_NEED_EXECUTE;
  }

  if (tdbctl_is_primary) {
    if(var->var_type() == set_var_base::SET_VAR_SYS) {
      set_var *sysvar = down_cast<set_var *>(var);
      if(sysvar) {
        Var_Rule_Hash::iterator it = m_primary_var_rules.end();
        if((it = m_primary_var_rules.find(sysvar->var->name.str)) != m_primary_var_rules.end()) {
          return it->second;
        }
      }
    }
    return m_primary_rules[SQLCOM_SET_OPTION];
  }
  
  return m_secondary_rules[SQLCOM_SET_OPTION];
}

/**
  @brief get execute flag of the current sql command.  

  @param thd        [IN]        The thd handler.
  @param lex        [IN]        Containing LEX object.
  @param sql_cmd    [IN]        Sql command type.
  @param flag       [OUT]       Execution flag.

  @return
  true      Success,
  false     Error
*/
bool Forwarding_rule_mgr::get_sql_execute_flag(THD *thd, LEX *lex, enum_sql_command sql_cmd, Exec_Flag &execute_flag) {
  execute_flag = 0;
  switch (sql_cmd)
  {
    /*
      Complex sql forwarding rules of SQLCOM_SET_OPTION
    */
    case SQLCOM_SET_OPTION:
    {
      List_iterator_fast<set_var_base> var_it(lex->var_list);
      set_var_base *first_var, *var;
      Exec_Flag unique_flag = 0;
      if((first_var = var_it++)) {  // get the first variable's execution flag
        execute_flag = get_var_execute_flag(first_var);
        unique_flag = execute_flag;
      }
      while ((var = var_it++)) {  // get the rest of variables' execution flag
        execute_flag = get_var_execute_flag(var);
        if(execute_flag != unique_flag) {
          my_error(ER_TCADMIN_EXECUTE_ERROR, MYF(0), "can't set variables with different execution flags at the same time");
          return false;
        }
      }
      break;
    }
    /*
      Complex sql forwarding rules of SQLCOM_ALTER_TABLE
    */
    case SQLCOM_ALTER_TABLE:
    {
      if (tdbctl_is_primary) {
        execute_flag = TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
        if (!lex->alter_info.has_alter_partitions()) {
          /* Only non-partitioning operations are allowed to be sent to Spider */
          execute_flag |= TC_SPIDER_NEED_EXECUTE;
          if (lex->alter_info.flags == Alter_info::ALTER_DROP_COLUMN) {
            execute_flag |= TC_SPIDER_EXECUTE_FIRST;
          }
        }
      } else {
        execute_flag = 0;
      }
      break;
    }
    /*
      For common sql commands, get the latest forwarding rules directly
    */
    default:
    {
      if (tdbctl_is_primary)
        execute_flag = m_primary_rules[sql_cmd];
      else
        execute_flag = m_secondary_rules[sql_cmd];
      break;
    }
  }
  return true;
}

inline void Forwarding_rule_mgr::reset_primary_rules() {
  for(Supported_SQL_Command supported_sql: supported_sql_commands) {
    m_primary_rules[supported_sql.sql_type] = supported_sql.primary_default_rule;
  }
}

/**
  @brief Check whether the sql-level forwarding rules are valid.

  @note This function is called from the ON_CHECK() function of the session variable
        'tc_forwarding_rules'.

  @param thd        [IN]        The thd handler.
  @param var        [IN]        A pointer to set_var holding the specified json text of forwarding rules.
  @param err_msg    [OUT]       Send an error message when the check fails.

  @return
    true                    Success
    false                   Error
*/
bool Forwarding_rule_mgr::check_forwarding_rules(THD *thd, set_var *var, std::string &err_msg) {
  char *json_text = var->save_result.string_value.str;
  size_t json_text_len = var->save_result.string_value.length;

  std::string rectified_json_str;
  if(var->type == OPT_GLOBAL) {
    if(!parse_forwarding_rules(json_text, json_text_len, err_msg, &(Forwarding_rule_mgr::m_global_rules_cache), &rectified_json_str)) {
      return false;
    }
  } else {
    if(!parse_forwarding_rules(json_text, json_text_len, err_msg, &(thd->forward_rule_mgr.m_session_rules_cache), &rectified_json_str)) {
      return false;
    }
  }
    
  /*
    print the rectified forwarding rules to be set.
  */
  sql_print_information("<tc_forwarding_rules> pass check: %s sql-level forwarding rules: \'%s\', rectified forwarding rules: \'%s\'.", 
    var->type == OPT_GLOBAL ? "global" : "session", json_text, rectified_json_str.c_str());

  return true;
}

/**
  @brief Method called during the server startup to verify the contents
         of @@tc_forwarding_rules.

  @param json_text        [IN]        json string describing the forwarding rules.
  @param json_text_len    [IN]        length of json string.
  @param err_str          [OUT]       Pass an error message when parsing fails.

  @return   false           Success
            true            failure
*/
bool Forwarding_rule_mgr::server_boot_verify_forwarding_rules(const char *json_text, size_t json_text_len, std::string &err_str) {
  if(!parse_forwarding_rules(json_text, json_text_len, err_str, &(Forwarding_rule_mgr::m_global_rules_cache))) {
    return true;
  }
  Forwarding_rule_mgr::m_global_rules_valid_cache = Forwarding_rule_mgr::m_global_rules_cache;
  return false;
}

/**
  @brief Method called during the server startup to verify the contents
         of @@tc_var_rules.

  @param json_text        [IN]        json string describing the forwarding rules.
  @param json_text_len    [IN]        length of json string.
  @param err_str          [OUT]       Pass an error message when parsing fails.

  @return   false           Success
            true            failure
*/
bool Forwarding_rule_mgr::server_boot_verify_variable_rules(const char *json_text, size_t json_text_len, std::string &err_str, std::string &wrn_str) {
  if(!parse_sysvar_rules(json_text, json_text_len, err_str, wrn_str, 0, &(Forwarding_rule_mgr::m_global_var_rules_cache))) {
    return true;
  }
  Forwarding_rule_mgr::m_global_var_rules_valid_cache = Forwarding_rule_mgr::m_global_var_rules_cache;
  return false;
}

/**
  @brief Parse json text to json dom.

  @param json_text        [IN]        json string describing the forwarding rules.
  @param json_text_len    [IN]        length of json string.
  @param err_str          [OUT]       Pass an error message when parsing fails.

  @return
    NULL                    Error,
    not NULL                Success
*/
Json_dom * Forwarding_rule_mgr::parse_json_text_to_dom(const char *json_text, size_t json_text_len, std::string &err_str) {
  err_str.clear();

  /* 
    Check if it is a valid json string.
  */
  if(!json_text) {
    err_str = "Get a null pointer of json text";
    return NULL;
  }
  if(!is_valid_json_syntax(json_text, json_text_len)) {
    err_str = "Invalid json string: \'" + std::string(json_text) + "\'";
    return NULL;
  }

  /* 
    Parse Json text to DOM.
  */
  const char *parse_err;
  size_t err_offset;
    /* 
      A property of the function Json_dom::parse: If the same key is specified multiple times, 
      only the first key makes sence, others will not be retained. 
    */
  Json_dom *json_dom = Json_dom::parse(json_text, json_text_len, &parse_err, &err_offset);
  if (json_dom == NULL) {
    if(parse_err != NULL) {
      err_str = parse_err;
    } else {
      err_str = "unknown error";
    }
  }
  return json_dom;
}

/**
  @brief Convert a comma-separated flag text to a Exec_Flag vector.

  @param text             [IN]        comma-separated string describing the execution flags.
  @param text_len         [IN]        length of string.
  @param err_str          [OUT]       Pass an error message when parsing fails.
  @param exec_flag_list   [OUT]       Exec_Flag vector.
  @param new_str          [OUT]       If it is not a null pointer, the rectified text describing the execution flags will be stored in it.

  @return
    true                    Success,
    false                   Error
*/
bool Forwarding_rule_mgr::parse_exec_flag_list(const char *text, size_t text_len, std::string &err_str, 
                                              std::vector<Exec_Flag> &exec_flag_list, std::string *new_str) {
  err_str.clear();
  if(new_str) {
    *new_str = "";
  }
  exec_flag_list.clear();
  exec_flag_list.reserve(settable_flag_count);

  std::unique_ptr<char> rule_list_str(new(std::nothrow) char[text_len + 1]);
  if(rule_list_str.get() == nullptr) {
    err_str = "Error when execute \'new(std::nothrow) char[text_len + 1]\'";
    return false;
  }
  strncpy(rule_list_str.get(), text, text_len);
  rule_list_str.get()[text_len] = '\0';

  char *token = NULL, *lasts = NULL;
  string new_token = "";
  Supported_Exec_Flag exec_flag_item;
  token = my_strtok_r(rule_list_str.get(), ",", &lasts);
  while(token)
  {
    new_token = trim_and_uppercase(string(token));
    if(!get_execute_flag_by_name(new_token.c_str(), exec_flag_item)) {
      err_str = "Invalid execute flag \'" + std::string(token) + "\'";
      return false;
    }
    if(exec_flag_item.cannot_set) {
      err_str = "This flag cannot be specified by users: \'" + std::string(exec_flag_item.name) + "\'";
      return false;
    }
    if(std::find(exec_flag_list.begin(), exec_flag_list.end(), exec_flag_item.single_flag) != exec_flag_list.end()) {
      err_str = "Duplicate execute flag \'" + std::string(exec_flag_item.name) + "\'";
      return false;
    }
    if(new_str) {
      *new_str += new_token + ", ";
    }
    exec_flag_list.push_back(exec_flag_item.single_flag);
    token = my_strtok_r(NULL, ",", &lasts);
  }

  if(new_str && (new_str->size() > 1)) {
    *new_str = new_str->substr(0, new_str->size() - 2);  // remove needless ', '.
  }
  return true;
}

/**
  @brief Parse sql-level forwarding rules expressed as json text.

  @param json_text        [IN]        json string describing the forwarding rules.
  @param json_text_len    [IN]        length of json string.
  @param err_str          [OUT]       Pass an error message when parsing fails.
  @param rules_cache      [OUT]       If it is not a null pointer, the parsed forwarding rules will be stored in it.
  @param new_json_str     [OUT]       If it is not a null pointer, the rectified json text describing the forwarding rules will be stored in it.

  @return
    true                    Success
    false                   Error
*/
bool Forwarding_rule_mgr::parse_forwarding_rules(const char *json_text, size_t json_text_len, std::string &err_str, SQL_Rule_Cache *rules_cache, std::string *new_json_str) {
  err_str.clear();

  /* 
    Parse Json text to DOM.
  */
  std::string parse_err;
  Json_dom *json_dom = parse_json_text_to_dom(json_text, json_text_len, parse_err);
  if (json_dom == NULL) {
    err_str = "Error when parsing json text: " + std::string(parse_err);
    return false;
  }
  Json_wrapper json_dom_mgr(json_dom);  // help us to manage json_dom resource.
  if (json_dom->json_type() != Json_dom::J_OBJECT) {
    err_str = "Only allow json text of type J_OBJECT";
    return false;
  }

  /*
    Check forwarding rules set by user carefully, and store them into rules_cache if rules_cache is not NULL.
  */
  if(rules_cache) {
    rules_cache->clear();
  }
  Json_object *rectified_json_object = NULL;
  if((rectified_json_object = new(std::nothrow) Json_object()) == NULL) {
    err_str = "Error when execute \'new(std::nothrow) Json_object()\'";
    return false;
  }
  Json_wrapper rectified_json_wrapper(rectified_json_object);  // help us to manage rectified_json_object.
  Json_object *origin_json_object = down_cast<Json_object *>(json_dom);

  for (Json_object::const_iterator it = origin_json_object->begin(); it != origin_json_object->end(); ++it) {
    /* get key and value */
    const std::string &key = it->first;
    Json_dom *value_dom = it->second;
    if (value_dom->json_type() != Json_dom::J_STRING) {
      err_str = "The value set to the key \'" + key + "\' is not a string";
      return false;
    }
    const std::string &value = down_cast<Json_string *>(value_dom)->value();

    /* check one key(sql_command) */
    std::string new_key = trim_and_uppercase(key);
    Supported_SQL_Command sql_cmd;
    if(!get_sql_command_by_name(new_key.c_str(), sql_cmd)) {
      err_str = "Unknown sql command \'" + key + "\'";
      return false;
    }
    if(sql_cmd.unchangeable) {
      err_str = "Setting forwarding rule for sql command \'" + std::string(sql_cmd.name) + "\' is forbidden (tips: " + std::string(sql_cmd.tips) + ")";
      return false;
    }
    if(rectified_json_object->get(new_key) != NULL){
      err_str = "Duplicate sql command \'" + std::string(sql_cmd.name) + "\'";
      return false;
    }

    /* check values(forwarding rules) */
    if(value.size() == 0) {
      err_str = "Empty string for key \'" + key + "\'";
      return false;
    }
    vector<Exec_Flag> exec_flag_list;
    std::string new_value;
    if(!parse_exec_flag_list(value.c_str(), value.size(), parse_err, exec_flag_list, &new_value)) {
      err_str = parse_err;
      return false;
    }
    if (exec_flag_list.size() == 0) {
      err_str = "No valid execute flag for sql command \'" + key + "\'";
      return false;
    }
    if(!check_conflict_exec_flag(exec_flag_list)){
      err_str = "Conflicting execute flags exist for \'" + key + "\'";
      return false;
    }
    if(!check_exec_flag_range(exec_flag_list, sql_cmd.max_supported_rule)){
      err_str = "Out-of-range flags exist for \'" + key + "\', max supported rule is \'" + 
        exec_flag_to_string(sql_cmd.max_supported_rule, " | ") + "\'";
      return false;
    }
    if(rectified_json_object->add_alias(new_key, new(std::nothrow) Json_string(new_value))) {
      err_str = "Error when execute \'add_alias\'";
      return false;
    }

    /* store a rule */
    if(rules_cache) {
      rules_cache->push_back({sql_cmd.sql_type, exec_flag_list});
    }
  }

  /*
    Get the rectified json text describing the forwarding rules.
  */
  if(new_json_str) {
    String buffer;
    if(rectified_json_wrapper.to_string(&buffer, true, "check_forwarding_rules")) {
      err_str = "Error when execute \'Json_wrapper::to_string\'";
      return false;
    }
    *new_json_str = std::string(buffer.c_ptr(), buffer.length());
  }

  return true;
}

/**
  @brief Parse system variables forwarding rules expressed as json text.

  @param json_text        [IN]        json string describing the forwarding rules.
  @param json_text_len    [IN]        length of json string.
  @param err_str          [OUT]       Pass an error message when parsing fails.
  @param wrn_str          [OUT]       Pass an warning message.
  @param thd              [OUT]       The thd handler needed by 'find_sys_var' function.
  @param rules_cache      [OUT]       If it is not a null pointer, the parsed forwarding rules will be stored in it.
  @param new_json_str     [OUT]       If it is not a null pointer, the rectified json text describing the forwarding rules will be stored in it.

  @return
    true                    Success
    false                   Error
*/
bool Forwarding_rule_mgr::parse_sysvar_rules(const char *json_text, 
                                            size_t json_text_len, 
                                            std::string &err_str, 
                                            std::string &wrn_str, 
                                            THD *thd,
                                            Var_Rule_Cache *rules_cache, 
                                            std::string *new_json_str) {
  err_str.clear();
  wrn_str.clear();
  std::vector<std::string> unknown_vars;

  /* 
    Parse Json text to DOM.
  */
  std::string parse_err;
  Json_dom *json_dom = parse_json_text_to_dom(json_text, json_text_len, parse_err);
  if (json_dom == NULL) {
    err_str = "Error when parsing json text: " + std::string(parse_err);
    return false;
  }
  Json_wrapper json_dom_mgr(json_dom);  // help us to manage json_dom resource.
  if (json_dom->json_type() != Json_dom::J_OBJECT) {
    err_str = "Only allow json text of type J_OBJECT";
    return false;
  }

  /*
    Check forwarding rules set by user carefully, and store them into rules_cache if rules_cache is not NULL.
  */
  if(rules_cache) {
    rules_cache->clear();
  }
  Json_object *rectified_json_object = NULL;
  if((rectified_json_object = new(std::nothrow) Json_object()) == NULL) {
    err_str = "Error when execute \'new(std::nothrow) Json_object()\'";
    return false;
  }
  Json_wrapper rectified_json_wrapper(rectified_json_object);  // help us to manage rectified_json_object.
  Json_object *origin_json_object = down_cast<Json_object *>(json_dom);

  {  // Limit the scope of plugin_locker
  
  Plugin_Locker plugin_locker();  // help us to lock/unlock LOCK_plugin

  for (Json_object::const_iterator it = origin_json_object->begin(); it != origin_json_object->end(); ++it) {
    /* get key and value */
    const std::string &key = it->first;
    Json_dom *value_dom = it->second;
    if (value_dom->json_type() != Json_dom::J_STRING) {
      err_str = "The value set to the key \'" + key + "\' is not a string";
      return false;
    }
    const std::string &value = down_cast<Json_string *>(value_dom)->value();

    /* check one key(system variable) */
    std::string var_name = string_strip(key);
    sys_var *sysvar = NULL;
    if(!(sysvar = find_sys_var_ex(thd, var_name.c_str(), var_name.size(), true, true))) {
      unknown_vars.push_back(var_name);
    } else {
      std::string unchangeable_tips;
      if(is_sys_var_unchangeable(sysvar, &unchangeable_tips)) {
        err_str = "Setting forwarding rule for system variable \'" + var_name + "\' is forbidden (tips: " + unchangeable_tips + ")";
        return false;
      }
      if(rectified_json_object->get(var_name) != NULL){
        err_str = "Duplicate system variable \'" + var_name + "\'";
        return false;
      }
    }
    
    /* check values(forwarding rules) */
    if(value.size() == 0) {
      err_str = "Empty string for key \'" + key + "\'";
      return false;
    }
    vector<Exec_Flag> exec_flag_list;
    std::string new_value;
    if(!parse_exec_flag_list(value.c_str(), value.size(), parse_err, exec_flag_list, &new_value)) {
      err_str = parse_err;
      return false;
    }
    if (exec_flag_list.size() == 0) {
      err_str = "No valid execute flag for system variable \'" + key + "\'";
      return false;
    }
    if(!check_conflict_exec_flag(exec_flag_list)){
      err_str = "Conflicting execute flags exist for \'" + key + "\'";
      return false;
    }
    if(rectified_json_object->add_alias(var_name, new(std::nothrow) Json_string(new_value))) {
      err_str = "Error when execute \'add_alias\'";
      return false;
    }

    /* store a rule */
    if(rules_cache) {
      rules_cache->push_back({var_name, exec_flag_list});
    }
  }

  }  // Limit the scope of plugin_locker

  /*
    Get the rectified json text describing the forwarding rules.
  */
  if(new_json_str) {
    String buffer;
    if(rectified_json_wrapper.to_string(&buffer, true, "check_var_rules")) {
      err_str = "Error when execute \'Json_wrapper::to_string\'";
      return false;
    }
    *new_json_str = std::string(buffer.c_ptr(), buffer.length());
  }

  if(unknown_vars.size() > 0) {
    wrn_str = "unknown system variables:";
    for(std::string elm: unknown_vars) {
      wrn_str += " " + elm + ",";
    }
    wrn_str.pop_back();
  }

  return true;
}

/**
  @brief update sql-level forwarding rules.

  @note This function is called from the ON_UPDATE() function of the session variable
        'tc_forwarding_rules'.

  @param thd    [IN]        The thd handler.
  @param type   [IN]        Specifies whether it is global or session.

  @return
    true                    Success
    false                   Error
*/
bool Forwarding_rule_mgr::update_forwarding_rules(THD *thd, enum_var_type type) {
  if(type != OPT_GLOBAL) {
    thd->forward_rule_mgr.reset_primary_rules();
    thd->forward_rule_mgr.cover_primary_forwarding_rules(thd->forward_rule_mgr.m_session_rules_cache);
  } else {
    Forwarding_rule_mgr::m_global_rules_valid_cache = Forwarding_rule_mgr::m_global_rules_cache;
  }
  return true;
}

/**
  @brief cover thread's primary sql-level forwarding rules by given rules cache.

  @param rules_cache    [IN]        verified new forwarding rules.
*/
void Forwarding_rule_mgr::cover_primary_forwarding_rules(const SQL_Rule_Cache &rules_cache) {
  for(SQL_Rule one_rule: rules_cache) {
    Exec_Flag exec_flag = 0;
    std::string rule_str = "";
    for(Exec_Flag rule_item: one_rule.second) {
      exec_flag |= rule_item;
      rule_str += to_single_exec_flag_name(rule_item) + " | ";
    }
    if(one_rule.second.size() > 0) {
      rule_str = rule_str.substr(0, rule_str.length() - 3);
    }
    m_primary_rules[one_rule.first] = exec_flag;
    // sql_print_information("new forwarding rules for \'%s\': \'%s\'.", 
    // to_sql_command_name(one_rule.first).c_str(), rule_str.c_str());
  }
}

inline void Forwarding_rule_mgr::reset_primary_var_rules() {
  m_primary_var_rules.clear();
}

/**
  @brief cover thread's primary variable-level forwarding rules by given rules cache.

  @param rules_cache    [IN]        verified new forwarding rules.
*/
void Forwarding_rule_mgr::cover_primary_var_rules(const Var_Rule_Cache &rules_cache) {
  for(Sys_Var_Rule one_rule: rules_cache) {
    Exec_Flag exec_flag = 0;
    std::string rule_str = "";
    for(Exec_Flag rule_item: one_rule.second) {
      exec_flag |= rule_item;
      rule_str += to_single_exec_flag_name(rule_item) + " | ";
    }
    if(one_rule.second.size() > 0) {
      rule_str = rule_str.substr(0, rule_str.length() - 3);
    }
    m_primary_var_rules[one_rule.first] = exec_flag;
    // sql_print_information("new forwarding rules for variable \'%s\': \'%s\'.", 
    //   one_rule.first.c_str(), rule_str.c_str());
  }
}

/**
  @brief Check whether the system variables' forwarding rules are valid.

  @note This function is called from the ON_CHECK() function of the session variable
        'tc_var_rules'.

  @param thd         [IN]        The thd handler.
  @param var         [IN]        A pointer to set_var holding the specified json text of forwarding rules.
  @param err_msg     [OUT]       Send an error message when the check fails.
  @param warn_msg    [OUT]       Pass a warning message.

  @return
    true                    Success
    false                   Error
*/
bool Forwarding_rule_mgr::check_var_rules(THD *thd, set_var *var, std::string &err_msg, std::string &warn_msg) {
  char *json_text = var->save_result.string_value.str;
  size_t json_text_len = var->save_result.string_value.length;

  std::string err_str, wrn_str, rectified_json_str;
  if(var->type == OPT_GLOBAL){
    if(!parse_sysvar_rules(json_text, json_text_len, err_msg, warn_msg, thd, 
      &(Forwarding_rule_mgr::m_global_var_rules_cache), &rectified_json_str)) {
      return false;
    }
  } else {
    if(!parse_sysvar_rules(json_text, json_text_len, err_msg, warn_msg, thd, 
      &(thd->forward_rule_mgr.m_session_var_rules_cache), &rectified_json_str)) {
      return false;
    }
  }
  
  /*
    print the rectified forwarding rules to be set.
  */
  sql_print_information("<tc_var_rules> pass check: %s variable-level forwarding rules: \'%s\', rectified forwarding rules: \'%s\'.", 
    var->type == OPT_GLOBAL ? "global" : "session", json_text, rectified_json_str.c_str());

  return true;
}

/**
  @brief update variable-level forwarding rules.

  @note This function is called from the ON_UPDATE() function of the session variable
        'tc_var_rules'.

  @param thd    [IN]        The thd handler.
  @param type   [IN]        Specifies whether it is global or session.

  @return
    true                    Success
    false                   Error
*/
bool Forwarding_rule_mgr::update_var_rules(THD *thd, enum_var_type type) {
  if(type != OPT_GLOBAL) {
    thd->forward_rule_mgr.reset_primary_var_rules();
    thd->forward_rule_mgr.cover_primary_var_rules(thd->forward_rule_mgr.m_session_var_rules_cache);
  } else {
    Forwarding_rule_mgr::m_global_var_rules_valid_cache = Forwarding_rule_mgr::m_global_var_rules_cache;
  }
  return true;
}

/**
  Initialize the sql-level and variable-level forwarding rules for the thread during session initialization.
*/
void Forwarding_rule_mgr::init() {
  reset_primary_rules();
  cover_primary_forwarding_rules(Forwarding_rule_mgr::m_global_rules_valid_cache);
  reset_primary_var_rules();
  cover_primary_var_rules(Forwarding_rule_mgr::m_global_var_rules_valid_cache);
}
