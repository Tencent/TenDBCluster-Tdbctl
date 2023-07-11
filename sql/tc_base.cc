/*
     Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

#include "sql_base.h"         // open_tables, open_and_lock_tables,
#include "sql_lex.h"
#include "sp_head.h"
#include "tc_base.h"
#include "sql_servers.h"
#include "mysql.h"
#include "sql_common.h"
#include "m_string.h"
#include "handler.h"
#include "log.h"
#include "rpl_group_replication.h"
#include <string.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <list>
#include <vector>
#include <sstream>
#include <regex>
#include <thread>
#include <mutex>
#include "rpl_slave.h"
#ifndef WIN32
#include <arpa/inet.h>
#else
#include <WinSock2.h>
#endif

#include <errmsg.h>

using namespace std;

#define SQL_SELECT_SERVER_UUID_STR "SELECT @@server_uuid"

/* For append_grant_privileges() */
extern const char *command_array[];
extern uint command_lengths[];

static PSI_memory_key key_memory_bases;

mutex remote_exec_mtx;
mutex spider_exec_mtx;

static void generate_remote_connect_info_by_idx(
    std::stringstream &ss, int server_idx, const std::string &db,
    const std::string &table, const std::string &server_prefix);

static string tc_dbname_replace_with_point(
  string sql, 
  string spider_db_name, 
  string remote_db_name
)
{
    string db_org1 = " " + spider_db_name + "\\.";
    string db_org2 = "`" + spider_db_name + "`\\.";
    string db_dst1 = " " + remote_db_name + ".";
    string db_dst2 = " `" + remote_db_name + "`.";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    sql = regex_replace(sql, pattern1, db_dst1);
    sql = regex_replace(sql, pattern2, db_dst2);
    return sql;
}

int tc_mysql_next_result(MYSQL *mysql)
{
    int status;
    if (!mysql || mysql->status != MYSQL_STATUS_READY)
    {
        return 1;
    }

    mysql->net.last_errno = 0;
    mysql->net.last_error[0] = '\0';
    my_stpcpy(mysql->net.sqlstate, "00000");
    mysql->affected_rows = ~(my_ulonglong)0;

    if (mysql->server_status & SERVER_MORE_RESULTS_EXISTS)
    {
        if ((status = mysql->methods->read_query_result(mysql)) > 0)
            return(mysql_errno(mysql));
        return status;
    }
    return -1;
}


const char* get_stmt_type_str(int type)
{
    switch (type)
    {
    case SQLCOM_SELECT: 
      return "SQLCOM_SELECT";
    case SQLCOM_CREATE_TABLE: 
      return "SQLCOM_CREATE_TABLE";
    case SQLCOM_CREATE_INDEX: 
      return "SQLCOM_CREATE_INDEX";
    case SQLCOM_ALTER_TABLE:
      return "SQLCOM_ALTER_TABLE";
    case SQLCOM_UPDATE: 
      return "SQLCOM_UPDATE";
    case SQLCOM_INSERT:
      return "SQLCOM_INSERT";
    case SQLCOM_INSERT_SELECT:
      return "SQLCOM_INSERT_SELECT";
    case SQLCOM_DELETE:
      return "SQLCOM_DELETE";
    case SQLCOM_TRUNCATE:
      return "SQLCOM_TRUNCATE";
    case SQLCOM_DROP_TABLE:
      return "SQLCOM_DROP_TABLE";
    case SQLCOM_DROP_INDEX:
      return "SQLCOM_DROP_INDEX";
    case SQLCOM_SHOW_DATABASES:
      return "SQLCOM_SHOW_DATABASES";
    case SQLCOM_SHOW_TABLES: 
      return "SQLCOM_SHOW_TABLES";
    case SQLCOM_SHOW_FIELDS:
      return "SQLCOM_SHOW_FIELDS";
    case SQLCOM_SHOW_KEYS: 
      return "SQLCOM_SHOW_KEYS";
    case SQLCOM_SHOW_VARIABLES: 
      return "SQLCOM_SHOW_VARIABLES";
    case SQLCOM_SHOW_STATUS:
      return "SQLCOM_SHOW_STATUS";
    case SQLCOM_SHOW_ENGINE_LOGS:
      return "SQLCOM_SHOW_ENGINE_LOGS";
    case SQLCOM_SHOW_ENGINE_STATUS:
      return "SQLCOM_SHOW_ENGINE_STATUS";
    case SQLCOM_SHOW_ENGINE_MUTEX: 
      return "SQLCOM_SHOW_ENGINE_MUTEX";
    case SQLCOM_SHOW_PROCESSLIST:
      return "SQLCOM_SHOW_PROCESSLIST";
    case SQLCOM_SHOW_MASTER_STAT:
      return "SQLCOM_SHOW_MASTER_STAT";
    case SQLCOM_SHOW_SLAVE_STAT:
      return "SQLCOM_SHOW_SLAVE_STAT";
    case SQLCOM_SHOW_GRANTS: 
      return "SQLCOM_SHOW_GRANTS";
    case SQLCOM_SHOW_CREATE:
      return "SQLCOM_SHOW_CREATE";
    case SQLCOM_SHOW_CHARSETS:
      return "SQLCOM_SHOW_CHARSETS";
    case SQLCOM_SHOW_COLLATIONS: 
      return "SQLCOM_SHOW_COLLATIONS";
    case SQLCOM_SHOW_CREATE_DB: 
      return "SQLCOM_SHOW_CREATE_DB";
    case SQLCOM_SHOW_TABLE_STATUS: 
      return "SQLCOM_SHOW_TABLE_STATUS";
    case SQLCOM_SHOW_TRIGGERS:
      return "SQLCOM_SHOW_TRIGGERS";
    case SQLCOM_LOAD:
      return "SQLCOM_LOAD";
    case SQLCOM_SET_OPTION: 
      return "SQLCOM_SET_OPTION";
    case SQLCOM_LOCK_TABLES:
      return "SQLCOM_LOCK_TABLES";
    case SQLCOM_UNLOCK_TABLES:
      return "SQLCOM_UNLOCK_TABLES";
    case SQLCOM_GRANT:
      return "SQLCOM_GRANT";
    case SQLCOM_CHANGE_DB: 
      return "SQLCOM_CHANGE_DB";
    case SQLCOM_CREATE_DB:
      return "SQLCOM_CREATE_DB";
    case SQLCOM_DROP_DB: 
      return "SQLCOM_DROP_DB";
    case SQLCOM_ALTER_DB: 
      return "SQLCOM_ALTER_DB";
    case SQLCOM_REPAIR: 
      return "SQLCOM_REPAIR";
    case SQLCOM_REPLACE:
      return "SQLCOM_REPLACE";
    case SQLCOM_REPLACE_SELECT:
      return "SQLCOM_REPLACE_SELECT";
    case SQLCOM_CREATE_FUNCTION:
      return "SQLCOM_CREATE_FUNCTION";
    case SQLCOM_DROP_FUNCTION:
      return "SQLCOM_DROP_FUNCTION";
    case SQLCOM_REVOKE: 
      return "SQLCOM_REVOKE";
    case SQLCOM_OPTIMIZE:
      return "SQLCOM_OPTIMIZE";
    case SQLCOM_CHECK: 
      return "SQLCOM_CHECK";
    case SQLCOM_ASSIGN_TO_KEYCACHE: 
      return "SQLCOM_ASSIGN_TO_KEYCACHE";
    case SQLCOM_PRELOAD_KEYS:
      return "SQLCOM_PRELOAD_KEYS";
    case SQLCOM_FLUSH: 
      return "SQLCOM_FLUSH";
    case SQLCOM_KILL: 
      return "SQLCOM_KILL";
    case SQLCOM_ANALYZE:
      return "SQLCOM_ANALYZE";
    case SQLCOM_ROLLBACK:
      return "SQLCOM_ROLLBACK";
    case SQLCOM_ROLLBACK_TO_SAVEPOINT: 
      return "SQLCOM_ROLLBACK_TO_SAVEPOINT";
    case SQLCOM_COMMIT: 
      return "SQLCOM_COMMIT";
    case SQLCOM_SAVEPOINT: 
      return "SQLCOM_SAVEPOINT";
    case SQLCOM_RELEASE_SAVEPOINT:
      return "SQLCOM_RELEASE_SAVEPOINT";
    case SQLCOM_SLAVE_START:
      return "SQLCOM_SLAVE_START";
    case SQLCOM_SLAVE_STOP:
      return "SQLCOM_SLAVE_STOP";
    case SQLCOM_BEGIN:
      return "SQLCOM_BEGIN";
    case SQLCOM_CHANGE_MASTER: 
      return "SQLCOM_CHANGE_MASTER";
    case SQLCOM_RENAME_TABLE:
      return "SQLCOM_RENAME_TABLE";
    case SQLCOM_RESET:
      return "SQLCOM_RESET";
    case SQLCOM_PURGE:
      return "SQLCOM_PURGE";
    case SQLCOM_PURGE_BEFORE:
      return "SQLCOM_PURGE_BEFORE";
    case SQLCOM_SHOW_BINLOGS:
      return "SQLCOM_SHOW_BINLOGS";
    case SQLCOM_SHOW_OPEN_TABLES:
      return "SQLCOM_SHOW_OPEN_TABLES";
    case SQLCOM_HA_OPEN: 
      return "SQLCOM_HA_OPEN";
    case SQLCOM_HA_CLOSE:
      return "SQLCOM_HA_CLOSE";
    case SQLCOM_HA_READ: 
      return "SQLCOM_HA_READ";
    case SQLCOM_SHOW_SLAVE_HOSTS: 
      return "SQLCOM_SHOW_SLAVE_HOSTS";
    case SQLCOM_DELETE_MULTI:
      return "SQLCOM_DELETE_MULTI";
    case SQLCOM_UPDATE_MULTI:
      return "SQLCOM_UPDATE_MULTI";
    case SQLCOM_SHOW_BINLOG_EVENTS:
      return "SQLCOM_SHOW_BINLOG_EVENTS";
    case SQLCOM_DO:
      return "SQLCOM_DO";
    case SQLCOM_SHOW_WARNS:
      return "SQLCOM_SHOW_WARNS";
    case SQLCOM_EMPTY_QUERY: 
      return "SQLCOM_EMPTY_QUERY";
    case SQLCOM_SHOW_ERRORS: 
      return "SQLCOM_SHOW_ERRORS";
    case SQLCOM_SHOW_STORAGE_ENGINES:
      return "SQLCOM_SHOW_STORAGE_ENGINES";
    case SQLCOM_SHOW_PRIVILEGES: 
      return "SQLCOM_SHOW_PRIVILEGES";
    case SQLCOM_HELP:
      return "SQLCOM_HELP";
    case SQLCOM_CREATE_USER:
      return "SQLCOM_CREATE_USER";
    case SQLCOM_DROP_USER: 
      return "SQLCOM_DROP_USER";
    case SQLCOM_RENAME_USER:
      return "SQLCOM_RENAME_USER";
    case SQLCOM_REVOKE_ALL:
      return "SQLCOM_REVOKE_ALL";
    case SQLCOM_CHECKSUM:
      return "SQLCOM_CHECKSUM";
    case SQLCOM_CREATE_PROCEDURE:
      return "SQLCOM_CREATE_PROCEDURE";
    case SQLCOM_CREATE_SPFUNCTION:
      return "SQLCOM_CREATE_SPFUNCTION";
    case SQLCOM_CALL: 
      return "SQLCOM_CALL";
    case SQLCOM_DROP_PROCEDURE:
      return "SQLCOM_DROP_PROCEDURE";
    case SQLCOM_ALTER_PROCEDURE:
      return "SQLCOM_ALTER_PROCEDURE";
    case SQLCOM_ALTER_FUNCTION:
      return "SQLCOM_ALTER_FUNCTION";
    case SQLCOM_SHOW_CREATE_PROC: 
      return "SQLCOM_SHOW_CREATE_PROC";
    case SQLCOM_SHOW_CREATE_FUNC:
      return "SQLCOM_SHOW_CREATE_FUNC";
    case SQLCOM_SHOW_STATUS_PROC:
      return "SQLCOM_SHOW_STATUS_PROC";
    case SQLCOM_SHOW_STATUS_FUNC:
      return "SQLCOM_SHOW_STATUS_FUNC";
    case SQLCOM_PREPARE: 
      return "SQLCOM_PREPARE";
    case SQLCOM_EXECUTE:
      return "SQLCOM_EXECUTE";
    case SQLCOM_DEALLOCATE_PREPARE:
      return "SQLCOM_DEALLOCATE_PREPARE";
    case SQLCOM_CREATE_VIEW: 
      return "SQLCOM_CREATE_VIEW";
    case SQLCOM_DROP_VIEW: 
      return "SQLCOM_DROP_VIEW";
    case SQLCOM_CREATE_TRIGGER:
      return "SQLCOM_CREATE_TRIGGER";
    case SQLCOM_DROP_TRIGGER:
      return "SQLCOM_DROP_TRIGGER";
    case SQLCOM_XA_START:
      return "SQLCOM_XA_START";
    case SQLCOM_XA_END:
      return "SQLCOM_XA_END";
    case SQLCOM_XA_PREPARE:
      return "SQLCOM_XA_PREPARE";
    case SQLCOM_XA_COMMIT:
      return "SQLCOM_XA_COMMIT";
    case SQLCOM_XA_ROLLBACK: 
      return "SQLCOM_XA_ROLLBACK";
    case SQLCOM_XA_RECOVER:
      return "SQLCOM_XA_RECOVER";
    case SQLCOM_SHOW_PROC_CODE: 
      return "SQLCOM_SHOW_PROC_CODE";
    case SQLCOM_SHOW_FUNC_CODE:
      return "SQLCOM_SHOW_FUNC_CODE";
    case SQLCOM_ALTER_TABLESPACE:
      return "SQLCOM_ALTER_TABLESPACE";
    case SQLCOM_INSTALL_PLUGIN:
      return "SQLCOM_INSTALL_PLUGIN";
    case SQLCOM_UNINSTALL_PLUGIN: 
      return "SQLCOM_UNINSTALL_PLUGIN";
    case SQLCOM_BINLOG_BASE64_EVENT:
      return "SQLCOM_BINLOG_BASE64_EVENT";
    case SQLCOM_SHOW_PLUGINS:
      return "SQLCOM_SHOW_PLUGINS";
    case SQLCOM_CREATE_SERVER:
      return "SQLCOM_CREATE_SERVER";
    case SQLCOM_DROP_SERVER:
      return "SQLCOM_DROP_SERVER";
    case SQLCOM_ALTER_SERVER:
      return "SQLCOM_ALTER_SERVER";
    case SQLCOM_CREATE_EVENT:
      return "SQLCOM_CREATE_EVENT";
    case SQLCOM_ALTER_EVENT:
      return "SQLCOM_ALTER_EVENT";
    case SQLCOM_DROP_EVENT:
      return "SQLCOM_DROP_EVENT";
    case SQLCOM_SHOW_CREATE_EVENT:
      return "SQLCOM_SHOW_CREATE_EVENT";
    case SQLCOM_SHOW_EVENTS:
      return "SQLCOM_SHOW_EVENTS";
    case SQLCOM_SHOW_CREATE_TRIGGER:
      return "SQLCOM_SHOW_CREATE_TRIGGER";
    case SQLCOM_ALTER_DB_UPGRADE: 
      return "SQLCOM_ALTER_DB_UPGRADE";
    case SQLCOM_SHOW_PROFILE:
      return "SQLCOM_SHOW_PROFILE";
    case SQLCOM_SHOW_PROFILES:
      return "SQLCOM_SHOW_PROFILES";
    case SQLCOM_SIGNAL:
      return "SQLCOM_SIGNAL";
    case SQLCOM_RESIGNAL:
      return "SQLCOM_RESIGNAL";
    case SQLCOM_SHOW_RELAYLOG_EVENTS:
      return "SQLCOM_SHOW_RELAYLOG_EVENTS";
    case SQLCOM_GET_DIAGNOSTICS:
      return "SQLCOM_GET_DIAGNOSTICS";
    case SQLCOM_SHUTDOWN:
      return "SQLCOM_SHUTDOWN";
    case SQLCOM_ALTER_USER:
      return "SQLCOM_ALTER_USER";
    case SQLCOM_SHOW_CREATE_USER: 
      return "SQLCOM_SHOW_CREATE_USER";
    case SQLCOM_ALTER_INSTANCE:
      return "SQLCOM_ALTER_INSTANCE";
    case SQLCOM_CHANGE_REPLICATION_FILTER:
      return "SQLCOM_CHANGE_REPLICATION_FILTER";
    case SQLCOM_CREATE_COMPRESSION_DICTIONARY:
      return "SQLCOM_CREATE_COMPRESSION_DICTIONARY";
    case SQLCOM_DROP_COMPRESSION_DICTIONARY:
      return "SQLCOM_DROP_COMPRESSION_DICTIONARY";
    case SQLCOM_EXPLAIN_OTHER:
      return "SQLCOM_EXPLAIN_OTHER";
    case SQLCOM_LOCK_BINLOG_FOR_BACKUP: 
      return "SQLCOM_LOCK_BINLOG_FOR_BACKUP";
    case SQLCOM_LOCK_TABLES_FOR_BACKUP:
      return "SQLCOM_LOCK_TABLES_FOR_BACKUP";
    case SQLCOM_SHOW_CLIENT_STATS:
      return "SQLCOM_SHOW_CLIENT_STATS";
    case SQLCOM_SHOW_INDEX_STATS: 
      return "SQLCOM_SHOW_INDEX_STATS";
    case SQLCOM_SHOW_TABLE_STATS:
      return "SQLCOM_SHOW_TABLE_STATS";
    case SQLCOM_SHOW_THREAD_STATS:
      return "SQLCOM_SHOW_THREAD_STATS";
    case SQLCOM_SHOW_USER_STATS:
      return "SQLCOM_SHOW_USER_STATS";
    case SQLCOM_START_GROUP_REPLICATION:
      return "SQLCOM_START_GROUP_REPLICATION";
    case SQLCOM_STOP_GROUP_REPLICATION: 
      return "SQLCOM_STOP_GROUP_REPLICATION";
    case SQLCOM_UNLOCK_BINLOG: return "SQLCOM_UNLOCK_BINLOG";
    case TC_SQLCOM_CREATE_TABLE_WITH_SELECT: 
      return "TC_SQLCOM_CREATE_TABLE_WITH_SELECT";
    case TC_SQLCOM_CREATE_TABLE_WITH_CONNECT_STRING: 
      return "TC_SQLCOM_CREATE_TABLE_WITH_CONNECT_STRING";
    case TC_SQLCOM_CREATE_TABLE_WITH_TABLE_COMMENT: 
      return "TC_SQLCOM_CREATE_TABLE_WITH_TABLE_COMMENT";
    case TC_SQLCOM_CREATE_TABLE_WITH_FIELD_CHARSET: 
      return "TC_SQLCOM_CREATE_TABLE_WITH_FIELD_CHARSET";
    case TC_SQLCOM_CREATE_TABLE_LIKE:
      return "TC_SQLCOM_CREATE_TABLE_LIKE";
    case TC_SQLCOM_CREATE_OR_DROP_UNIQUE_KEY: 
      return "TC_SQLCOM_CREATE_OR_DROP_UNIQUE_KEY";
    case TC_SQLCOM_ALTER_TABLE_UNSUPPORT: 
      return "TC_SQLCOM_ALTER_TABLE_UNSUPPORT";
    case TC_SQLCOM_CREATE_NODE:
      return "TC_SQLCOM_CREATE_NODE";
    case TC_SQLCOM_ALTER_NODE:
      return "TC_SQLCOM_ALTER_NODE";
    case TC_SQLCOM_DROP_NODE:
      return "TC_SQLCOM_DROP_NODE";
    case TC_SQLCOM_FLUSH_ROUTING:
      return "TC_SQLCOM_FLUSH_ROUTING";
    case TC_SQLCOM_MONITOR_INIT:
      return "TC_SQLCOM_MONITOR_INIT";
    case TC_SQLCOM_SHOW_PROCESSLIST:
      return "TC_SQLCOM_SHOW_PROCESSLIST";
    case TC_SQLCOM_SHOW_VARIABLES:
      return "TC_SQLCOM_SHOW_VARIABLES";
    case TC_SQLCOM_CONN_NODE_EXECUTE_SQL:
      return "TC_SQLCOM_CONN_NODE_EXECUTE_SQL";
    case TC_SQLCOM_ENABLE_PRIMARY:
      return "TC_SQLCOM_ENABLE_PRIMARY";
    case TC_SQLCOM_DISABLE_PRIMARY:
      return "TC_SQLCOM_DISABLE_PRIMARY";
    case TC_SQLCOM_GET_PRIMARY:
      return "TC_SQLCOM_GET_PRIMARY";
    default:
        return "unknown type";
    }
    return "";
}



void gettype_create_filed(Create_field *cr_field, String &res)
{
    const CHARSET_INFO *cs = res.charset();
    ulonglong field_length = cr_field->length;
	ulong length = 0;
    bool unsigned_flag = cr_field->flags & UNSIGNED_FLAG;
    bool zerofill_flag = cr_field->flags & ZEROFILL_FLAG;
    ulonglong tmp = field_length;
	const char *str = NULL;
    switch (cr_field->field->type())
    {
    case MYSQL_TYPE_DECIMAL:
        tmp = cr_field->length;
        if (!unsigned_flag)
            tmp--;
        if (cr_field->decimals)
            tmp--;
        res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "decimal(%lld,%d)", tmp, cr_field->decimals));
        filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
        break;
    case MYSQL_TYPE_TINY:
        res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "tinyint(%d)", (int)field_length));
        filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
        break;
    case MYSQL_TYPE_SHORT:
        res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "smallint(%d)", (int)field_length));
        filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
        break;
    case MYSQL_TYPE_INT24:
        res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "mediumint(%d)", (int)field_length));
        filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
        break;
    case MYSQL_TYPE_LONG:
        res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "int(%lld)", field_length));
        filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
        break;
    case MYSQL_TYPE_FLOAT:
        if (cr_field->decimals == NOT_FIXED_DEC)
        {
            res.set_ascii(STRING_WITH_LEN("float"));
        }
        else
        {
            const CHARSET_INFO *cs = res.charset();
            res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
                "float(%ld,%d)", cr_field->length, cr_field->decimals));
        }
        filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
        break;
    case MYSQL_TYPE_DOUBLE:
        if (cr_field->decimals == NOT_FIXED_DEC)
        {
            res.set_ascii(STRING_WITH_LEN("double"));
        }
        else
        {
            res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
                "double(%ld,%d)", cr_field->length, cr_field->decimals));
        }
        filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
        break;
    case MYSQL_TYPE_NULL:
        res.set_ascii(STRING_WITH_LEN("null"));
        break;
    case MYSQL_TYPE_TIMESTAMP:
        res.set_ascii(STRING_WITH_LEN("timestamp"));
        break;
    case MYSQL_TYPE_LONGLONG:
        res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "bigint(%d)", (int)field_length));
        filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);

    case MYSQL_TYPE_DATE:
        res.set_ascii(STRING_WITH_LEN("date"));
        break;
    case MYSQL_TYPE_TIME:
        res.set_ascii(STRING_WITH_LEN("time"));
        break;
    case MYSQL_TYPE_DATETIME:
        res.set_ascii(STRING_WITH_LEN("datetime"));
        break;
    case MYSQL_TYPE_YEAR:
        res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "year(%d)", (int)field_length));
        break;
    case MYSQL_TYPE_NEWDATE:
        res.set_ascii(STRING_WITH_LEN("date"));
        break;
    case MYSQL_TYPE_BIT:
        length = cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "bit(%d)", (int)field_length);
        res.length((uint)length);
        break;
    case MYSQL_TYPE_NEWDECIMAL:
        res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "decimal(%ld,%d)", cr_field->length 
             - (cr_field->decimals>0 ? 1 : 0) - 
            (unsigned_flag || !cr_field->length ? 0 : 1),
            cr_field->decimals));
        filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
        break;
        /*
        case MYSQL_TYPE_ENUM:
        char buffer[255];
        String enum_item(buffer, sizeof(buffer), res.charset());

        res.length(0);
        res.append(STRING_WITH_LEN("enum("));

        bool flag=0;
        uint *len= typelib->type_lengths;
        for (const char **pos= typelib->type_names; *pos; pos++, len++)
        {
        uint dummy_errors;
        if (flag)
        res.append(',');
        enum_item.copy(*pos, *len, charset(), res.charset(), &dummy_errors);
        append_unescaped(&res, enum_item.ptr(), enum_item.length());
        flag= 1;
        }
        res.append(')');
        break;
        case MYSQL_TYPE_SET:
        char buffer_tmp[255];
        String set_item(buffer_tmp, sizeof(buffer_tmp), res.charset());

        res.length(0);
        res.append(STRING_WITH_LEN("set("));

        bool flag=0;
        uint *len= typelib->type_lengths;
        for (const char **pos= typelib->type_names; *pos; pos++, len++)
        {
        uint dummy_errors;
        if (flag)
        res.append(',');
        set_item.copy(*pos, *len, charset(), res.charset(), &dummy_errors);
        append_unescaped(&res, set_item.ptr(), set_item.length());
        flag= 1;
        }
        res.append(')');
        break;
        case MYSQL_TYPE_GEOMETRY:
        CHARSET_INFO *cs= &my_charset_latin1;
        switch (geom_type)
        {
        case GEOM_POINT:
        res.set(STRING_WITH_LEN("point"), cs);
        break;
        case GEOM_LINESTRING:
        res.set(STRING_WITH_LEN("linestring"), cs);
        break;
        case GEOM_POLYGON:
        res.set(STRING_WITH_LEN("polygon"), cs);
        break;
        case GEOM_MULTIPOINT:
        res.set(STRING_WITH_LEN("multipoint"), cs);
        break;
        case GEOM_MULTILINESTRING:
        res.set(STRING_WITH_LEN("multilinestring"), cs);
        break;
        case GEOM_MULTIPOLYGON:
        res.set(STRING_WITH_LEN("multipolygon"), cs);
        break;
        case GEOM_GEOMETRYCOLLECTION:
        res.set(STRING_WITH_LEN("geometrycollection"), cs);
        break;
        default:
        res.set(STRING_WITH_LEN("geometry"), cs);
        }
        break;
        */

    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
        switch (cr_field->field->type())
        {
        case MYSQL_TYPE_TINY_BLOB:
            str = "tiny"; length = 4; break;
        case MYSQL_TYPE_BLOB:
            str = "";     length = 0; break;
        case MYSQL_TYPE_MEDIUM_BLOB:
            str = "medium"; length = 6; break;
        case MYSQL_TYPE_LONG_BLOB:
            str = "long";  length = 4; break;
        default:
            break;
        }
        res.set_ascii(str, length);
        if (cr_field->charset == &my_charset_bin)
            res.append(STRING_WITH_LEN("blob"));
        else
        {
            res.append(STRING_WITH_LEN("text"));
        }
        break;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:

        if (cr_field->charset)
        {
                length = cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(), "%s(%d)",
                (strcmp(cr_field->charset->csname, "binary") == 0 ? "varbinary" : "varchar"),
                (int)field_length);
        }
        else
        {
            length = cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(), "%s(%d)",
                ("varchar"), (int)field_length);

        }
        res.length(length);
        break;
    case MYSQL_TYPE_STRING:
        if (cr_field->charset)
        {
            length = cs->cset->snprintf(cs, (char*)res.ptr(),
                res.alloced_length(), "%s(%d)",
                (strcmp(cr_field->charset->csname, "binary") == 0 ? "binary" : "char"), (int)field_length);
        }
        else
        {
            length = cs->cset->snprintf(cs, (char*)res.ptr(),
                res.alloced_length(), "%s(%d)", "char", (int)field_length);
        }
        res.length(length);
        break;
    default:
        break;
    }
}


void filed_add_zerofill_and_unsigned(String &res, bool unsigned_flag, bool zerofill)
{
    if (unsigned_flag)
        res.append(STRING_WITH_LEN(" unsigned"));
    if (zerofill)
        res.append(STRING_WITH_LEN(" zerofill"));
}




int parse_get_shard_key_for_spider(
    const char*		table_comment,
    char*		key_buf,
    uint		key_len
)
{
    // comment=' shard_key "****"';
    const char* pos = strstr(table_comment, "shard_key");
    if (!pos)
        return 1;

    pos += strlen("shard_key");

    // ignore the space
    while (*pos == ' ' || *pos == '\t')
    {
        pos++;
    }

    // find the beginning "
    if (*pos == '"')
        pos++;
    else
        return 1;

    // find the ending "
    const char* end = strstr(pos, "\"");

    if (!end || (end - pos) <= 0)
        return 1;

    uint len = ((key_len - 1) > (uint)(end - pos)) ? (uint)(end - pos) : (key_len - 1);
    strncpy(key_buf, pos, len);
    key_buf[len] = '\0';

    return 0;
}

int parse_get_config_table_for_spider(
    const char*		table_comment,
    char*		key_buf,
    uint		key_len
)
{
    // comment=' config_table "****"';
    const char* pos = strstr(table_comment, "config_table");
    if (!pos)
        return 1;

    pos += strlen("config_table");

    // ignore the space
    while (*pos == ' ' || *pos == '\t')
    {
        pos++;
    }

    // find the beginning "
    if (*pos == '"')
        pos++;
    else
        return 1;

    // find the ending "
    const char* end = strstr(pos, "\"");

    if (!end || (end - pos) <= 0)
        return 1;

    uint len = ((key_len - 1) > (uint)(end - pos)) ? (uint)(end - pos) : (key_len - 1);
    strncpy(key_buf, pos, len);
    key_buf[len] = '\0';

    return 0;
}

// parse table comment
// get shard_count, shard_function, shard_type, etc.
int parse_get_spider_user_comment(
    const char*   table_comment,
    int*    shard_count,
    tspider_shard_func*    shard_func,
    tspider_shard_type*    shard_type
) {
    /* stage indicators */
    enum stage_indicator {TRIM = 0, PARSE_KEY = 1, PARSE_VALUE = 2, PARSE_DONE = 3};
    
    const int buf_len = 32;
    char keyword_buf[buf_len], value_buf[buf_len];
    const char* pos = table_comment;
    const char* begin = NULL;
    uint len = 0;
    int stage = 0;
    int ret = TCADMIN_PARSE_TABLE_COMMENT_OK;
    int get_key = 0, get_value = 0;
    
    /* example: shard_count "1", shard_method "crc32", shard_type "list" */
    while (pos && *pos != '\0') {
        switch (stage) {
        case TRIM:
            while (*pos != '\0' && (*pos == ' ' || *pos == '\t')) { pos++; }
            if ((*pos == '\0') || (get_key && get_value)) { stage = PARSE_DONE; }
            else if (get_key) { stage = PARSE_VALUE; }
            else { stage = PARSE_KEY; }
            break;
        case PARSE_KEY:
            begin = pos;
            while (*pos != '\0' && *pos != ' ' && *pos != '\t') { pos++; }
            if (*pos == '\0' || begin >= pos) {
                return TCADMIN_PARSE_TABLE_COMMENT_ERROR;
            }
            len = ((buf_len - 1) > (uint)(pos - begin)) ? (uint)(pos - begin) : (buf_len - 1);
            strncpy(keyword_buf, begin, len);
            keyword_buf[len] = '\0';
            /* validate keyword_buf */
            if (!tcadmin_validate_comment_keyword(keyword_buf)) { return TCADMIN_PARSE_TABLE_COMMENT_UNSUPPORTED; }
            get_key = 1;
            stage = TRIM;    
            break;
        case PARSE_VALUE:
            if (*pos != '"') { return TCADMIN_PARSE_TABLE_COMMENT_ERROR; }
            pos++;  /* skip beginning '"' */
            begin = pos;
            pos = strstr(begin, "\"");
            if (!pos || (pos - begin) <= 0) { return TCADMIN_PARSE_TABLE_COMMENT_ERROR; }
            len = ((buf_len - 1) > (uint)(pos - begin)) ? (uint)(pos - begin) : (buf_len - 1);
            strncpy(value_buf, begin, len);
            value_buf[len] = '\0';
            /* validate and fill value */
            ret = tcadmin_validate_and_fill_value(keyword_buf, value_buf, shard_count, shard_func, shard_type);
        	/* if ret != OK, return ret */
            if (ret != TCADMIN_PARSE_TABLE_COMMENT_OK) { goto parse_all_done; }
            pos++; /* skip ending '"' */
            get_value = 1;
            stage = TRIM;
            break;
        case PARSE_DONE:
            if (*pos == ',') { 
                /* parse next (key, value) pair */
                stage = TRIM; 
                get_key = 0;
                get_value = 0;
                pos++;
            } else {
                return TCADMIN_PARSE_TABLE_COMMENT_ERROR;
            }
            break;
        default:
            /* code should not reach here */
            break;
        }
    }
parse_all_done:
    return ret;
}

// return if tdbctl and tspider support this keyword
int tcadmin_validate_comment_keyword(const char* buf) {
    char shard_cnt[] = "shard_count";
    char shard_func[] = "shard_func";
    char shard_type[] = "shard_type";
    char shard_key[] = "shard_key";
    char config_table[] = "config_table";
    return (
        (!strcasecmp(buf, shard_cnt)) ||
        (!strcasecmp(buf, shard_func)) ||
        (!strcasecmp(buf, shard_type)) ||
        (!strcasecmp(buf, shard_key)) ||
        (!strcasecmp(buf, config_table))
    );
}

// validate the value for keyword
// currently, five keywords are allowed
// 1. shard_count, value should be 0 or 1 or # of remote DBs
// 2. shard_func, should be "crc32" or "crc32_ci" or "none"
// 3. shard_type, should be "list" or "range"
// 4. shard_key, don't process value here
// 5. config_table, let tspider to handle it
// If the keyword not in the above 5, return UNSUPPORTED
int tcadmin_validate_and_fill_value(
    const char* key_buf,
    const char* value_buf,
    int* shard_count,
    tspider_shard_func* shard_func,
    tspider_shard_type* shard_type
) {
    const char *str_shard_cnt = "shard_count";
    const char *str_shard_func = "shard_func";
    const char *str_shard_type = "shard_type";
    const char *str_shard_key = "shard_key";
    const char *str_config_table = "config_table";
    const char *str_crc32 = "crc32";
    const char *str_crc32_ci = "crc32_ci";
    const char *str_murmur_jump_hash = "murmur_jump_hash";
    const char *str_none = "none";
    const char *str_type_list = "list";
    const char *str_type_range = "range";
    int cnt;

    if (!strcasecmp(key_buf, str_shard_cnt)) {
        /* count, only 0, 1, original shar_count are valid */
        cnt = atoi(value_buf);
        /* since atoi fails also return 0, we need to manually check "0" */
        if ((strlen(value_buf) == 1 && value_buf[0] == '0')) {
            /* do nothing, leave shard_count as it was */
        } else {
            if (cnt == 0) { return TCADMIN_PARSE_TABLE_COMMENT_ERROR; }
            if (cnt == 1) {
                *shard_count = 1;
            } else if (cnt != *shard_count) {
                return TCADMIN_PARSE_SHARD_COUNT_INVALID;
            } /* else do nothing, leave shard_count as it was */
        }
    } else if (!strcasecmp(key_buf, str_shard_func)) {
        /* function, only 'crc32' and 'none' are supported */
        if (!strcasecmp(value_buf, str_crc32)) {
            *shard_func = tspider_shard_func_crc32;
        } else if (!strcasecmp(value_buf, str_crc32_ci)) {
            *shard_func = tspider_shard_func_crc32_ci;
        } else if (!strcasecmp(value_buf, str_none)) {
            *shard_func = tspider_shard_func_none;
        } else if (!strcasecmp(value_buf, str_murmur_jump_hash)) {
          *shard_func = tspider_shard_func_murmur_jump_hash;
        } else {
            return TCADMIN_PARSE_SHARD_FUNCTION_INVALID;
        }
    } else if (!strcasecmp(key_buf, str_shard_type)) {
        /* type, only 'list' and 'range' are supported */
        if (!strcasecmp(value_buf, str_type_list)) {
            *shard_type = tspider_shard_type_list;
        } else if (!strcasecmp(value_buf, str_type_range)) {
            *shard_type = tspider_shard_type_range;
        } else {
            return TCADMIN_PARSE_SHARD_TYPE_INVALID;
        }
    } else if (strcasecmp(key_buf, str_shard_key) && (strcasecmp(key_buf, str_config_table))) {
    	  /* if it is neither `shard_key` nor `config_table`, then the comment is not supported */
    	  /* if it is `shard_key`, process it later */
    	  /* if it is `config_table`, let tspider handle it */
        return TCADMIN_PARSE_TABLE_COMMENT_UNSUPPORTED;
    }
    return TCADMIN_PARSE_TABLE_COMMENT_OK;
}

// buf_len means length of key_name ... result, etc
//return val
//  TRUE : parse failed.
//  FALSE: parse success.
bool tc_parse_getkey_for_spider(THD *thd, char *key_name, char *result, int buf_len, bool *is_unique_key, bool *is_unsigned_key)
{
    LEX* lex = thd->lex;
    List_iterator<Create_field> it_field = lex->alter_info.create_list;
    Create_field *field;
    List_iterator<Key> key_iterator(lex->alter_info.key_list);
    Key *key;
    //const char *shard_key_str = "AS TSPIDER SHARD KEY";
    bool has_shard_key = false;
    Key_part_spec *column;
    int is_key_part = 0; 
    int level = 0;  
    /* 
    first part of the common key,level is 1;  
    first part of the unique key,level is 2; 
    first part of the primary key, level is 3 */
    *is_unique_key = FALSE; // do not have unique key

    strcpy(result, "SUCCESS");
    const char* table_comment = lex->create_info.comment.str;
    if (table_comment)
    {
        if (!parse_get_shard_key_for_spider(table_comment, key_name, buf_len))
        {
            has_shard_key = TRUE;

            List_iterator<Create_field> list_field = lex->alter_info.create_list;
            Create_field *field;

            bool field_exists = false;
            while ((field = list_field++))
            {
                if (!strcmp(field->field_name, key_name))
                {
                    field_exists = true;
                    break;
                }
            }
            if (!field_exists)
            {
                snprintf(result, buf_len, "ERROR: %s as TSpider key, but not exist", key_name);
                strcpy(key_name, "");
                return TRUE;
            }
        }
    }


    key_iterator.rewind();
    while ((key = key_iterator++))
    {
        List_iterator<Key_part_spec> cols(key->columns);
        column = cols++;

        switch (key->type)
        {
        case keytype::KEYTYPE_PRIMARY:
        case keytype::KEYTYPE_UNIQUE:
        {

            if (has_shard_key)
            {
				/*
				if exist shard_key,allow it is the common part of multi unique_key;
				if not the common part of multi unique_key,print error 
				*/
                int has_flag = 0;
                Key_part_spec *tmp_column;
                cols.rewind();
                while ((tmp_column = cols++))
                {
                    if (!strcmp(key_name, tmp_column->field_name.str))
                    {
                        has_flag = 1;
                        is_key_part = 1;
                    }
                }
                if (!has_flag)
                {
					/* if not a part of any unique_key*/
                    snprintf(result, buf_len, "ERROR: %s as TSpider key, but not in some unique key", key_name);
                    strcpy(key_name, "");
                    return TRUE;
                }
            }
            else
            {
                if (level > 1 && strcmp(key_name, column->field_name.str))
                {
					/*
					if prefix of  multi unique_key not the same,then print error
					*/
                    snprintf(result, buf_len, "%s", "ERROR: too more unique key with the different pre key");
                    strcpy(key_name, "");
                    return TRUE;
                }

                strcpy(key_name, column->field_name.str);
                level = ((key->type == keytype::KEYTYPE_PRIMARY) ? 3 : 2);
            }
            *is_unique_key = TRUE;
            break;
        }
        case keytype::KEYTYPE_MULTIPLE:
        {
            if (!has_shard_key && level < 1)
            {	
				/*
				if not specify shard key,and no  unique key,then get the first common key  as partition key
				*/
                strcpy(key_name, column->field_name.str);
                level = 1;
            }

            if (has_shard_key)
            {				
				/*
				if exist shard_key,whether is the part of common index
				*/
                Key_part_spec *tmp_column;
                cols.rewind();
                while ((tmp_column = cols++))
                {
                    if (!strcmp(key_name, tmp_column->field_name.str))
                        is_key_part = 1;
                }
            }

            break;
        }
        case keytype::KEYTYPE_FULLTEXT:
        {
          if (has_shard_key && !(strcmp(key_name, column->field_name.str))) {
            strcpy(key_name, "");
            snprintf(result, buf_len, "%s", "ERROR: fulltext key can't be shard key");
            return 1;
          }
          break;
        }
        case keytype::KEYTYPE_FOREIGN:
        case keytype::KEYTYPE_SPATIAL:
        default:
        {
            strcpy(key_name, "");
            snprintf(result, buf_len, "%s", "ERROR: no support key type");
            return TRUE;
        }
        }
    }

	/*
	if only exist multi common key,and not specify shard key , then print error
	*/
    if (!has_shard_key && level == 1 && lex->alter_info.key_list.elements > 1)
    {
        //strcpy(key_name, "");  // key_name is the first key
        snprintf(result, buf_len, "%s", "ERROR: too many key more than 1, but without unique key or set shard key");
        return TRUE;
    }

    if (has_shard_key && level <= 1 && is_key_part == 0)
    {
		/*
		specify shard key,no unique key,but shard_key is not a part of common index
		*/
        snprintf(result, buf_len, "%s", "ERROR: shard_key must be part of key");
        return TRUE;
    }

    it_field.rewind();
    while ((has_shard_key || level == 1 || level == 2) && !!(field = it_field++))
    {
		/*
		the column which specified as key must be not null.because of primary key default not null,so need consider it.
		flag stores the option information of create table sql
		*/
        uint flags = field->flags;
        if (!strcmp(field->field_name, key_name) && !(flags & NOT_NULL_FLAG))
        {
            snprintf(result, buf_len, "%s", "ERROR: the key must default not null");
            return TRUE;
        }
    }

	// get if key is a unsigned type
  List_iterator<Create_field> list_field = lex->alter_info.create_list;
  Create_field *tmp_field;

    while ((tmp_field = list_field++))
    {
        if (!strcmp(tmp_field->field_name, key_name))
        {
            *is_unsigned_key = tmp_field->flags & UNSIGNED_FLAG;
            break;
        }
    }
	  //specify shard_key or contains index
    if (has_shard_key || level > 0)
        return FALSE;

    strcpy(key_name, "");
    snprintf(result, buf_len, "%s", "ERROR: no key");
    return TRUE;
}


bool is_add_or_drop_unique_key(THD *thd, LEX *lex)
{
    List_iterator<Key> key_iterator(lex->alter_info.key_list);
    List_iterator<Alter_drop> it_drop_field = lex->alter_info.drop_list;
    Alter_drop *alter_drop_field;
    Key *key;
    ulonglong flags = lex->alter_info.flags;
    if (flags & Alter_info::ALTER_ADD_INDEX)
    {
        while ((key = key_iterator++))
        {
            if (key->type == keytype::KEYTYPE_PRIMARY || key->type == keytype::KEYTYPE_UNIQUE)
            {
                return true;
            }
        }
    }
    if (flags & Alter_info::ALTER_DROP_INDEX)
    {
        while ((alter_drop_field = it_drop_field++))
        {
            if (!strcmp(alter_drop_field->name, "PRIMARY"))
                return true;
        }
    }
    else
    {
        return false;
    }

    return false;
}


int tcadmin_execute(THD *thd)
{
    return 0;
}

int tcadmin_parse(THD *thd)
{
    return 0;
}



/***

my $convert_sql = $sql_result->{convert_sql}->[0];
my $sql_type = $sql_result->{sql_type}->[0];
my $key = $sql_result->{shard_key}->[0];
my $parse_result = $sql_result->{parse_result}->[0];
my $table_name = $sql_result->{table_name}->[0];
my $db_name = $sql_result->{db_name}->[0];
my $new_table_name = $sql_result->{new_table_name};
my $new_db_name = $sql_result->{new_db_name};
my $is_with_shard = $sql_result->{is_with_shard_key_comment};
my $is_with_autoincrement = $sql_result->{is_with_autoincrement};
****/

enum_sql_command tc_get_sql_type(THD *thd, LEX *lex)
{
    return lex->sql_command;
}

int tc_get_shard_key(THD *thd, LEX *lex, char *buf, uint len)
{
   if(lex->create_info.comment.str)
   {
       return parse_get_shard_key_for_spider(lex->create_info.comment.str, buf, sizeof(buf));
   }
   return 1;
}


const char* tc_get_cur_tbname(THD *thd, LEX *lex)
{
    /* ddl query always involved only one db_name and one table_name except create talbe .. like / trigger/rename table
    */
    TABLE_LIST* table_list = lex->query_tables;
    return (table_list->table_name);
}

const char* tc_get_cur_dbname(THD *thd, LEX *lex)
{
    TABLE_LIST* table_list = lex->query_tables;
    return (table_list->db);
}

const char* tc_get_new_tbname(THD *thd, LEX *lex)
{
    TABLE_LIST* table_list = lex->query_tables->next_local;
    return (table_list->table_name);
}

const char* tc_get_new_dbname(THD *thd, LEX *lex)
{
    TABLE_LIST* table_list = lex->query_tables->next_local;
    return (table_list->db);
}


bool tc_is_with_shard(THD *thd, LEX *lex)
{
    bool is_with_shard = FALSE;
    char buf[256];
    if (!lex->create_info.comment.str || parse_get_shard_key_for_spider(lex->create_info.comment.str, buf, sizeof(buf)))
    {/* no shard key*/
        is_with_shard = FALSE;
    }
    else
    {
        is_with_shard = TRUE;
    }
    return is_with_shard;
}

bool tc_is_with_autoincrement(THD *thd, LEX *lex)
{
    bool is_with_auto = FALSE;
    if (lex->sql_command == SQLCOM_CREATE_TABLE)
    {
        List_iterator<Create_field> it_field;
        Create_field *cur_field;
        it_field = lex->alter_info.create_list;
        while (!!(cur_field = it_field++))
        {
            if (cur_field->flags & AUTO_INCREMENT_FLAG)
            {/* with autoincrement */
                is_with_auto = TRUE;
            }
            break;
        }
    }
    return is_with_auto;
}


void tc_parse_result_init(TC_PARSE_RESULT *parse_result_t)
{
  parse_result_t->shard_func = tspider_shard_func_crc32;
  parse_result_t->shard_type = tspider_shard_type_list;
  parse_result_t->execute_flag = 0;
  parse_result_t->result_set_flag = 0;
}

void tc_parse_result_destory(TC_PARSE_RESULT *parse_result_t)
{
  parse_result_t->remote_sql_map.clear();
}


void tc_parse_remote_create_table(TC_PARSE_RESULT *tc_parse_result_t)
{
    map<string, string> map;
    ostringstream  sstr;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
    string create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string db_org1 = " " + db_name + "\\.";
    string db_org2 = "`" + db_name + "`\\.";
    regex pattern("ENGINE\\s*=\\s*spider", regex::icase);
    int shard_count = tc_parse_result_t->shard_count;
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    create_sql = regex_replace(create_sql, pattern, " ");
    for (int i = 0; i < shard_count; i++)
    {
        string remote_create_sql = create_sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string server = server_name_pre + hash_value;
        string db_dst1 = " " + remote_db + ".";
        string db_dst2 = " `" + remote_db + "`.";

        remote_create_sql = regex_replace(remote_create_sql, pattern1, db_dst1);
        remote_create_sql = regex_replace(remote_create_sql, pattern2, db_dst2);
        remote_create_sql = "use " + remote_db + ";" + remote_create_sql;
        tc_parse_result_t->remote_sql_map.insert(pair<string, string>(server, remote_create_sql));
    }
}

string tc_get_only_spider_ddl_withdb(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    sql = "use " + db_name + ";" + sql;
    return sql;
}

string tc_get_only_spider_ddl(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    return sql;
}

string tcadmin_get_shard_range_by_index(int index, int shard_count, bool is_unsigned) {
    string ret;
    ostringstream sstr;
    /* index should always be less than shard_count, at most equal than shard_count - 1*/
    if (index == shard_count - 1) 
    {
        ret = "MAXVALUE";
    }
    else 
    {
        if (is_unsigned)
        {   /* If shard_count == 1, it should not come here */
            unsigned int range = INT_MAX / shard_count;
            unsigned int ret_val = range * (index + 1);
            sstr << ret_val;
            ret = sstr.str();
        }
        else 
        {
            int zero_line = (shard_count - 1) / 2;
            int below_zero_cnt = zero_line + 1;
            int above_zero_cnt = shard_count - below_zero_cnt;
            int ret_val = 0;
            if (index == zero_line)
            {
                ret = "0";
            }            
            else if (index < zero_line)
            {
                ret_val = INT_MIN / below_zero_cnt * (below_zero_cnt - 1 - index);
                sstr << ret_val;
                ret = sstr.str();
            }
            else
            {
                ret_val = INT_MAX / above_zero_cnt * (index - zero_line);
                sstr << ret_val;
                ret = sstr.str();
            }
            
        }
    }
    return ret;
}

static void generate_remote_connect_info_by_idx(
    std::stringstream &ss, int server_idx, const std::string &db,
    const std::string &table, const std::string &server_prefix) {
  ss << "COMMENT = ";
  ss << "'"; /* start COMMENT */
  ss << "database " << TC_STR_DOUBLE_QUOTED(db + "_" + to_string(server_idx));
  ss << TC_STR_COMMA;
  ss << "table " << TC_STR_DOUBLE_QUOTED(table);
  ss << TC_STR_COMMA;
  ss << "server " << TC_STR_DOUBLE_QUOTED(server_prefix + to_string(server_idx));
  ss << "'"; /* end COMMENT */
}

void tc_parse_spider_create_table(TC_PARSE_RESULT *tc_parse_result_t,
                                  bool is_unsigned_key, size_t part_start) {
  stringstream sstr;
  string spider_create_sql;
  string orig_sql = string(tc_parse_result_t->query_string.str,
                           tc_parse_result_t->query_string.length);
  string server_name_pre = tdbctl_mysql_wrapper_prefix;
  string db_name = tc_parse_result_t->db_name;
  string tb_name = tc_parse_result_t->table_name;
  int shard_count = tc_parse_result_t->shard_count;
  tspider_shard_func shard_func = tc_parse_result_t->shard_func;
  tspider_shard_type shard_type = tc_parse_result_t->shard_type;
  string hash_key = tc_parse_result_t->shard_key;

  spider_create_sql += "USE " + db_name + TC_STR_DELIMITER;
  if (part_start) {
    /*
      PARTITION BY is present, remove it for Spider. Note that it is assumed
      (mostly the case) that the PARTITION clause is the last part of the
      query, so we simply do a substr().
    */
    spider_create_sql += orig_sql.substr(0, part_start);
  } else {
    spider_create_sql += orig_sql;
  }

  regex pattern1("ENGINE\\s*=\\s*MyISAM", regex::icase);
  regex pattern2("ENGINE\\s*=\\s*InnoDB", regex::icase);
  regex pattern3("ENGINE\\s*=\\s*tokudb", regex::icase);
  regex pattern4("ROW_FORMAT\\s*=\\s*GCS_DYNAMIC", regex::icase);
  regex pattern5("ROW_FORMAT\\s*=\\s*GCS", regex::icase);
  regex pattern6("ENGINE\\s*=\\s*heap", regex::icase);
  spider_create_sql =
      regex_replace(spider_create_sql, pattern1, "ENGINE = spider");
  spider_create_sql =
      regex_replace(spider_create_sql, pattern2, "ENGINE = spider");
  spider_create_sql =
      regex_replace(spider_create_sql, pattern3, "ENGINE = spider");
  spider_create_sql = regex_replace(spider_create_sql, pattern4, "");
  spider_create_sql = regex_replace(spider_create_sql, pattern5, "");
  spider_create_sql = regex_replace(spider_create_sql, pattern6, "");

  /* Assemble PARTITION BY part */
  sstr.str("");
  sstr << " PARTITION BY ";
  if (shard_type == tspider_shard_type_list) {
    sstr << "LIST";
  } else { /* tspider_shard_type_range */
    sstr << "RANGE";
  }

  sstr << "("; /* start shard function  */
  switch (shard_func) {
  case tspider_shard_func_crc32:
    is_unsigned_key = true;
    sstr << "CRC32"
         << "(" << TC_STR_IDENTIFIER(hash_key) << ")" << TC_STR_MOD
         << shard_count;
    break;

  case tspider_shard_func_crc32_ci:
    is_unsigned_key = true;
    sstr << "CRC32_CI"
         << "(" << TC_STR_IDENTIFIER(hash_key) << ")" << TC_STR_MOD
         << shard_count;
    break;

  case tspider_shard_func_murmur_jump_hash:
    is_unsigned_key = true;
    sstr << "MURMUR_JUMP_HASH"
         << "(" << TC_STR_IDENTIFIER(hash_key) << TC_STR_COMMA << shard_count
         << ")";
    break;

  case tspider_shard_func_none:
  default:
    sstr << TC_STR_IDENTIFIER(hash_key) << TC_STR_MOD << shard_count << ")";
    break;
  }
  sstr << ")";  /* end shard function */
  sstr << " ("; /* start PARTITION INFO */
  spider_create_sql += sstr.str();

  /* Assemble PARTITION INFO */
  for (int i = 0; i < shard_count; i++) {
    sstr.str("");
    sstr << "PARTITION pt" << i << " VALUES ";
    if (shard_type == tspider_shard_type_list) {
      sstr << "IN (" << i << ") ";
      generate_remote_connect_info_by_idx(sstr, i, db_name, tb_name,
                                          server_name_pre);
    } else { /* range */
      sstr << "LESS THAN ("
           << tcadmin_get_shard_range_by_index(i, shard_count, is_unsigned_key)
           << ") ";
      generate_remote_connect_info_by_idx(sstr, i, db_name, tb_name,
                                          server_name_pre);
    }
    sstr << " ENGINE = SPIDER";
    if (i < shard_count - 1) {
      sstr << TC_STR_COMMA;
    } else {
      sstr << ")"; /* end PARTITION INFO */
      sstr << TC_STR_DELIMITER; /* end query */
    }
    spider_create_sql += sstr.str();
  }
  sstr.clear();
  tc_parse_result_t->spider_sql = spider_create_sql;
}


void tc_parse_spider_drop_table(TC_PARSE_RESULT *tc_parse_result_t)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    sql = "use " + db_name + ";" + sql;
    tc_parse_result_t->spider_sql = sql;
}

void tc_parse_remote_drop_table(TC_PARSE_RESULT *tc_parse_result_t)
{
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;


    string db_org1 = " " + db_name + "\\.";
    string db_org2 = "`" + db_name + "`\\.";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < tc_parse_result_t->shard_count; i++)
    {
        string remote_sql = sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string server = server_name_pre + hash_value;
        string db_dst1 = " " + remote_db + ".";
        string db_dst2 = " `" + remote_db + "`.";

        remote_sql = regex_replace(remote_sql, pattern1, db_dst1);
        remote_sql = regex_replace(remote_sql, pattern2, db_dst2);
        remote_sql = "use " + remote_db + ";" + remote_sql;
        tc_parse_result_t->remote_sql_map.insert(pair<string, string>(server, remote_sql));
    }
}

void tc_parse_remote_create_database(TC_PARSE_RESULT *tc_parse_result_t)
{
    ostringstream  sstr;
    string create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    string db_org1 = " " + db_name;
    string db_org2 = "`" + db_name + "`";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < tc_parse_result_t->shard_count; i++)
    {
        string remote_create_sql = create_sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string server = server_name_pre + hash_value;
        string db_dst1 = " " + remote_db;
        string db_dst2 = " `" + remote_db + "`";

        remote_create_sql = regex_replace(remote_create_sql, pattern1, db_dst1);
        remote_create_sql = regex_replace(remote_create_sql, pattern2, db_dst2);
        tc_parse_result_t->remote_sql_map.insert(pair<string, string>(server, remote_create_sql));
    }
}

void tc_parse_remote_drop_database(TC_PARSE_RESULT *tc_parse_result_t)
{
    ostringstream  sstr;
    string create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    string db_org1 = " " + db_name;
    string db_org2 = "`" + db_name + "`";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < tc_parse_result_t->shard_count; i++)
    {
        string remote_create_sql = create_sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string server = server_name_pre + hash_value;
        string db_dst1 = " " + remote_db;
        string db_dst2 = " `" + remote_db + "`";

        remote_create_sql = regex_replace(remote_create_sql, pattern1, db_dst1);
        remote_create_sql = regex_replace(remote_create_sql, pattern2, db_dst2);
        tc_parse_result_t->remote_sql_map.insert(pair<string, string>(server, remote_create_sql));
    }
}

void tc_parse_remote_change_database(TC_PARSE_RESULT *tc_parse_result_t)
{
    ostringstream  sstr;
    string create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    string db_org1 = " " + db_name;
    string db_org2 = "`" + db_name + "`";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < tc_parse_result_t->shard_count; i++)
    {
        string remote_create_sql = create_sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string server = server_name_pre + hash_value;
        string db_dst1 = " " + remote_db;
        string db_dst2 = " `" + remote_db + "`";

        remote_create_sql = regex_replace(remote_create_sql, pattern1, db_dst1);
        remote_create_sql = regex_replace(remote_create_sql, pattern2, db_dst2);
        tc_parse_result_t->remote_sql_map.insert(pair<string, string>(server, remote_create_sql));
    }
}


void tc_parse_spider_create_or_drop_index(TC_PARSE_RESULT *tc_parse_result_t)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    sql = "use " + db_name + ";" + sql;
    tc_parse_result_t->spider_sql = sql;
}


void tc_parse_remote_create_or_drop_index(TC_PARSE_RESULT *tc_parse_result_t)
{
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    string db_org1 = " " + db_name + "\\.";
    string db_org2 = "`" + db_name + "`\\.";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < tc_parse_result_t->shard_count; i++)
    {
        string remote_sql = sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string server = server_name_pre + hash_value;
        string db_dst1 = " " + remote_db + ".";
        string db_dst2 = " `" + remote_db + "`.";

        remote_sql = regex_replace(remote_sql, pattern1, db_dst1);
        remote_sql = regex_replace(remote_sql, pattern2, db_dst2);
        remote_sql = "use " + remote_db + ";" + remote_sql;
        tc_parse_result_t->remote_sql_map.insert(pair<string, string>(server, remote_sql));
    }
}


void tc_parse_spider_alter_table(TC_PARSE_RESULT *tc_parse_result_t)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    regex pattern1("ENGINE\\s*=\\s*MyISAM", regex::icase);
    regex pattern2("ENGINE\\s*=\\s*InnoDB", regex::icase);
    regex pattern3("ENGINE\\s*=\\s*tokudb", regex::icase);
    regex pattern4("ROW_FORMAT\\s*=\\s*GCS_DYNAMIC", regex::icase);
    regex pattern5("ROW_FORMAT\\s*=\\s*GCS", regex::icase);
    sql = regex_replace(sql, pattern1, "ENGINE = spider");
    sql = regex_replace(sql, pattern2, "ENGINE = spider");
    sql = regex_replace(sql, pattern3, "ENGINE = spider");
    sql = regex_replace(sql, pattern4, "");
    sql = regex_replace(sql, pattern5, "");
    sql = "use " + db_name + ";" + sql;
    tc_parse_result_t->spider_sql = sql;
}


void tc_parse_remote_alter_table(TC_PARSE_RESULT *tc_parse_result_t)
{
    map<string, string> map;
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    for (int i = 0; i < tc_parse_result_t->shard_count; i++)
    {
        string remote_sql = sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string server = server_name_pre + hash_value;

        remote_sql = tc_dbname_replace_with_point(remote_sql, db_name, remote_db);
        remote_sql = "use " + remote_db + ";" + remote_sql;
        tc_parse_result_t->remote_sql_map.insert(pair<string, string>(server, remote_sql));
    }
}


void tc_parse_spider_rename_table(TC_PARSE_RESULT *tc_parse_result_t)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string table_name = tc_parse_result_t->table_name;
    string new_db = tc_parse_result_t->new_db_name;
    string new_table = tc_parse_result_t->new_table_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
    int shard_count = tc_parse_result_t->shard_count;
    ostringstream  sstr;
    string reorganize_partition_sql = "";
    string  partition_sql = "";
    sql = sql + "; alter table " + new_db + "." + new_table + " reorganize partition ";

    for (int i = 0; i < shard_count; i++)
    {
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string server_info;
        string pt_sql;
        string server_name = server_name_pre + hash_value;
        server_info = "server \"" + server_name + "\"";
        pt_sql = "PARTITION pt" + hash_value + " values in (" + hash_value + ") COMMENT = 'database \""
            + new_db + "_" + hash_value + "\", table \"" + new_table + "\", " + server_info + "\' ENGINE = SPIDER";

        if (i < shard_count - 1)
        {
            pt_sql = pt_sql + ",";
            reorganize_partition_sql = reorganize_partition_sql + "pt" + hash_value + ",";
        }
        else
        {
            pt_sql = pt_sql + ");";
            reorganize_partition_sql = reorganize_partition_sql + "pt" + hash_value + " into(";
        }
        partition_sql = partition_sql + pt_sql;
    }
    sstr.clear();

    sql = "use " + db_name + ";" + sql + reorganize_partition_sql + partition_sql;
    tc_parse_result_t->spider_sql = sql;
}


void tc_parse_remote_rename_table(TC_PARSE_RESULT *tc_parse_result_t)
{
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string new_db = tc_parse_result_t->new_db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    for (int i = 0; i < tc_parse_result_t->shard_count; i++)
    {
        string remote_sql = sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string new_remote_db = new_db + "_" + hash_value;
        string server = server_name_pre + hash_value;

        remote_sql = tc_dbname_replace_with_point(remote_sql, db_name, remote_db);
        remote_sql = tc_dbname_replace_with_point(remote_sql, new_db, new_remote_db);
        remote_sql = "use " + remote_db + ";" + remote_sql;
        tc_parse_result_t->remote_sql_map.insert(pair<string, string>(server, remote_sql));
    }
}


void tc_parse_spider_create_table_like(
  TC_PARSE_RESULT *tc_parse_result_t
)
{
    string sql(tc_parse_result_t->query_string.str, 
      tc_parse_result_t->query_string.length);
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
    string db_name = tc_parse_result_t->db_name;
    string table_name = tc_parse_result_t->table_name;
    string new_db = tc_parse_result_t->new_db_name;
    string new_table = tc_parse_result_t->new_table_name;
    ostringstream  sstr;
    string reorganize_partition_sql = "";
    string  partition_sql = "";
    sql = sql + "; alter table " + db_name + "." + table_name 
      + " reorganize partition ";

    for (int i = 0; i < tc_parse_result_t->shard_count; i++)
    {
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string server_info;
        string pt_sql;
        string server_name = server_name_pre + hash_value;
        server_info = "server \"" + server_name + "\"";
        pt_sql = "PARTITION pt" + hash_value + 
          " values in (" + hash_value + ") COMMENT = 'database \""
            + db_name + "_" + hash_value 
            + "\", table \"" + table_name + "\", " 
            + server_info + "\' ENGINE = SPIDER";

        if (i < tc_parse_result_t->shard_count - 1)
        {
            reorganize_partition_sql = 
              reorganize_partition_sql + "pt" + hash_value + ",";
            pt_sql = pt_sql + ",";
        }
        else
        {
            pt_sql = pt_sql + ");";
            reorganize_partition_sql = 
              reorganize_partition_sql + "pt" + hash_value + " into(";
        }
        partition_sql = partition_sql + pt_sql;
    }
    sstr.clear();

    sql = "use " + db_name + ";" + sql + 
      reorganize_partition_sql + partition_sql;
    tc_parse_result_t->spider_sql = sql;
}

void tc_parse_remote_create_table_like(
  TC_PARSE_RESULT *tc_parse_result_t
)
{
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, 
      tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string new_db = tc_parse_result_t->new_db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    for (int i = 0; i < tc_parse_result_t->shard_count; i++)
    {
        string remote_sql = sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string new_remote_db = new_db + "_" + hash_value;
        string server = server_name_pre + hash_value;

        remote_sql = tc_dbname_replace_with_point(remote_sql, db_name, remote_db);
        remote_sql = tc_dbname_replace_with_point(remote_sql, new_db, new_remote_db);
        remote_sql = "use " + remote_db + ";" + remote_sql;
        tc_parse_result_t->remote_sql_map.insert(pair<string, string>(server, remote_sql));
    }
}



bool tc_query_parse(THD *thd, LEX *lex, TC_PARSE_RESULT *tc_parse_result_t)
{
    return TRUE;
}

/*
   convert client Query to internal spider, remote node execute query
   @retval
      true:  convert success
      false: convert error
 */
bool tc_command_convert(THD *thd, LEX *lex, TC_PARSE_RESULT *tc_parse_result_t)
{
  bool command_support = true;
  /* The secondary_node_allowed indicates that whether the sql_cmd is allowed to execute
     on the tdbctl secondary node */
  bool secondary_node_allowed = true;

  set_var_base *var;
  int tdbctl_var_num = 0;
  int total_var_num = 0;
  List<set_var_base> *lex_var_list = &lex->var_list;
  List_iterator_fast<set_var_base> var_it(*lex_var_list);

  switch (lex->sql_command)
  {
    // Whether it is a tcadmin master or a slave node,
    // these commands are executed on tdbctl itself
    case SQLCOM_SHOW_VARIABLES:
    case SQLCOM_SHOW_EVENTS:
    case SQLCOM_SHOW_STATUS:
    case SQLCOM_SHOW_STATUS_PROC:
    case SQLCOM_SHOW_STATUS_FUNC:
    case SQLCOM_SHOW_DATABASES:
    case SQLCOM_SHOW_TABLES:
    case SQLCOM_SHOW_TRIGGERS:
    case SQLCOM_SHOW_FIELDS:
    case SQLCOM_SHOW_KEYS:
    case SQLCOM_SHOW_WARNS:
    case SQLCOM_SHOW_ERRORS:
    case SQLCOM_SHOW_BINLOGS:
    case SQLCOM_SHOW_CREATE:
    case SQLCOM_SHOW_PROCESSLIST:
    case SQLCOM_SHOW_CREATE_DB:
    case SQLCOM_SHOW_PRIVILEGES:
    case SQLCOM_SHOW_CREATE_USER:
    case SQLCOM_SHOW_GRANTS:
    case SQLCOM_SHOW_PROC_CODE:
    case SQLCOM_SHOW_FUNC_CODE:
    case SQLCOM_SHOW_CREATE_PROC:
    case SQLCOM_SHOW_CREATE_FUNC:
    case SQLCOM_SHOW_CREATE_TRIGGER:
    case SQLCOM_SHOW_SLAVE_HOSTS:
    case SQLCOM_HELP:
    case SQLCOM_SELECT:
    case SQLCOM_PURGE:
    case SQLCOM_PURGE_BEFORE:
    case SQLCOM_SHOW_PROFILES:
    case SQLCOM_BINLOG_BASE64_EVENT:
    case SQLCOM_CHANGE_REPLICATION_FILTER:
    case SQLCOM_LOCK_BINLOG_FOR_BACKUP:
    case SQLCOM_LOCK_TABLES_FOR_BACKUP:
    case SQLCOM_START_GROUP_REPLICATION:
    case SQLCOM_STOP_GROUP_REPLICATION:
    case SQLCOM_UNLOCK_BINLOG:
    case SQLCOM_FLUSH:
    case SQLCOM_KILL:
    case SQLCOM_SHUTDOWN:
    case SQLCOM_RELEASE_SAVEPOINT:
    case SQLCOM_ROLLBACK_TO_SAVEPOINT:
    case SQLCOM_SAVEPOINT:
    case SQLCOM_ALTER_DB_UPGRADE:
    case SQLCOM_CHANGE_MASTER:
    case SQLCOM_SHOW_BINLOG_EVENTS:
    case SQLCOM_SHOW_CREATE_EVENT:
    case SQLCOM_SHOW_MASTER_STAT:
    case SQLCOM_SHOW_RELAYLOG_EVENTS:
    case SQLCOM_SHOW_SLAVE_STAT:
    case SQLCOM_SLAVE_START:
    case SQLCOM_SLAVE_STOP:
    case SQLCOM_INSTALL_PLUGIN:
    case SQLCOM_UNINSTALL_PLUGIN:
    case SQLCOM_RESET:
      tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE;
      break;
    // These commands are not supported in tcadmin primary or secondary mode.
    case SQLCOM_SHOW_PLUGINS:
    case SQLCOM_SHOW_PROFILE:
    case SQLCOM_SHOW_ENGINE_STATUS:
    case SQLCOM_SHOW_ENGINE_MUTEX:
    case SQLCOM_SHOW_ENGINE_LOGS:
    case SQLCOM_EMPTY_QUERY:
    case SQLCOM_ASSIGN_TO_KEYCACHE:
    case SQLCOM_PRELOAD_KEYS:
    case SQLCOM_CHECKSUM:
    case SQLCOM_LOAD:
    case SQLCOM_XA_START:
    case SQLCOM_XA_END:
    case SQLCOM_XA_PREPARE:
    case SQLCOM_XA_COMMIT:
    case SQLCOM_XA_ROLLBACK:
    case SQLCOM_XA_RECOVER:
    case SQLCOM_ALTER_TABLESPACE:
    case SQLCOM_ANALYZE:
    case SQLCOM_CHECK:
    case SQLCOM_OPTIMIZE:
    case SQLCOM_REPAIR:
    case SQLCOM_SIGNAL:
    case SQLCOM_RESIGNAL:
    case SQLCOM_GET_DIAGNOSTICS:
    case SQLCOM_HA_OPEN:
    case SQLCOM_HA_CLOSE:
    case SQLCOM_HA_READ:
    case SQLCOM_ALTER_INSTANCE:
    case SQLCOM_CREATE_COMPRESSION_DICTIONARY:
    case SQLCOM_DROP_COMPRESSION_DICTIONARY:
    case SQLCOM_EXPLAIN_OTHER:
    case SQLCOM_CREATE_SERVER:
    case SQLCOM_ALTER_SERVER:
    case SQLCOM_DROP_SERVER:
    case SQLCOM_SHOW_CLIENT_STATS:
    case SQLCOM_SHOW_INDEX_STATS:
    case SQLCOM_SHOW_TABLE_STATS:
    case SQLCOM_SHOW_THREAD_STATS:
    case SQLCOM_SHOW_USER_STATS:
      command_support = false;
      /*push_warning_printf(thd, Sql_condition::SL_WARNING, ER_TCADMIN_UNSUPPORT_SQL_TYPE,
                   ER(ER_TCADMIN_UNSUPPORT_SQL_TYPE), get_stmt_type_str(lex->sql_command));*/
      if (!thd->is_error())
        my_error(ER_TCADMIN_UNSUPPORT_SQL_TYPE, MYF(0), get_stmt_type_str(lex->sql_command));
      break;
    // These commands are executed on only one spider node in tcadmin primary mode, 
    // and are executed on tdbctl itself in tcadmin secondary mode.
    case SQLCOM_SHOW_TABLE_STATUS:
    case SQLCOM_SHOW_OPEN_TABLES:
    case SQLCOM_SHOW_CHARSETS:
    case SQLCOM_SHOW_COLLATIONS:
    case SQLCOM_SHOW_STORAGE_ENGINES:
      if (tdbctl_is_primary)
      {
        tc_parse_result_t->spider_sql = std::string(thd->query().str, thd->query().length);
        tc_parse_result_t->execute_flag |= TC_ONLY_ONE_SPIDER_NEED_EXECUTE;
        tc_parse_result_t->result_set_flag |= RETURN_RESULT_SET_FROM_ONE_NODE;
      }
      else
        tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE;
      break;
    // These commands are executed on only one spider node in tcadmin primary mode, 
    // and not allowed to be executed in tcadmin secondary mode.
    case SQLCOM_PREPARE:
    case SQLCOM_EXECUTE:
    case SQLCOM_DEALLOCATE_PREPARE:
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
    case SQLCOM_TRUNCATE:
    case SQLCOM_CALL:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
       if (thd->db().str)
        tc_parse_result_t->db_name = thd->db().str;
      else
        tc_parse_result_t->db_name = tc_get_cur_dbname(thd, lex);
      tc_parse_result_t->spider_sql = "use " + tc_parse_result_t->db_name + ";" + std::string(thd->query().str, thd->query().length);
      tc_parse_result_t->execute_flag |= TC_ONLY_ONE_SPIDER_NEED_EXECUTE;
      break;
    // These commands are executed on only one spider node and tdbctl itself in tcadmin primary mode,
    // and not allowed to be executed in tcadmin secondary mode.
    case SQLCOM_BEGIN:
    case SQLCOM_COMMIT:
    case SQLCOM_ROLLBACK:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->spider_sql = std::string(thd->query().str, thd->query().length);
      tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE | TC_ONLY_ONE_SPIDER_NEED_EXECUTE;
      break;
    case SQLCOM_SET_OPTION:
      // if the sys_var is tdbctl var, we only execute it on tdbctl itself
      while ((var = var_it++))
      {
        if(var->check_tdbctl_var())
          tdbctl_var_num++;
        total_var_num++;
      }
      if (tdbctl_var_num > 0 && tdbctl_var_num == total_var_num)
      {
        tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE;
        break;
      }
      else if (tdbctl_var_num > 0 && tdbctl_var_num != total_var_num)
      {
        my_error(ER_TCADMIN_EXECUTE_ERROR, MYF(0), "can't set tdbctl-only var and common var at the same time");
        return FALSE;
      }

      if (tdbctl_is_primary)
      {
        tc_parse_result_t->spider_sql = std::string(thd->query().str, thd->query().length);
        tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE | TC_ONLY_ONE_SPIDER_NEED_EXECUTE;
      }
      else
        tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE;
      break;
    case SQLCOM_UNLOCK_TABLES:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->spider_sql = std::string(thd->query().str, thd->query().length);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
      break;
    case SQLCOM_LOCK_TABLES:
    case SQLCOM_CREATE_EVENT:
    case SQLCOM_ALTER_EVENT:
    case SQLCOM_DROP_EVENT:
    case SQLCOM_CREATE_FUNCTION:                  // UDF function
    case SQLCOM_CREATE_PROCEDURE:
    case SQLCOM_CREATE_SPFUNCTION:
    case SQLCOM_ALTER_PROCEDURE:
    case SQLCOM_ALTER_FUNCTION:
    case SQLCOM_DROP_PROCEDURE:
    case SQLCOM_DROP_FUNCTION:
    case SQLCOM_CREATE_TRIGGER:
    case SQLCOM_DROP_TRIGGER:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      if (thd->db().str)
        tc_parse_result_t->db_name = thd->db().str;
      else
        tc_parse_result_t->db_name = tc_get_cur_dbname(thd, lex);
      tc_parse_result_t->spider_sql = "use " + tc_parse_result_t->db_name + ";" + std::string(thd->query().str, thd->query().length);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
      break;
    case SQLCOM_CREATE_VIEW:
    case SQLCOM_DROP_VIEW:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      if (thd->db().str)
        tc_parse_result_t->db_name = thd->db().str;
      else
        tc_parse_result_t->db_name = tc_get_cur_dbname(thd, lex);
      tc_parse_result_t->spider_sql = "use " + tc_parse_result_t->db_name + ";" + std::string(thd->query().str, thd->query().length);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
      break;
    case SQLCOM_CREATE_USER:
    case SQLCOM_DROP_USER:
    case SQLCOM_ALTER_USER:
    case SQLCOM_RENAME_USER:
    case SQLCOM_REVOKE:
    case SQLCOM_GRANT:
    case SQLCOM_DO:
    case SQLCOM_REVOKE_ALL:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->spider_sql = std::string(thd->query().str, thd->query().length);
      tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE;
      break;
    case SQLCOM_CREATE_TABLE:
    {
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      //unsigned type
      bool is_unsigned_key = false;
      bool with_unique = false;
      //bool with_auto = false;
      List_iterator<Create_field> it_field;
      Create_field* cur_field;
      const char* tb_charset = NULL;
      bool create_table_with_field_charset = false;
      char key_name[256];
      char result_info[256];

      tc_parse_result_t->query_string = thd->processed_query();
      tc_parse_result_t->db_name = tc_get_cur_dbname(thd, lex);
      tc_parse_result_t->table_name = tc_get_cur_tbname(thd, lex);
      if (tc_parse_getkey_for_spider(thd, key_name, result_info, sizeof(result_info), &with_unique, &is_unsigned_key))
      {
        my_error(ER_TCADMIN_CREATE_TABLE, MYF(0), result_info);
        return FALSE;
      }
      tc_parse_result_t->shard_key = key_name;

      // tb_charset as table charset
      if (lex->create_info.default_table_charset)
        tb_charset = lex->create_info.default_table_charset->csname;
      else
        tb_charset = thd->charset()->csname;
      it_field = lex->alter_info.create_list;
      while (!!(cur_field = it_field++))
      {// column charset must be same with table
        switch (cur_field->sql_type)
        {
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_SET:
          if (cur_field->charset)
          {
            if (strcmp(cur_field->charset->csname, tb_charset) && strcmp(cur_field->charset->csname, "binary"))
            {// column have different charset
              create_table_with_field_charset = true;
            }
          }
        default:
          break;
        }
      }

      // handle user table comment (shard_count, shard_func, shard_type, etc.)
      if (lex->create_info.comment.str)
      {
        int ret = parse_get_spider_user_comment(
          lex->create_info.comment.str,
          &tc_parse_result_t->shard_count,
          &tc_parse_result_t->shard_func,
          &tc_parse_result_t->shard_type
        );

        if (ret != TCADMIN_PARSE_TABLE_COMMENT_OK)
        {
          switch (ret) {
          case TCADMIN_PARSE_TABLE_COMMENT_UNSUPPORTED:
            my_error(ER_TCADMIN_CREATE_TABLE, MYF(0), "ERROR: UNSUPPORT SQL CREATE TABLE WITH TABLE COMMENT");
            return FALSE;
          case TCADMIN_PARSE_SHARD_COUNT_INVALID:
            my_error(ER_TCADMIN_CREATE_TABLE, MYF(0), "ERROR: SQL CREATE TABLE WITH INVALID SHARD COUNT COMMENT");
            return FALSE;
          case TCADMIN_PARSE_SHARD_FUNCTION_INVALID:
            my_error(ER_TCADMIN_CREATE_TABLE, MYF(0), "ERROR: SQL CREATE TABLE WITH INVALID SHARD FUNCTION COMMENT");
            return FALSE;
          case TCADMIN_PARSE_SHARD_TYPE_INVALID:
            my_error(ER_TCADMIN_CREATE_TABLE, MYF(0), "ERROR: SQL CREATE TABLE WITH INVALID SHARD TYPE COMMENT");
            return FALSE;
          case TCADMIN_PARSE_TABLE_COMMENT_ERROR:
          default:
            /* handle TCADMIN_PARSE_TABLE_COMMENT_ERROR here */
            my_error(ER_TCADMIN_CREATE_TABLE, MYF(0), "ERROR: SQL CREATE TABLE WITH ERROR TABLE COMMENT");
            return FALSE;
          }
        }
      }

      if (lex->create_info.options & HA_LEX_CREATE_TABLE_LIKE)
      {
        tc_parse_result_t->new_db_name = tc_get_new_dbname(thd, lex);
        tc_parse_result_t->new_table_name = tc_get_new_tbname(thd, lex);
        break;
      }
      else if (lex->select_lex && lex->select_lex->item_list.elements > 0)
      {// create table select
        my_error(ER_TCADMIN_CREATE_TABLE, MYF(0), "ERROR: UNSUPPORT SQL CREATE TABLE WITH SELECT");
        return FALSE;
      }
      else if (lex->create_info.connect_string.str)
      {// create table with connect string
        my_error(ER_TCADMIN_CREATE_TABLE, MYF(0), "ERROR: UNSUPPORT SQL CREATE TABLE WITH TABLE CONNECT STRING");
        return FALSE;
      }
      else if (create_table_with_field_charset)
      {// table with other filed charset
        my_error(ER_TCADMIN_CREATE_TABLE, MYF(0), "ERROR: UNSUPPORT SQL CREATE TABLE WITH FIELD_CHARSET");
        return FALSE;
      }

      //parse spider sql
      tc_parse_spider_create_table(tc_parse_result_t, is_unsigned_key,
                                   lex->partition_start_pos);
      tc_parse_remote_create_table(tc_parse_result_t);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE|TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;

      break;
    }

    case SQLCOM_CREATE_INDEX:
    case SQLCOM_DROP_INDEX:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->query_string = thd->query();
      tc_parse_result_t->db_name = tc_get_cur_dbname(thd, lex);
      tc_parse_result_t->table_name = tc_get_cur_tbname(thd, lex);
      tc_parse_spider_create_or_drop_index(tc_parse_result_t);
      tc_parse_remote_create_or_drop_index(tc_parse_result_t);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE|TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
      break;
    case SQLCOM_ALTER_TABLE:
    {
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->query_string = thd->query();
      if (lex->alter_info.flags == Alter_info::ALTER_DROP_COLUMN)
        tc_parse_result_t->execute_flag |= TC_SPIDER_EXECUTE_FIRST;
      if (lex->alter_info.flags == Alter_info::ALTER_RENAME)
      {
        tc_parse_result_t->db_name = tc_get_cur_dbname(thd, lex);
        tc_parse_result_t->table_name = tc_get_cur_tbname(thd, lex);
        tc_parse_result_t->new_db_name = lex->select_lex->db;
        tc_parse_result_t->new_table_name = lex->name.str;
      }
      else if (lex->alter_info.flags == Alter_info::ADD_FOREIGN_KEY ||
        lex->alter_info.flags == Alter_info::DROP_FOREIGN_KEY)
      {
        my_error(ER_TCADMIN_ALTER_TABLE, MYF(0), "command not support");
        return FALSE;
      }
      else {
        tc_parse_result_t->db_name = tc_get_cur_dbname(thd, lex);
        tc_parse_result_t->table_name = tc_get_cur_tbname(thd, lex);
      }
      if (!lex->alter_info.has_alter_partitions()) {
        /* Only non-partitioning operations are allowed to be sent to Spider */
        tc_parse_spider_alter_table(tc_parse_result_t);
        tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE;
      } else if (lex->alter_info.has_non_alter_partitions()) {
        /*
          Do not allow a single ALTER query to have both partitioning and
          non-partitioning operations, because the former could not be sent to
          Spider nodes while the latter possibly could.
        */
        my_error(ER_TCADMIN_ALTER_TABLE, MYF(0),
                 "combination of both partitioning and non-partitioning "
                 "operations is not allowed in a single query");
        return FALSE;
      }
      tc_parse_remote_alter_table(tc_parse_result_t);
      tc_parse_result_t->execute_flag |= TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
      break;
    }
    case SQLCOM_RENAME_TABLE:
    {
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->query_string = thd->query();
      tc_parse_result_t->db_name = tc_get_cur_dbname(thd, lex);
      tc_parse_result_t->table_name = tc_get_cur_tbname(thd, lex);
      if (lex->query_tables->next_global)
      {
        tc_parse_result_t->new_db_name = lex->query_tables->next_global->db;
        tc_parse_result_t->new_table_name = lex->query_tables->next_global->table_name;
      }
      else
      {
        my_error(ER_TCADMIN_ALTER_TABLE, MYF(0), "Invalid RENAME TABLE statement");
        return FALSE;
      }
      tc_parse_spider_rename_table(tc_parse_result_t);
      tc_parse_remote_rename_table(tc_parse_result_t);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE|TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
      break;
    }
    case SQLCOM_DROP_TABLE:
    {
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->query_string = thd->query();
      tc_parse_result_t->db_name = tc_get_cur_dbname(thd, lex);
      tc_parse_result_t->table_name = tc_get_cur_tbname(thd, lex);
      tc_parse_spider_drop_table(tc_parse_result_t);
      tc_parse_remote_drop_table(tc_parse_result_t);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE | TC_REMOTE_NEED_EXECUTE | TC_SPIDER_EXECUTE_FIRST | TC_TDBCTL_NEED_EXECUTE;
      break;
    }
    case SQLCOM_CHANGE_DB:
      tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE;
      //do nothing
      break;
    case SQLCOM_CREATE_DB:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->query_string = thd->query();
      tc_parse_result_t->db_name = lex->name.str;
      tc_parse_result_t->spider_sql = std::string(thd->query().str, thd->query().length);
      tc_parse_remote_create_database(tc_parse_result_t);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE |TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
      break;
    case SQLCOM_DROP_DB:
    case SQLCOM_ALTER_DB:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->query_string = thd->query();
      tc_parse_result_t->db_name = lex->name.str;
      tc_parse_result_t->spider_sql = std::string(thd->query().str, thd->query().length);
      tc_parse_remote_drop_database(tc_parse_result_t);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE |TC_REMOTE_NEED_EXECUTE | TC_SPIDER_EXECUTE_FIRST | TC_TDBCTL_NEED_EXECUTE;
      break;
    case TC_SQLCOM_CREATE_TABLE_LIKE:
    {
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->query_string = thd->query();
      tc_parse_spider_create_table_like(tc_parse_result_t);
      tc_parse_remote_create_table_like(tc_parse_result_t);
      tc_parse_result_t->execute_flag |= TC_SPIDER_NEED_EXECUTE |TC_REMOTE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
      break;
    }
    case TC_SQLCOM_CREATE_NODE:
    case TC_SQLCOM_ALTER_NODE:
    case TC_SQLCOM_DROP_NODE:
      tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE;
      break;
    case TC_SQLCOM_FLUSH_ROUTING:
    case TC_SQLCOM_CREATE_SERVER:
    case TC_SQLCOM_DROP_SERVER:
    case TC_SQLCOM_ALTER_SERVER:
    case TC_SQLCOM_CREATE_TABLE_WITH_SELECT:
    case TC_SQLCOM_CREATE_TABLE_WITH_CONNECT_STRING:
    case TC_SQLCOM_CREATE_TABLE_WITH_TABLE_COMMENT:
    case TC_SQLCOM_CREATE_TABLE_WITH_FIELD_CHARSET:
    case TC_SQLCOM_CREATE_OR_DROP_UNIQUE_KEY:
    case TC_SQLCOM_ALTER_TABLE_UNSUPPORT:
    case TC_SQLCOM_MONITOR_INIT:
    case TC_SQLCOM_SHOW_PROCESSLIST:
    case TC_SQLCOM_SHOW_VARIABLES:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
    /*
      fallthrough:
      ENABLE/DISABLE/GET PRIMARY commands should be allowed to be executed on
      secondary nodes.
    */
    case TC_SQLCOM_ENABLE_PRIMARY:
    case TC_SQLCOM_DISABLE_PRIMARY:
    case TC_SQLCOM_GET_PRIMARY:
      tc_parse_result_t->execute_flag |= TC_TDBCTL_NEED_EXECUTE;
      break;
    case TC_SQLCOM_CONN_NODE_EXECUTE_SQL:
      secondary_node_allowed = false;
      if (!tdbctl_is_primary)
        break;
      tc_parse_result_t->execute_flag |= TC_DESIGNATED_NODE_NEED_EXECUTE | TC_TDBCTL_NEED_EXECUTE;
      tc_parse_result_t->result_set_flag |= RETURN_RESULT_SET_FROM_ONE_NODE;
      break;
    default:
      my_error(ER_TCADMIN_UNSUPPORT_SQL_TYPE, MYF(0), get_stmt_type_str(lex->sql_command));
      command_support = false;
  }
  if (!command_support)
    return FALSE;
  // if the sql_command is not allowed to be executed on secondary node, we will return error.
  if (!tdbctl_is_primary && !secondary_node_allowed)
  {
    my_error(ER_TCADMIN_NOT_PRIMARY, MYF(0), get_stmt_type_str(lex->sql_command));
    return FALSE;
  }
  return TRUE;
}

int tc_store_mysql_result_into_protocol(THD *thd, MYSQL_RES *res)
{
  DBUG_ENTER("tc_store_mysql_result_into_protocol");
  List<Item> field_list;
  Protocol *protocol= thd->get_protocol();
  MYSQL_ROW row;

  if (!res)
  {
    my_ok(thd);
    DBUG_RETURN(0);
  }

  uint field_num = mysql_num_fields(res);
  for (uint i = 0; i < field_num; i++)
  {
    field_list.push_back(tc_make_item(&res->fields[i]));
  }
  if (thd->send_result_metadata(&field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(1);

  while ((row = mysql_fetch_row(res)))
  {
    protocol->start_row();
    for (uint idx = 0; idx < field_num; ++idx)
    {
      protocol_store_field(protocol, res->fields[idx], row[idx], mysql_fetch_lengths(res)[idx]);
    }
    if(protocol->end_row())
      break;
  }

  my_eof(thd);
  DBUG_RETURN(0);
}

void protocol_store_field(Protocol *protocol, MYSQL_FIELD &field, const char *row, ulong length)
{
	DBUG_ENTER("protocol_store_field");
	if (row == NULL) {
		protocol->store_null();
		DBUG_VOID_RETURN;
	}

	switch (field.type) {
  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_ENUM:
  case MYSQL_TYPE_SET:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_BIT:
  case MYSQL_TYPE_NEWDECIMAL:
  case MYSQL_TYPE_JSON:
  {
    protocol->store(row, length, get_charset(field.charsetnr, MYF(MY_WME)));
    break;
  }
  case MYSQL_TYPE_TINY:
  {
    protocol->store_tiny(atoll(row));
    break;
  }
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_YEAR:
  {
    protocol->store_short(atoll(row));
    break;
  }
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_LONG:
  {
    protocol->store_long(atoll(row));
    break;
  }
  case MYSQL_TYPE_LONGLONG:
  {
    protocol->store_longlong(atoll(row), (field.flags & MY_I_S_UNSIGNED));
    break;
  }
  case MYSQL_TYPE_FLOAT:
  {
    //protocol->store((float)atof(row), field.length%10, buffer);
    protocol->store(row, length, get_charset(field.charsetnr, MYF(MY_WME)));
    break;
  }
  case MYSQL_TYPE_DOUBLE:
  {
    //protocol->store(atof(row), field.length%10, buffer);
    protocol->store(row, length, get_charset(field.charsetnr, MYF(MY_WME)));
    break;
  }
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIMESTAMP:
  {
    protocol->store(row, length, get_charset(field.charsetnr, MYF(MY_WME)));
    break;
  }
  case MYSQL_TYPE_TIME:
  {
    //protocol->store_time(&tm, decimals);
    protocol->store(row, length, get_charset(field.charsetnr, MYF(MY_WME)));
    break;
  }
	default:
		protocol->store(row, get_charset(field.charsetnr, MYF(MY_WME)));
		break;
	}

	DBUG_VOID_RETURN;
}

Item* tc_make_item(MYSQL_FIELD* field)
{
  Item* item;
  switch (field->type)
  {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_INT24:
    {
      item = new Item_return_int(field->name, field->length, field->type);
      item->unsigned_flag = (field->flags & MY_I_S_UNSIGNED);
      break;
    }
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
    {
      const Name_string field_name(field->name, field->length);
      item = new Item_temporal(field->type, field_name, 0, 0);

      if (field->type == MYSQL_TYPE_TIMESTAMP ||
          field->type == MYSQL_TYPE_DATETIME)
        item->decimals= field->length;
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    {
      const Name_string field_name(field->name, field->length);
      item = new Item_float(field_name, 0.0, NOT_FIXED_DEC, field->length);
      break;
    }
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
    {
      item = new Item_decimal((longlong)0, false);
      item->unsigned_flag = (field->flags & MY_I_S_UNSIGNED);
      item->decimals = field->length%10;
      item->max_length = (field->length/100)%100;
      if (item->unsigned_flag == 0)
        item->max_length+= 1;
      if (item->decimals > 0)
        item->max_length+= 1;
      item->item_name.copy(field->name);
      break;
    }
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    {
      item = new Item_blob(field->name, field->length);
      break;
    }
    case MYSQL_TYPE_STRING:
    default:
    {
      item = new Item_empty_string(field->name, field->length, system_charset_info);
      break;
    }
  }
  return item;
}

void tc_clean_exec_result(TC_EXEC_RESULT* exec_result)
{
  DBUG_ENTER("tc_clean_exec_result");
  for (int i = ENUM_NODE_TYPE_BEGIN; i < ENUM_NODE_TYPE_COUNT_EXCLUDE_TDBCTL; i++)
  {
    for (map<std::string, tc_exec_info>::iterator iter = exec_result->result_info[i].begin(); iter != exec_result->result_info[i].end(); iter++)
    {
      tc_exec_info &exec_info = iter->second;
      if (exec_info.res)
      {
        mysql_free_result(exec_info.res);
      }
    }
  }
  DBUG_VOID_RETURN;
}

void tc_real_query(Query_exec_manager *query_mgr, const string &server_name,
                   MYSQL *mysql, enum_node_type node_type) {
  DBUG_ENTER("tc_real_query");
  DBUG_PRINT("info", ("server_name: %s", server_name.c_str()));
  int err = 0;
  string query;
  tc_exec_info exec_info;

  if ((err = query_mgr->get_real_query(server_name, query, node_type))) {
    DBUG_ASSERT(0);
    query = "";
  }
  exec_info.err_code = 0;
  exec_info.err_msg = "";

  // If we dont prepare sql statements for some instances(spider or remote node),
  // we will skip querying to these instances.
  if (query != string()) {
    err = mysql_real_query(mysql, query.c_str(), query.length());
    while (!err) {
      err = tc_mysql_next_result(mysql);
    }

    if (err != -1) {
      exec_info.err_code = mysql_errno(mysql);
      exec_info.err_msg = mysql_error(mysql);
    }
  }
  query_mgr->store_exec_info(server_name, exec_info, node_type);

  DBUG_VOID_RETURN;
}

void tc_get_query_result(Query_exec_manager *query_mgr, const string &server_name,
                   MYSQL *mysql, enum_node_type node_type) {
  DBUG_ENTER("tc_get_query_result");
  DBUG_PRINT("info", ("server_name: %s", server_name.c_str()));
  int err = 0;
  string query;
  tc_exec_info exec_info;

  if ((err = query_mgr->get_real_query(server_name, query, node_type)))
  {
    DBUG_ASSERT(0);
    query = "";
  }
  exec_info.err_code = 0;
  exec_info.err_msg = "";
  exec_info.prepare_sql = false;
  exec_info.res = NULL;
  // If we dont prepare sql statements for some instances(spider or remote node),
  // we will skip querying to these instances.
  if (query != string())
  {
    err = mysql_real_query(mysql, query.c_str(), query.length());
    exec_info.prepare_sql = true;
    exec_info.res = mysql_store_result(mysql);

    // scan all query results from mysql, and store the last query result in to exec_info.res
    while (!err)
    {
      if (exec_info.res)
      {
        mysql_free_result(exec_info.res);
      }
      exec_info.res = mysql_store_result(mysql);
      err = tc_mysql_next_result(mysql);
    }

    if (err != -1) {
      exec_info.err_code = mysql_errno(mysql);
      exec_info.err_msg = mysql_error(mysql);
    }
  }
  query_mgr->store_exec_info(server_name, exec_info, node_type);

  DBUG_VOID_RETURN;
}


bool tc_exec_query_paral(Query_exec_manager *query_mgr,
                      const std::map<std::string, MYSQL *> &conns,
                      enum_node_type node_type) {
  uint i, server_cnt = conns.size();
  std::vector<std::thread> threads(server_cnt);
  map<string, MYSQL *>::const_iterator it;

  for (i = 0, it = conns.begin(); it != conns.end(); ++it, ++i) {
    thread t(tc_get_query_result, query_mgr, it->first, it->second, node_type);
    threads[i] = move(t);
  }
  for (i = 0; i < server_cnt; ++i) {
    if (threads[i].joinable()) threads[i].join();
  }
  return query_mgr->get_error();
}

bool tc_run_command(THD *thd, Cluster_conn_manager *conn_mgr,
                Query_exec_manager *query_mgr) {
  bool force = thd->variables.tc_force_execute;
  int exec_flag = query_mgr->get_exec_flag();

  //tc_exec_query_paral() will skip sending sql to the node if no sql statement was prepared for the node
  if (exec_flag & TC_SPIDER_EXECUTE_FIRST)
  {
    if ((!tc_exec_query_paral(query_mgr, conn_mgr->get_spider_conn_map(),
                              NODE_TYPE_SPIDER) &&
         !tc_exec_query_paral(query_mgr, conn_mgr->get_conn_map(NODE_TYPE_SPIDER_SLAVE),
                              NODE_TYPE_SPIDER_SLAVE)) ||
        force)
    {
      return tc_exec_query_paral(query_mgr, conn_mgr->get_remote_conn_map(),
                                 NODE_TYPE_REMOTE) ||
             tc_exec_query_paral(query_mgr, conn_mgr->get_conn_map(NODE_TYPE_REMOTE_SLAVE),
                                 NODE_TYPE_REMOTE_SLAVE);
    }
  }
  else
  {
    if ((!tc_exec_query_paral(query_mgr, conn_mgr->get_remote_conn_map(),
                              NODE_TYPE_REMOTE) &&
         tc_exec_query_paral(query_mgr, conn_mgr->get_conn_map(NODE_TYPE_REMOTE_SLAVE),
                             NODE_TYPE_REMOTE_SLAVE)) ||
        force)
    {
      return tc_exec_query_paral(query_mgr, conn_mgr->get_spider_conn_map(),
                                 NODE_TYPE_SPIDER) ||
             tc_exec_query_paral(query_mgr, conn_mgr->get_conn_map(NODE_TYPE_SPIDER_SLAVE),
                                 NODE_TYPE_SPIDER_SLAVE);
    }
  }

  return FALSE;
}

void fill_lex_to_alter_node(LEX* lex, FOREIGN_SERVER *server)
{
  DBUG_ENTER("fill_lex_to_alter_node");
  if (!lex->server_options.get_host())
  {
    lex->server_options.set_host({ server->host, strlen(server->host) });
  }
  if (lex->server_options.get_port() == Server_options::PORT_NOT_SET)
  {
    lex->server_options.set_port(server->port);
  }
  if (!lex->server_options.get_username())
  {
    lex->server_options.set_username({server->username, strlen(server->username)});
  }
  if (!lex->server_options.get_password())
  {
    lex->server_options.set_password({server->password, strlen(server->password)});
  }
  DBUG_VOID_RETURN;
}

Server_options foreign_server_to_server_options(FOREIGN_SERVER *fs)
{
  Server_options so;
  so.m_server_name = {fs->server_name, fs->server_name_length};

  so.set_port(fs->port);
  if (fs->host) {
    so.set_host({fs->host, strlen(fs->host)});
  }
  if (fs->db) {
    so.set_db({fs->db, strlen(fs->db)});
  }
  if (fs->username) {
    so.set_username({fs->username, strlen(fs->username)});
  }
  if (fs->password) {
    so.set_password({fs->password, strlen(fs->password)});
  }
  if (fs->scheme) {
    so.set_scheme({fs->scheme, strlen(fs->scheme)});
  }
  if (fs->socket) {
    so.set_socket({fs->socket, strlen(fs->socket)});
  }
  if (fs->owner) {
    so.set_owner({fs->owner, strlen(fs->owner)});
  }

  return so;
}

set<string> get_spider_ipport_set(
  MEM_ROOT *mem, 
  map<string, string> &spider_user_map, 
  map<string, string> &spider_passwd_map,
  bool with_slave,
  string wrapper_name
)
{
    set<string> ipport_set;
    FOREIGN_SERVER *server;
    ostringstream  sstr;
    list<FOREIGN_SERVER*> server_list;
    //string wrapper_name = tdbctl_spider_wrapper_prefix;
    spider_user_map.clear();
    spider_passwd_map.clear();

    get_server_by_wrapper(server_list, mem, wrapper_name.c_str(), with_slave);

    list<FOREIGN_SERVER*>::iterator its;
    for(its = server_list.begin(); its != server_list.end(); its++)
    {
        server = *its;
        string host = server->host;
        string user = server->username;
        string passwd = server->password;
        sstr.str("");
        sstr << server->port;
        string ports = sstr.str();
        string s = host + "#" + ports;
        ipport_set.insert(s);
        spider_user_map.insert(pair<string, string>(s, user));
        spider_passwd_map.insert(pair<string, string>(s, passwd));
    }

    return ipport_set;
}


map<string, string> get_remote_ipport_map(
  MEM_ROOT* mem, 
  map<string, string> &remote_user_map, 
  map<string, string> &remote_passwd_map,
  bool with_slave /* = false (default value)*/
)
{
    map<string, string> ipport_map;
    FOREIGN_SERVER *server, server_buffer;
    ostringstream  sstr;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
    string server_slave_name_pre = tdbctl_mysql_slave_wrapper_prefix;
    ulong records = get_servers_count();
    remote_user_map.clear();
    remote_passwd_map.clear();

    for (ulong i = 0; i < records; i++)
    {
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string server_name = server_name_pre + hash_value;
        if ((server = get_server_by_name(mem, server_name.c_str(), &server_buffer)))
        {
            string host = server->host;
            string user = server->username;
            string passwd = server->password;
            sstr.str("");
            sstr << server->port;
            string ports = sstr.str();
            string s = host + "#" + ports;
            ipport_map.insert(pair<string, string>(server_name, s));
            remote_user_map.insert(pair<string, string>(s, user));
            remote_passwd_map.insert(pair<string, string>(s, passwd));
        }

        if (with_slave)
        {
          string server_slave_name = server_slave_name_pre + hash_value;
          if ((server = get_server_by_name(mem, server_slave_name.c_str(), &server_buffer)))
          {
            string host = server->host;
            string user = server->username;
            string passwd = server->password;
            sstr.str("");
            sstr << server->port;
            string ports = sstr.str();
            string s = host + "#" + ports;
            ipport_map.insert(pair<string, string>(server_slave_name, s));
            remote_user_map.insert(pair<string, string>(s, user));
            remote_passwd_map.insert(pair<string, string>(s, passwd));
          }
        }
    }
    return ipport_map;
}

/*
  get map for ipport->server_name

  @retval
  map for result
  key:   ipport
  value: server_name
*/
map<string, string> get_server_name_map(
	MEM_ROOT *mem,
	const char* wrapper,
	bool with_slave
)
{
  map<string, string> server_name_map;
  ostringstream  sstr;
  list<FOREIGN_SERVER*> server_list;

  get_server_by_wrapper(server_list, mem, wrapper, with_slave);
  for (auto &server : server_list)
  {
    string host = server->host;
    sstr.str("");
    sstr << server->port;
    string ports = sstr.str();
    string s = host + "#" + ports;
    string server_name = server->server_name;
    server_name_map.insert(pair<string, string>(s, server_name));
  }

  return server_name_map;
}

/*
  get server_name with specifc wrapper

*/
void get_server_name_set(
	MEM_ROOT *mem,
  std::set<std::string> &server_set,
	const char* wrapper
)
{
  DBUG_ENTER("get_server_name_set");
  list<FOREIGN_SERVER*> server_list;
  std::string tmp;
  get_server_by_wrapper(server_list, mem, wrapper, false);
  for (auto &server : server_list)
  {
    string server_name = server->server_name;
    string user = server->username;
    string passwd = server->password;
    server_set.insert(server_name);
    tmp += server_name + " ";
  }

  DBUG_VOID_RETURN;
}

/*
  get map for server_uuid->server_name

  @retval
  map for result
  key:   server_uuid
  value: server_name
*/
map<string, string> get_server_uuid_map(
	int &ret,
	MEM_ROOT *mem,
	const char* wrapper,
	bool with_slave
)
{
	ret = 0;
	MYSQL_RES* res;
	MYSQL_ROW row = NULL;
	map<string, string> server_uuid_map;
	FOREIGN_SERVER *server;
	list<FOREIGN_SERVER*> server_list;
	get_server_by_wrapper(server_list, mem, wrapper, with_slave);
	list<FOREIGN_SERVER*>::iterator its;
	MYSQL *conn = NULL;
	string sql = "show variables like  'server_uuid'";
	string uuid;
	for (its = server_list.begin(); its != server_list.end(); its++)
	{
		server = *its;
		string host = server->host;
		long port = server->port;
		string server_name = server->server_name;
		string user = server->username;
		string passwd = server->password;
		string address = host + "#" + to_string(port);
		conn = tc_conn_connect(address, user, passwd);
		if (conn == NULL) {
			ret = 1;
			my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), address.c_str());
			goto finish;
		}
		MYSQL_GUARD(conn);
		res = tc_exec_sql_with_result(conn, sql);
		//use to free result.
		MYSQL_RES_GUARD(res);
		if (res && (row = mysql_fetch_row(res)))
		{
			uuid = row[1];
		}
		else
		{
			ret = 1;
			goto finish;
		}
		server_uuid_map.insert(pair<string, string>(uuid, server_name));
	}
finish:
	return server_uuid_map;
}


/*
  get server_name of current TDBCTL node

  @param (out)
  ret: 0 for ok, 1 for error

  @retval
  server_name  of current TDBCTL node
*/
string tc_get_server_name(
	int &ret,
	MEM_ROOT *mem,
	const char* wrapper,
	bool with_slave) 
{
	ret = 0;
	string server_name;
	map<string, string> tdbctl_server_uuid_map = get_server_uuid_map(ret, mem, wrapper, with_slave);
	if (ret)
	{
		goto finish;
	}
	server_name = tdbctl_server_uuid_map[server_uuid];
	if (server_name.size() == 0) 
	{
		ret = 1;
		goto finish;
	}
	return server_name;
finish:
	tdbctl_server_uuid_map.clear();
	return server_name;
}


/*
  get username of current TDBCTL node

  @param (out)
  ret: 0 for ok, 1 for error

  @retval
  username  of current TDBCTL node
*/
string tc_get_user_name(
	int &ret,
	const char* wrapper,
	bool with_slave)
{
	ret = 0;
	string username;
	MEM_ROOT mem_root;
	init_sql_alloc(key_memory_bases, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
	MEM_ROOT_GUARD(mem_root);
	FOREIGN_SERVER *server, server_buffer;
	string tdbctl_server_name = tc_get_server_name(ret, &mem_root, wrapper, with_slave);
	if (ret)
	{
		return username;
	}
	if ((server = get_server_by_name(&mem_root, tdbctl_server_name.c_str(), &server_buffer)))
	{
	  username = server->username;
		return username;
	}
	if (username.size() <= 0) 
	{
	  ret = 1;
		return username;
	}
	return username;
}

/* TODO: get rid of this */
MYSQL* tc_conn_connect(string ipport, string user, string passwd)
{
  int read_timeout = 600;
  int write_timeout = 600;
  int connect_timeout = 60;
  ulong pos = ipport.find("#");
  string hosts = ipport.substr(0, pos);
  string ports = ipport.substr(pos + 1);
  uint port = atoi(ports.c_str());
  uint connect_retry_count = 3;
  uint real_connect_option = 0;
  uint ssl_mode = SSL_MODE_DISABLED;
  MYSQL* mysql;

  if (user.length() == 0 && passwd.length() == 0)
  {
	  sql_print_error("tc connect fail: username or password is empty");
    return NULL;
  }

  while (connect_retry_count-- > 0)
  {
    mysql = mysql_init(NULL);
    mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &read_timeout);
    mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &write_timeout);
    mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
    mysql_options(mysql, MYSQL_OPT_SSL_MODE, &ssl_mode);
    real_connect_option = CLIENT_INTERACTIVE | CLIENT_MULTI_STATEMENTS;
    if (!mysql_real_connect(mysql, hosts.c_str(), user.c_str(), passwd.c_str(), "", port, NULL, real_connect_option))
    {
      sql_print_warning("tc connect fail: error code is %d, error message: %s", mysql_errno(mysql), mysql_error(mysql));
      if(mysql)
        mysql_close(mysql);
      if (!connect_retry_count)
        return NULL;
    }
    else
      break;
  }

  return mysql;
}

MYSQL *tc_conn_connect(const AUTH_INFO &auth) {
  return tc_conn_connect(auth.host, auth.port, auth.user, auth.passwd);
}

MYSQL *tc_conn_connect(const string &host, uint port, const string &user,
                       const string &passwd) {
  int read_timeout = TC_CONN_READ_TIMEOUT;
  int write_timeout = TC_CONN_WRITE_TIMEOUT;
  int connect_timeout = TC_CONN_CONNECT_TIMEOUT;
  uint connect_retry_count = TC_CONN_MAX_RETRIES_ON_FAILS;
  uint real_connect_option = 0;
  uint ssl_mode = SSL_MODE_DISABLED;
  MYSQL *mysql;

  if (user.length() == 0 && passwd.length() == 0) {
    sql_print_error("tc connect fail: username or password is empty");
    return NULL;
  }

  while (connect_retry_count-- > 0) {
    mysql = mysql_init(NULL);
    mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &read_timeout);
    mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &write_timeout);
    mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
    mysql_options(mysql, MYSQL_OPT_SSL_MODE, &ssl_mode);
    real_connect_option = CLIENT_INTERACTIVE | CLIENT_MULTI_STATEMENTS;
    if (!mysql_real_connect(mysql, host.c_str(), user.c_str(), passwd.c_str(),
                            "", port, NULL, real_connect_option)) {
      sql_print_warning("tc connect fail: error code is %d, error message: %s",
                        mysql_errno(mysql), mysql_error(mysql));
      if (mysql)
        mysql_close(mysql);
      if (!connect_retry_count)
        return NULL;
    } else
      break;
  }

  return mysql;
}

/*
  get map for  server_name->ipport

  @param (out)
  tdbctl_user_map: 
    key:   ip#port
	value: username

  tdbctl_passwd_map
    key:   ip#port
	value: password

  @retval
	map for result
	key:   server_name
	value: ip#port
*/
map<string, string> get_tdbctl_ipport_map(
	MEM_ROOT* mem,
	map<string, string> &tdbctl_user_map,
	map<string, string> &tdbctl_passwd_map
)
{
  map<string, string> ipport_map;
  FOREIGN_SERVER *server, server_buffer;
  ostringstream  sstr;
  string server_name_pre = tdbctl_control_wrapper_prefix;
  ulong records = get_servers_count();
  tdbctl_user_map.clear();
  tdbctl_passwd_map.clear();

  for (ulong i = 0; i < records; i++)
  {
    sstr.str("");
    sstr << i;
    string hash_value = sstr.str();
    string server_name = server_name_pre + hash_value;
    if ((server = get_server_by_name(mem, server_name.c_str(), &server_buffer)))
    {
      string host = server->host;
      string user = server->username;
      string passwd = server->password;
      sstr.str("");
      sstr << server->port;
      string ports = sstr.str();
      string s = host + "#" + ports;
      ipport_map.insert(pair<string, string>(server_name, s));
      tdbctl_user_map.insert(pair<string, string>(s, user));
      tdbctl_passwd_map.insert(pair<string, string>(s, passwd));
    }
  }

  return ipport_map;
}

map<string, MYSQL*> tc_spider_conn_connect(
  int &ret, 
  set<string> spider_ipport_set, 
  map<string, string> spider_user_map, 
  map<string, string> spider_passwd_map
)
{
  map<string, MYSQL*> conn_map;
  set<string>::iterator its;
  for (its = spider_ipport_set.begin(); its != spider_ipport_set.end(); its++)
  {// ipport_c must like 1.1.1.1#3306
    string ipport = (*its);
    MYSQL* mysql;
    if((mysql = tc_conn_connect(ipport, spider_user_map[ipport], spider_passwd_map[ipport])))
     conn_map.insert(pair<string, MYSQL*>(ipport, mysql));
    else
    {
      /* error */
      ret = 1;
      my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
      break;
    }
  }

  return conn_map;
}

MYSQL* tc_spider_conn_single(
	string &err_msg,
	set<string> spider_ipport_set,
	map<string, string> spider_user_map,
	map<string, string> spider_passwd_map
)
{
  MYSQL* mysql = NULL;
  set<string>::iterator its;
  char buff[1024];
  if (spider_ipport_set.size())
  {
    // ipport must like 1.1.1.1#3306
    string ipport = *(spider_ipport_set.begin());
    if (!(mysql = tc_conn_connect(ipport, spider_user_map[ipport], spider_passwd_map[ipport])))
    {
      /* error */
      sprintf(buff, ER(ER_TCADMIN_CONNECT_ERROR), ipport.c_str());
      err_msg = buff;
    }
  }
  else
    err_msg = "no spider in mysql.servers";

  return mysql;
}

map<string, MYSQL*> tc_remote_conn_connect(
  int &ret, 
  map<string, string> remote_ipport_map,
  map<string, string> remote_user_map, 
  map<string, string> remote_passwd_map
)
{
  map<int, string> ipport_map;
  map<string, MYSQL*> conn_map;
  map<string, string>::iterator its2;

  for (its2 = remote_ipport_map.begin(); its2 != remote_ipport_map.end(); its2++)
  {
    string ipport = its2->second;
    ulong pos = ipport.find("#");
    string hosts = ipport.substr(0, pos);
    string ports = ipport.substr(pos + 1);
    MYSQL* mysql;
    if ((mysql = tc_conn_connect(ipport, remote_user_map[ipport], remote_passwd_map[ipport])))
      conn_map.insert(pair<string, MYSQL*>(ipport, mysql));
    else
    {
      /* error */
      ret = 1;
      my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
      break;
    }
  }
  return conn_map;
}

/*
 use user#password to connect tdbctl

 @retval
   return a map which store each tdbctl's connect
*/
map<string, MYSQL*> tc_tdbctl_conn_connect(
  int &ret,
  map<string, string> tdbctl_ipport_map,
  map<string, string> tdbctl_user_map,
  map<string, string> tdbctl_passwd_map
)
{
  map<int, string> ipport_map;
  map<string, MYSQL*> conn_map;
  map<string, string>::iterator its2;

  for (its2 = tdbctl_ipport_map.begin(); its2 != tdbctl_ipport_map.end(); its2++)
  {
    MYSQL* mysql;
    string ipport = its2->second;
    if ((mysql = tc_conn_connect(ipport, tdbctl_user_map[ipport], tdbctl_passwd_map[ipport])))
      conn_map.insert(pair<string, MYSQL*>(ipport, mysql));
    else
    {
      /* error */
      ret = 1;
      my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
      break;
    }
  }
  return conn_map;
}

/*
  @NOTES
   for MGR on Single-Primary return an connect to the primary member;
   for no MGR mode or Multi-Primary, always use the
     tdbctl_map's first key(which default ordered) to connect

  @retval
   ret: 0 for ok, others error
*/
MYSQL *tc_tdbctl_conn_primary(
        int &ret,
        map<string, string> &tdbctl_ipport_map,
        map<string, string> &tdbctl_user_map,
        map<string, string> &tdbctl_passwd_map
)
{
  MYSQL *conn = NULL;
  string address, host;
  uint port;

  if (tdbctl_ipport_map.empty())
  {
    ret = 1;
    sql_print_warning("tc connect to primary node failed: no tdbctl in mysql.servers");
    return NULL;
  }

  if (tc_get_primary_node(host, &port))
  {
    address = host + "#" + to_string(port);
    /* NOTE: primary member must exist in mysql.servers */
    if (std::find_if(tdbctl_ipport_map.begin(), tdbctl_ipport_map.end(),
        [address](const std::pair<string, string> &tdbctl_ip_port) -> bool {
        return address.compare(tdbctl_ip_port.second) == 0; }) == tdbctl_ipport_map.end())
    {
      ret = 1;
      sql_print_warning("primary member %s not exist in mysql.servers", address.c_str());
      return NULL;
    }
  }
  else
  {//error happened, such as network partitioning
    sql_print_warning("get single-Primary node failed");
    ret = 1;

    return NULL;
  }

  conn = tc_conn_connect(address, tdbctl_user_map[address], tdbctl_passwd_map[address]);
  if (conn == NULL) {
    ret = 1;
    my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), address.c_str());
  }

  return conn;
}

bool tc_conn_free(map<string, MYSQL*> &conn_map)
{
  map<string, MYSQL*>::iterator its;
  for (its = conn_map.begin(); its != conn_map.end(); its++)
  {
    MYSQL *mysql = its->second;
    if (mysql)
    {
      mysql_close(mysql);
      mysql = NULL;
    }
  }
  conn_map.clear();
  return FALSE;
}

bool tc_exec_sql_paral(
    string exec_sql,
    map<string, MYSQL*>& conn_map,
    map<string, tc_exec_info>& result_map)
{
  int i = 0;
  bool result = FALSE;
  int count = conn_map.size();
  thread* thread_array = new thread[count];

  map<string, MYSQL*>::iterator its;
  map<string, tc_exec_info>::iterator its2;
  for (its = conn_map.begin(); its != conn_map.end(); its++)
  {
    string servername = its->first;
    MYSQL* mysql = its->second;
    thread tmp_t(tc_exec_sql_up, mysql, exec_sql, &result_map[servername]);
    thread_array[i] = move(tmp_t);
    i++;
  }

  for (int i = 0; i < count; i++)
  {
    if (thread_array[i].joinable())
      thread_array[i].join();
  }

  for (its2 = result_map.begin(); its2 != result_map.end(); its2++)
  {/* */
    string ipport_or_servername = its2->first;
    tc_exec_info exec_info = its2->second;
    if (exec_info.err_code > 0)
    {
      result = TRUE;
    }
  }

  delete[] thread_array;
  return result;
}

/* execute sql parallel without result, main for command or replace/delete/update */
bool tc_exec_sql_paral(
  string exec_sql, 
  map<string, MYSQL*>& conn_map,
  map<string, tc_exec_info>& result_map,
  map<string, string> user_map,
  map<string, string> passwd_map,
  bool error_retry)
{
  int i = 0;
  bool result = FALSE;
  int count = conn_map.size();
  thread* thread_array = new thread[count];

  map<string, MYSQL*>::iterator its;
  map<string, tc_exec_info>::iterator its2;
  for (its = conn_map.begin(); its != conn_map.end(); its++)
  {
    string ipport_or_servername = its->first;
    MYSQL* mysql = its->second;
    thread tmp_t(tc_exec_sql_up, mysql, exec_sql, &result_map[ipport_or_servername]);
    thread_array[i] = move(tmp_t);
    i++;
  }

  for (int i = 0; i < count; i++)
  {
    if (thread_array[i].joinable())
      thread_array[i].join();
  }

  for (its2 = result_map.begin(); its2 != result_map.end(); its2++)
  {/* */
    string ipport_or_servername = its2->first;
    tc_exec_info exec_info = its2->second;
    if (exec_info.err_code > 0)
    {
      if (error_retry)
      {
        int retry_times = 3;
        while (retry_times-- > 0)
        {/* retry 3 times, 2 seconds interval */
          sleep(2);
          if (conn_map[ipport_or_servername])
          {
            mysql_close(conn_map[ipport_or_servername]);
            conn_map[ipport_or_servername] = NULL;
          }
          if (!tc_reconnect(ipport_or_servername, conn_map, user_map, passwd_map))
          {
            if (!tc_exec_sql_up(conn_map[ipport_or_servername], exec_sql, &exec_info))
              break;
          }
        }
        if (retry_times == -1)
        {/* error after retry, yet */
          result = TRUE;
        }
      }
      else
        result = TRUE;
    }
  }

  delete[] thread_array;
  return result;
}


/* execute sql parallel with result, main for select 
   after call this function, program need to free result_map
*/

bool tc_exec_sql_paral_with_result(
  string exec_sql, 
  map<string, MYSQL*> &conn_map,
  map<string, MYSQL_RES*> &result_map,
  map<string, string> &user_map,
  map<string, string> &passwd_map,
  bool error_retry)
{
  int i = 0;
  bool result = FALSE;
  int count = conn_map.size();
  thread* thread_array = new thread[count];

  map<string, MYSQL*>::iterator its;
  map<string, MYSQL_RES*>::iterator its2;
  for (its = conn_map.begin(); its != conn_map.end(); its++)
  {
    string ipport = its->first;
    MYSQL* mysql = its->second;
    thread tmp_t(tc_exec_sql_up_with_result, mysql, exec_sql, &result_map[ipport]);
    thread_array[i] = move(tmp_t);
    i++;
  }

  for (int i = 0; i < count; i++)
  {
    if (thread_array[i].joinable())
      thread_array[i].join();
  }

  for (its2 = result_map.begin(); its2 != result_map.end(); its2++)
  {/* */
    string ipport = its2->first;
    MYSQL_RES *res = its2->second;
    if (!res)
    {
      if (error_retry)
      {
        int retry_times = 3;
        while (retry_times-- > 0)
        {/* retry 3 times, 2 seconds interval */
          sleep(2);
          if (conn_map[ipport])
          {
            mysql_close(conn_map[ipport]);
            conn_map[ipport] = NULL;
          }
          if (!tc_reconnect(ipport, conn_map, user_map, passwd_map))
          {
            if (!tc_exec_sql_up_with_result(conn_map[ipport], exec_sql, &res))
              break;
          }
        }
        if (retry_times == -1)
        {/* error after retry, yet */
          result = TRUE;
        }
      }
      else
        result = TRUE;
    }
  }

  delete[] thread_array;
  return result;
}

bool tc_reconnect(string ipport,
  map<string, MYSQL*>& spider_conn_map,
  map<string, string> spider_user_map,
  map<string, string> spider_passwd_map)
{
  bool ret = FALSE;
  MYSQL* mysql;
  if ((mysql = tc_conn_connect(ipport, spider_user_map[ipport], spider_passwd_map[ipport])))
  {
    spider_conn_map[ipport] = mysql;
  }
  else
    ret = TRUE;
  return ret;
}


bool tc_exec_sql_up(MYSQL* mysql, string sql, tc_exec_info* exec_info)
{
  if (mysql)
  {
    exec_info->err_code = 0;
    exec_info->err_msg = "";
    return tc_exec_sql_without_result(mysql, sql, exec_info);
  }
  else
  {
    exec_info->err_code = 2013;
    exec_info->err_msg = "mysql is an null pointer";
    return TRUE;
  }
}

MYSQL_RES* tc_exec_sql_up_with_result(MYSQL* mysql, string sql, MYSQL_RES** res)
{
  if (mysql)
  {
    *res = tc_exec_sql_with_result(mysql, sql);
  }
  else
  {
    *res = NULL;
  }
  return *res;
}

MYSQL_RES* tc_exec_sql_with_result(MYSQL* mysql, string sql)
{
  MYSQL_RES* result;
  if (mysql_real_query(mysql, sql.c_str(), sql.length()))
  {
    sql_print_error("failed to query: %s . errno : %d; error msg: %s", sql.c_str(), mysql_errno(mysql), mysql_error(mysql));
    result = NULL;
  }
  else
  {
    /*
    //  not a select, field count is 0 , and result is NULL
    if (mysql_field_count(mysql) == 0)
    */
    result = mysql_store_result(mysql);
  }
  return result;
}


bool tc_exec_sql_without_result(MYSQL* mysql, string sql, tc_exec_info* exec_info)
{
  int ret = mysql_real_query(mysql, sql.c_str(), sql.length());
  while (!ret)
  {
    ret = tc_mysql_next_result(mysql);
  }
  if (ret != -1)
  {/* error happened */
    exec_info->err_code = mysql_errno(mysql);
    exec_info->err_msg = mysql_error(mysql);
    return TRUE;
  }
  return FALSE;
}

my_time_t string_to_timestamp(const string s)
{
  MYSQL_TIME_STATUS status;
  MYSQL_TIME l_time;
  long dummy_my_timezone;
  my_bool dummy_in_dst_time_gap;
  const char* str = s.c_str();
  /* We require a total specification (date AND time) */
  if (str_to_datetime(str, strlen(str), &l_time, 0, &status) ||
        l_time.time_type != MYSQL_TIMESTAMP_DATETIME || status.warnings)
  {
    exit(1);
  }
  return my_system_gmt_sec(&l_time, &dummy_my_timezone, &dummy_in_dst_time_gap);
}

void init_result_map(map<string, tc_exec_info>& result_map,
        set<string> &ipport_set)
{
  std::for_each(ipport_set.begin(), ipport_set.end(), [&](string ipport) {
    tc_exec_info exec_info;
    exec_info.err_code = 0;
    exec_info.row_affect = 0;
    exec_info.err_msg = "";
    result_map.insert(pair<string, tc_exec_info>(ipport, exec_info));
  });
}

void init_result_map2(map<string, tc_exec_info>& result_map,
        map<string, string> &ipport_map)
{
  std::for_each(ipport_map.begin(), ipport_map.end(), [&](std::pair<string, string>its){
    string ipport = its.second;
    tc_exec_info exec_info;
    exec_info.err_code = 0;
    exec_info.row_affect = 0;
    exec_info.err_msg = "";
    result_map.insert(pair<string, tc_exec_info>(ipport, exec_info));
  });
}

string concat_result_map(map<string, tc_exec_info> result_map)
{
  string result;
  std::for_each(result_map.begin(), result_map.end(), [&](std::pair<string, tc_exec_info>its) {
    if (its.second.err_code != 0)
      result += its.first + its.second.err_msg;
  });

  return result;
}

/*
  get mysql variable value
*/
string tc_get_variable_value(MYSQL *conn, const char *variable)
{
  MYSQL_RES* res;
  MYSQL_ROW row = NULL;
  char sql[256];
  sprintf(sql, "select @@%s", variable);
  res = tc_exec_sql_with_result(conn, sql);
  //use to free result.
  MYSQL_RES_GUARD(res);
  if (res && (row = mysql_fetch_row(res)))
    return row[0];

  return NULL;
}

/*
  check whether wrapper name is valid

  @param org_name Name of wrapper and length

  @retval IDENT_WRAPPER_OK    Identifier wrapper name is Ok (Success)
  @retval IDENT_WRAPPER_WRONG Identifier wrapper name is Wrong (ER_TCADMIN_WRONG_WRAPPER_NAME)

*/
enum_ident_wrapper_check tc_check_wrapper_name(LEX_STRING *org_name)
{
  char *name= org_name->str;
  size_t name_length= org_name->length;

  if (!name_length || name_length > NAME_LEN)
  {
    my_error(ER_TCADMIN_WRONG_WRAPPER_NAME, MYF(0), org_name->str);
    return IDENT_WRAPPER_WRONG;
  }

  if (strcasecmp(name, SPIDER_WRAPPER) != 0 &&
          strcasecmp(name, TDBCTL_WRAPPER) != 0 &&
          strcasecmp(name, MYSQL_WRAPPER) != 0 &&
          strcasecmp(name, SPIDER_SLAVE_WRAPPER) != 0 &&
          strcasecmp(name, MYSQL_SLAVE_WRAPPER) !=0)
  {
    my_error(ER_TCADMIN_WRONG_WRAPPER_NAME, MYF(0), name, "only support TDBCTL, SPIDER, SPIDER_SLAVE, mysql wrapper");
    return IDENT_WRAPPER_WRONG;
  }

  return IDENT_WRAPPER_OK;
}

/*
  Generate internal spider GRANT sql according to mysql.servers's info.
  Each spider should do [GRANT ALL PRIVILEGES] sql for all tdbctls, which use to
  do DDL on spider. If not, after tdbctl(MGR) failover, new primary tdbctl may
  access denied by spider
  The generate sqls should execute on each spider
*/
string tc_get_spider_grant_sql(
        set<string> &spider_ipport_set,
        map<string, string> &spider_user_map,
        map<string, string> &spider_passwd_map,
        map<string, string> &tdbctl_ipport_map,
        map<string, string> &tdbctl_user_map,
        map<string, string> &tdbctl_passwd_map)
{
  string spider_do_sql;
  char create_sql[FN_REFLEN], grant_sql[FN_REFLEN];
  std::for_each(spider_ipport_set.begin(), spider_ipport_set.end(), [&](string spider_ip_port)
  {
    const char *spider_user = spider_user_map[spider_ip_port].c_str();
    const char *spider_passwd = spider_passwd_map[spider_ip_port].c_str();
    std::for_each(tdbctl_ipport_map.begin(), tdbctl_ipport_map.end(), [&](std::pair<string, string>tdbctl_ip_port)
    {
      string tdbctl_address = tdbctl_ip_port.second;
      ulong pos = tdbctl_address.find("#");
      string tdbctl_host = tdbctl_address.substr(0, pos);

      //tdbctl use spider's user, password to connect current spider
      sprintf(create_sql, "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED BY '%s';",
              spider_user, tdbctl_host.c_str(), spider_passwd);
      sprintf(grant_sql, "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION;",
              spider_user, tdbctl_host.c_str());
      spider_do_sql += create_sql;
      spider_do_sql += grant_sql;
    });
  });

  return spider_do_sql;
}


/*
  Generate internal tdbctl GRANT sql according to mysql.servers's info.
  All tdbctl should do [GRANT ALL PRIVILEGES] sql for all spiders, which use
  to transfer sql from spider to tdbctl.
  All tdbctl should do [GRANT ALL PRIVILEGES] sql for other tdbctl, which use
  to connect and manager cluster, if not, after failure, new elected primary tdbctl
  may had no privileges to connect other tdbctl
  In replication scenario, only primary/master node need to do this, which ensure to sync privileges
  to other tdbctl.
*/
string tc_get_tdbctl_grant_sql(
        set<string> &spider_ipport_set,
        map<string, string> &spider_user_map,
        map<string, string> &spider_passwd_map,
        map<string, string> &tdbctl_ipport_map,
        map<string, string> &tdbctl_user_map,
        map<string, string> &tdbctl_passwd_map)
{
  string tdbctl_do_sql;
  char create_sql[FN_REFLEN], grant_sql[FN_REFLEN];

  std::for_each(tdbctl_ipport_map.begin(), tdbctl_ipport_map.end(), [&](std::pair<string, string>tdbctl_ip_port)
  {
    string tdbctl_address = tdbctl_ip_port.second;
    ulong pos = tdbctl_address.find("#");
    string tdbctl_host = tdbctl_address.substr(0, pos);
    const char *tdbctl_user = tdbctl_user_map[tdbctl_address].c_str();
    const char *tdbctl_passwd = tdbctl_passwd_map[tdbctl_address].c_str();

    /**
      use tdbctl's user, password to connect other tdbctl.
      It's necessary to do this, otherwise, after failure, new elected primary
      tdbctl may have no privilege to manager cluster.
    */
    sprintf(create_sql, "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED BY '%s';",
      tdbctl_user, tdbctl_host.c_str(), tdbctl_passwd);
    sprintf(grant_sql, "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION;",
      tdbctl_user, tdbctl_host.c_str());
    tdbctl_do_sql += create_sql;
    tdbctl_do_sql += grant_sql;

    //spider use tdbctl's user, password to connect tdbctl
    std::for_each(spider_ipport_set.begin(), spider_ipport_set.end(), [&](string spider_ip_port)
    {
      ulong pos = spider_ip_port.find("#");
      string spider_host = spider_ip_port.substr(0, pos);
      sprintf(create_sql, "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED BY '%s';",
        tdbctl_user, spider_host.c_str(), tdbctl_passwd);
      sprintf(grant_sql, "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION;",
        tdbctl_user, spider_host.c_str());
      tdbctl_do_sql += create_sql;
      tdbctl_do_sql += grant_sql;
    });
  });

  return tdbctl_do_sql;
}

/*
  Generate internal remote GRANT sql according to mysql.servers's info.
  1. remote should do [GRANT SELECT, INSERT, TRUNCATE PRIVILEGES] sql for all spiders, spider need
  privileges do DML on remote
  2. remote should do [GRANT ALL PRIVILEGES] for all tdbctls, which use to connect remote and
  do DDL. We must do this, if not, after tdbctl(MGR) failover, new primary tdbctl may access denied by remote
  The generate sqls need to execute on all remotes
*/
string tc_get_remote_grant_sql(
        set<string> &spider_ipport_set,
        map<string, string> &spider_user_map,
        map<string, string> &spider_passwd_map,
        map<string, string> &remote_ipport_map,
        map<string, string> &remote_user_map,
        map<string, string> &remote_passwd_map,
        map<string, string> &tdbctl_ipport_map,
        map<string, string> &tdbctl_user_map,
        map<string, string> &tdbctl_passwd_map)
{
  string remote_do_sql;
  char create_sql[FN_REFLEN], grant_sql[FN_REFLEN];

  std::for_each(remote_ipport_map.begin(), remote_ipport_map.end(), [&](std::pair<string, string>remote_ip_port)
  {
    string remote_address = remote_ip_port.second;
    const char *remote_user = remote_user_map[remote_address].c_str();
    const char *remote_passwd = remote_passwd_map[remote_address].c_str();

    //remote do grants for spider
    std::for_each(spider_ipport_set.begin(), spider_ipport_set.end(), [&](string spider_address)
    {
      ulong pos = spider_address.find("#");
      string spider_host = spider_address.substr(0, pos);

      //spider use remote's user, password to connect remote do DML
      sprintf(create_sql, "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED BY '%s';",
              remote_user, spider_host.c_str(), remote_passwd);
      sprintf(grant_sql, "GRANT SELECT, INSERT, DELETE, UPDATE, DROP ON *.* to '%s'@'%s' WITH GRANT OPTION;",
              remote_user, spider_host.c_str());
      remote_do_sql += create_sql;
      remote_do_sql += grant_sql;
    });

    //remote do grants for tdbctl
    std::for_each(tdbctl_ipport_map.begin(), tdbctl_ipport_map.end(), [&](std::pair<string, string>tdbctl_ip_port)
    {
      string tdbctl_address = tdbctl_ip_port.second;
      ulong pos = tdbctl_address.find("#");
      string tdbctl_host = tdbctl_address.substr(0, pos);

      //tdbctl use remote's user, password to connect remote do all
      sprintf(create_sql, "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED BY '%s';",
              remote_user, tdbctl_host.c_str(), remote_passwd);
      sprintf(grant_sql, "GRANT ALL PRIVILEGES ON *.* to '%s'@'%s' WITH GRANT OPTION;",
              remote_user, tdbctl_host.c_str());
      remote_do_sql += create_sql;
      remote_do_sql += grant_sql;
    });
  });

  return remote_do_sql;
}

/*
  @param (out)
   host: host to primary member
   port: port to primary member

  @retval
   0: empty mysql.servers, host not found or error happened.
   1: mgr running with single-primary.
   2. not mgr or multi-primary

  @Note
   only when retval=1 or 2, host and port[out] with value
*/
uint tc_get_primary_node(std::string &host, uint *port)
{
  int ret = 0;
  ret = get_group_replication_primary_node_info(host, port);

  if (ret == 2)
  {//not mgr or multi-Primary
    MEM_ROOT mem_root;
    list<FOREIGN_SERVER*> server_list;
    std::list<FOREIGN_SERVER*>::iterator iter;
    std::string checkhost;
    bool localcluster_flag = false;
    bool found_host = false;

    init_sql_alloc(key_memory_bases, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
    MEM_ROOT_GUARD(mem_root);
    get_server_by_wrapper(server_list, &mem_root, TDBCTL_WRAPPER, false);

    //empty, error happened
    if (server_list.empty())
      return 0;

    // When execute CREATE/ALTER/DROP NODE query command, we
    // have ensured that mysql.servers can't contain both loopback network
    // address and external network address (through func verify_validity_of_routing_host()).
    // Therefore, we believe that mysql.servers contain either only loopback network addresses
    // or only external network addresses. 
    // We just need to check the first host of mysql.servers.
    checkhost = server_list.front()->host;
    if (!checkhost.compare("localhost") || !checkhost.compare("127.0.0.1")) {
      localcluster_flag = true;
    }

    // return local network address ip and port
    std::set<std::string> ips;
    get_ip_local_addresses(ips, true);
    for(iter = server_list.begin(); iter != server_list.end(); iter++) {
      if (localcluster_flag && (*iter)->port == mysqld_port) {
        host = (*iter)->host;
        *port = (*iter)->port;
        found_host = true;
        break;
      } else if (!localcluster_flag &&
                 ips.find((*iter)->host) != ips.end() &&
                 (*iter)->port == mysqld_port) {
        host = (*iter)->host;
        *port = (*iter)->port;
        found_host = true;
        break;
      }
    }
    
    // no host was found, return error
    if(!found_host)
      return 0;
  }

  return ret;
}


/*
  @retval
    0, not primary node
    1, primary node

  @Note
    anytime call this function, should consider deadlock.
    if we call this in mysql_execute_command, MGR's work thread
    may deadlock when do command internal use Sql_service_command_interface
  
  @todo: this func need to be assessed in the future
*/
int tc_is_primary_tdbctl_node()
{
  int ret = 0;
  string host;
  uint port = 0;

  /*
    NB: always need do this at present.
    If MGR member go to OFFLINE, ERROR, or network partition, new elect
    happened, tdbctl_is_primary's value changed automatic by MGR handler.
  */
  ret = tc_get_primary_node(host, &port);

  //ret == 1, mgr running with single-primary
  if (ret == 1)
    return tdbctl_is_primary;

  //not mgr or multi-Primary
  if (ret == 2)
  {//not mgr or multi-Primary

  /* DEPRECATED*/
  /*
    MYSQL *conn;
    MYSQL_RES* res;
    MYSQL_ROW row;
    MEM_ROOT mem_root;
    list<FOREIGN_SERVER*> server_list;
    string uuid, user, passwd, address;
    uint port = 0;

    string sql = "show variables like  'server_uuid'";
    init_sql_alloc(key_memory_for_tdbctl, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
    MEM_ROOT_GUARD(mem_root);
    get_server_by_wrapper(server_list, &mem_root, TDBCTL_WRAPPER, false);

    //error
    if (server_list.empty())
      return 0;

    //list had been sorted, use first Server_name directly.
    host = server_list.front()->host;
    port = server_list.front()->port;
    user = server_list.front()->username;
    passwd = server_list.front()->password;
    address = host + "#" + to_string(port);
    conn = tc_conn_connect(address, user, passwd);
    if (conn == NULL) {
      sql_print_warning("CONNECT ERROR : error happened when connect to %s",
        address.c_str());
      return 0;
    }

    MYSQL_GUARD(conn);
    res = tc_exec_sql_with_result(conn, sql);
    //use to free result.
    MYSQL_RES_GUARD(res);
    if (res && (row = mysql_fetch_row(res)))
      uuid = row[1];
    else
      return 0;

    //set value
    tdbctl_is_primary = (strcasecmp(uuid.c_str(), server_uuid) == 0) ? 1 : 0;
    */

    //Dont need to set the value of tdbctl_is_primary here.
    //For mgr multi-primary mode, tdbctl_is_primary is set to true automatically 
    //when group_replication plugin is loaded.
    //For non-mgr, users need to set the value of tdbctl_is_primary manually.
    return tdbctl_is_primary;
  }

  return tdbctl_is_primary;
}

/*
  get server_list according to wrapper_name.
  connect each server and parallel execute sql.

  @param
   exec_sql: sql to execute
   wraper_name: wrapper_name
   with_slave: whether diffuse to slave

  @retval
   map for result
   key:Server_name for mysql.servers
   value: execute result
*/
map<string, MYSQL_RES*> tc_exec_sql_paral_by_wrapper(
    string exec_sql, string wrapper_name, bool with_slave)
{
  map<string, MYSQL_RES*> result_map;
  MEM_ROOT mem_root;
  list<FOREIGN_SERVER*> server_list;
  list<thread> thread_list;

  init_sql_alloc(key_memory_bases, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
  MEM_ROOT_GUARD(mem_root);
  get_server_by_wrapper(server_list, &mem_root, wrapper_name.c_str(), with_slave);

  /*
    create thread for work
    each thread do connect, and execute sql
  */
  for (auto & server: server_list)
  {
    thread tmp_t([&]{
      MYSQL* mysql;
      MYSQL_RES *res;
      string ipport = string(server->host) + "#" + to_string(server->port);
      if (!(mysql = tc_conn_connect(ipport, server->username, server->password)))
      {
        /* error */
        my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
        return;
      }
      //use shared_ptr to release mysql
      MYSQL_GUARD(mysql);
      res = tc_exec_sql_with_result(mysql, exec_sql);
      result_map.insert(pair<string, MYSQL_RES *>(server->server_name, std::move(res)));
    });
    thread_list.push_back(std::move(tmp_t));
  }

  /* wait all thread complete */
  for (auto &td : thread_list) {
    if (td.joinable())
      td.join();
  }

  return result_map;
}

/*
  get server by server_name and then connect
  to server and execute sql.

  @param
   exec_sql: sql to execute
   server_name: Server_name in mysql.servers.
*/
MYSQL_RES* tc_exec_sql_by_server(
	string exec_sql, const char *server_name)
{
  MEM_ROOT mem_root;
  MYSQL* mysql;
  MYSQL_RES *res;
  FOREIGN_SERVER *server;

  init_sql_alloc(key_memory_bases, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
  MEM_ROOT_GUARD(mem_root);

  server = get_server_by_name(&mem_root, server_name, NULL);
  if (server == NULL)
    return NULL;

  string ipport = string(server->host) + "#" + to_string(server->port);
  if (!(mysql = tc_conn_connect(ipport, server->username, server->password)))
  {
    /* error */
    my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
    return NULL;
  }
  //use shared_ptr to release mysql
  MYSQL_GUARD(mysql);
  res = tc_exec_sql_with_result(mysql, exec_sql);

  return res;
}

bool check_server_version(ulong& server_version)
{
	bool res = false;
	ulong server_version_new = get_modify_server_version();
	if (server_version != server_version_new)
	{
		server_version = server_version_new;
		res = true;
	}
	return res;
}

/**
 * Wrapper function which retries and checks errors from getaddrinfo
 */
int
checked_getaddrinfo(const char *nodename, const char *servname,
    const struct addrinfo *hints, struct addrinfo **res)
{
  int	errval = 0;
  /** FIXME: Lookup IPv4 only for now */
  struct addrinfo _hints;
  memset(&_hints, 0, sizeof(_hints));
  _hints.ai_family = PF_INET;
  if (hints == NULL)
    hints = &_hints;
  do {
    if (*res) {
      freeaddrinfo(*res);
      *res = NULL;
    }
    errval = getaddrinfo(nodename, servname, hints, res);
  } while (errval == EAI_AGAIN);
#if defined(EAI_NODATA) && EAI_NODATA != EAI_NONAME
  /* Solaris may return EAI_NODATA as well as EAI_NONAME */
  if (errval && errval != EAI_NONAME && errval != EAI_NODATA) {
#else
  /* FreeBSD has removed the definition of EAI_NODATA altogether. */
  if (errval && errval != EAI_NONAME) {
#endif
  }
  assert((errval == 0 && *res) || (errval != 0 && *res == NULL));
  return errval;
}

bool
get_ipv4_addr_from_hostname(const std::string& host, std::string& ip)
{
  char cip[INET6_ADDRSTRLEN];
  struct addrinfo *addrinf = NULL;

  checked_getaddrinfo(host.c_str(), 0, NULL, &addrinf);
  if (!inet_ntop(AF_INET, &((struct sockaddr_in *)addrinf->ai_addr)->sin_addr,
    cip, sizeof(cip)))
  {
    if (addrinf)
      freeaddrinfo(addrinf);
    return true;
  }

  ip.assign(cip);
  if (addrinf)
    freeaddrinfo(addrinf);

  return false;
}

#ifdef _WIN32

bool get_ip_local_addresses(std::set<std::string>& network_addr, bool filter_out_inactive)
{
    HMODULE hIphlpapi = LoadLibrary("iphlpapi.dll");
    if (hIphlpapi == NULL) {
      printf("Failed to load iphlpapi.dll\n");
      return 1;
    }

    typedef DWORD(WINAPI *GetAdaptersAddresses_t)(ULONG, ULONG, PVOID, PIP_ADAPTER_ADDRESSES, PULONG);
    GetAdaptersAddresses_t GetAdaptersAddresses_func = (GetAdaptersAddresses_t)GetProcAddress(hIphlpapi, "GetAdaptersAddresses");
    if (GetAdaptersAddresses_func == NULL) {
      printf("Failed to get GetAdaptersAddresses function pointer\n");
      return 1;
    }

    PIP_ADAPTER_ADDRESSES pAddresses = NULL;
    ULONG family = AF_UNSPEC;
    ULONG flags = GAA_FLAG_INCLUDE_PREFIX;

    // Allocate a buffer to hold the adapter addresses
    ULONG bufLen = 0;
    DWORD ret = GetAdaptersAddresses_func(family, flags, NULL, pAddresses, &bufLen);
    if (ret == ERROR_BUFFER_OVERFLOW) {
      pAddresses = (IP_ADAPTER_ADDRESSES*)malloc(bufLen);
      ret = GetAdaptersAddresses_func(family, flags, NULL, pAddresses, &bufLen);
    }

    // Enumerate the adapter addresses
    for (PIP_ADAPTER_ADDRESSES pCurrAddresses = pAddresses; pCurrAddresses != NULL; pCurrAddresses = pCurrAddresses->Next) {
      for (PIP_ADAPTER_UNICAST_ADDRESS pUnicast = pCurrAddresses->FirstUnicastAddress; pUnicast != NULL; pUnicast = pUnicast->Next) {
        // Print the interface name and address
        printf("%ws: %ws\n", pCurrAddresses->FriendlyName, pUnicast->Address.lpSockaddr->sa_data);
      }
    }

    // Free the buffer
    free(pAddresses);

    // Free the iphlpapi.dll library
    FreeLibrary(hIphlpapi);

    return 0;
}

#else
bool
get_ip_local_addresses(std::set<std::string>& network_addr,
                         bool filter_out_inactive)
{
  struct ifaddrs * ifAddrStruct=NULL;
  struct ifaddrs * ifa=NULL;
  void * tmpAddrPtr=NULL;

  getifaddrs(&ifAddrStruct);

  for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
    if (!ifa->ifa_addr) {
      continue;
    }
    if (ifa->ifa_addr->sa_family == AF_INET) { // check it is IP4
      tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      if(!inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN)) {
        if (ifAddrStruct!=NULL) freeifaddrs(ifAddrStruct);
        return true;
      }

      if (filter_out_inactive && (ifa->ifa_flags & IFF_UP) && (ifa->ifa_flags & IFF_RUNNING)) {
        if(!(ifa->ifa_flags & IFF_LOOPBACK)) {
          network_addr.insert(addressBuffer);
        }
      }
    } else if (ifa->ifa_addr->sa_family == AF_INET6) { // check it is IP6
      tmpAddrPtr=&((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
      char addressBuffer[INET6_ADDRSTRLEN];
      if(!inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN)) {
        if (ifAddrStruct!=NULL) freeifaddrs(ifAddrStruct);
        return true;
      }

      if (filter_out_inactive && (ifa->ifa_flags & IFF_UP) && (ifa->ifa_flags & IFF_RUNNING)) {
        if(!(ifa->ifa_flags & IFF_LOOPBACK)) {
            network_addr.insert(addressBuffer);
        }
      }
    } 
  }
  if (ifAddrStruct!=NULL) freeifaddrs(ifAddrStruct);
  return false;
}
#endif

bool verify_validity_of_routing_host(MEM_ROOT *mem, const char *server_host) {
  std::list<FOREIGN_SERVER*> server_list;
  uint localhost_count = 0;
  if (!server_host || strlen(server_host) == 0) {
    return true;
  }

  get_server_by_wrapper(server_list, mem, NULL_WRAPPER, TRUE);
  if (!strcasecmp(server_host, "127.0.0.1") || !strcasecmp(server_host, "localhost")) {
        localhost_count++;
  }

  if (!server_list.empty()) {
    std::for_each(server_list.begin(), server_list.end(), [&localhost_count](const FOREIGN_SERVER *s){
      if (!strcasecmp(s->host, "127.0.0.1") || !strcasecmp(s->host, "localhost")) {
        localhost_count++;
      }
    });
    if (localhost_count != 0 && localhost_count != server_list.size() + 1) {
        return false;
    }
  }
  return true;
}

const char *get_wrapper_name_by_node_type(enum_node_type type) {
  switch (type) {
  case NODE_TYPE_SPIDER:
    return SPIDER_WRAPPER;
  case NODE_TYPE_SPIDER_SLAVE:
    return SPIDER_SLAVE_WRAPPER;
  case NODE_TYPE_REMOTE:
    return MYSQL_WRAPPER;
  case NODE_TYPE_REMOTE_SLAVE:
    return MYSQL_SLAVE_WRAPPER;
  case NODE_TYPE_CTL:
    return TDBCTL_WRAPPER;
  default: /* should be unreachable */
    break;
  }
  return NULL;
}

Query_exec_manager::Query_exec_manager(THD *thd) : m_thd(thd), error(0) {}

void Query_exec_manager::clear() {
  reset_error();
  for (int i = ENUM_NODE_TYPE_BEGIN; i < ENUM_NODE_TYPE_END; ++i) {
    exec_results[i].clear();
    exec_queries[i].clear();
    real_queries[i].clear();
  }
}

int Query_exec_manager::make_real_query(const std::string &exec_query,
                                        std::string &real_query,
                                        enum_node_type node_type) {
  real_query.clear();

  if (m_thd) {
    /* 1. SET NAMES */
    real_query += "SET NAMES ";
    real_query += m_thd->charset()->csname;
    real_query += ";";

    /* 2. SQL_MODE */
    LEX_STRING sql_mode_str;
    sql_mode_string_representation(m_thd, m_thd->variables.sql_mode, &sql_mode_str);
    real_query += "SET sql_mode='";
    real_query += string(sql_mode_str.str, sql_mode_str.length);
    real_query += "';";

    /* 3.(only for Spider) */
    if (node_type == NODE_TYPE_SPIDER || node_type == NODE_TYPE_SPIDER_SLAVE)
      real_query += "/*!50600 SET ddl_execute_by_ctl=0 */;";

    real_query += exec_query;
  } else {
    real_query = exec_query;
  }

  return 0;
}

bool Query_exec_manager::get_real_query(const std::string &server_name,
                                       std::string &real_query,
                                       enum_node_type node_type) {
  std::map<std::string, std::string>::iterator found;
  found = real_queries[node_type].find(server_name);
  if (found == real_queries[node_type].end())
    return true;
  real_query = found->second;
  return false;
}

void Query_exec_manager::store_exec_info(const std::string &server_name,
                                     const tc_exec_info &exec_info,
                                     enum_node_type node_type) {
  result_mtx.lock();
  if (exec_info.err_code)
    error = 1;
  exec_results[node_type][server_name] = exec_info;
  result_mtx.unlock();
}

int Query_exec_manager::get_exec_info(const std::string &server_name, tc_exec_info &exec_info,
                  enum_node_type node_type) {
  result_mtx.lock();
  exec_info = exec_results[node_type][server_name];
  result_mtx.unlock();
  return 0;
}

MY_ATTRIBUTE((unused))
void Query_exec_manager::store_exec_query(const std::string &server_name,
                                          const std::string &query,
                                          enum_node_type node_type) {
  DBUG_ENTER("Query_exec_manager::store_exec_query");
  DBUG_PRINT("info", ("storing to server: %s", server_name.c_str()));
  string real_query;
  make_real_query(query, real_query, node_type);

  DBUG_ASSERT(exec_queries[node_type].find(server_name) !=
              exec_queries[node_type].end());
  DBUG_ASSERT(real_queries[node_type].count(server_name));
  exec_queries[node_type][server_name] = query;
  real_queries[node_type][server_name] = real_query;

  DBUG_VOID_RETURN;
}

void Query_exec_manager::store_exec_query(const std::string &query,
                                          enum_node_type node_type) {
  DBUG_ENTER("Query_exec_manager::store_exec_query");
  DBUG_PRINT("info",
             ("storing to all nodes of type: %d using query", (int)node_type));
  string real_query;
  make_real_query(query, real_query, node_type);

  std::map<string, string>::iterator it;
  for (it = exec_queries[node_type].begin();
       it != exec_queries[node_type].end(); ++it) {
    const string &server_name = it->first;
    it->second = query;
    real_queries[node_type][server_name] = real_query;
  }

  DBUG_VOID_RETURN;
}

void Query_exec_manager::store_exec_query(
    const std::map<std::string, std::string> &sql_map,
    enum_node_type node_type) {
  DBUG_ENTER("Query_exec_manager::store_exec_query");
  DBUG_PRINT("info",
             ("storing all nodes of type: %d using sql_map", (int)node_type));
  DBUG_ASSERT(sql_map.size() == exec_queries[node_type].size());

  std::map<string, string>::const_iterator it;
  for (it = sql_map.begin(); it != sql_map.end(); ++it) {
    const string &server_name = it->first;
    const string &query = it->second;
    string real_query;
    make_real_query(query, real_query, node_type);

    DBUG_ASSERT(exec_queries[node_type].count(server_name));
    DBUG_ASSERT(real_queries[node_type].count(server_name));
    exec_queries[node_type][server_name] = query;
    real_queries[node_type][server_name] = real_query;
  }

  DBUG_VOID_RETURN;
}

int Query_exec_manager::get_results(tc_execute_result *res) const {
  res->result = error;
  std::map<string, tc_exec_info>::const_iterator it;
  for (it = exec_results[NODE_TYPE_SPIDER].begin();
       it != exec_results[NODE_TYPE_SPIDER].end(); ++it) {
    res->result_info[NODE_TYPE_SPIDER].insert(std::make_pair(it->first, it->second));
  }
  for (it = exec_results[NODE_TYPE_SPIDER_SLAVE].begin();
       it != exec_results[NODE_TYPE_SPIDER_SLAVE].end(); ++it) {
    res->result_info[NODE_TYPE_SPIDER_SLAVE].insert(std::make_pair(it->first, it->second));
  }
  for (it = exec_results[NODE_TYPE_REMOTE].begin();
       it != exec_results[NODE_TYPE_REMOTE].end(); ++it) {
    res->result_info[NODE_TYPE_REMOTE].insert(std::make_pair(it->first, it->second));
  }
  for (it = exec_results[NODE_TYPE_REMOTE_SLAVE].begin();
       it != exec_results[NODE_TYPE_REMOTE_SLAVE].end(); ++it) {
    res->result_info[NODE_TYPE_REMOTE_SLAVE].insert(std::make_pair(it->first, it->second));
  }
  return 0;
}

void Query_exec_manager::build_server_maps(Cluster_conn_manager *conn_mgr) {
  clear();
  for (int i = ENUM_NODE_TYPE_BEGIN; i < ENUM_NODE_TYPE_END; ++i) {
    const std::map<string, MYSQL *> &servers = conn_mgr->server_conns[i];
    std::map<string, MYSQL *>::const_iterator it;
    for (it = servers.begin(); it != servers.end(); ++it) {
      const string &server_name = it->first;
      exec_results[i][server_name] = tc_exec_info();
      exec_queries[i][server_name] = string();
      real_queries[i][server_name] = string();
    }
  }
}

Cluster_conn_manager::Cluster_conn_manager()
    : initialized(false), spider_count(0U), shard_count(0U),
      server_version(0UL) {
  init_alloc_root(PSI_NOT_INSTRUMENTED, &mem_root, 8192, 0);
}

Cluster_conn_manager::~Cluster_conn_manager() {
  clear();
  free_root(&mem_root, MYF(0));
}

void Cluster_conn_manager::clear() {
  for (int i = ENUM_NODE_TYPE_BEGIN; i < ENUM_NODE_TYPE_END; ++i) {
    std::map<string, MYSQL *>::iterator svr;
    for (svr = server_conns[i].begin(); svr != server_conns[i].end(); ++svr) {
      /* No need for NULL checks, mysql_close() does it */
      mysql_close(svr->second);
    }
    server_conns[i].clear();
    server_auths[i].clear();
  }
  initialized = false;
  spider_count = 0;
  shard_count = 0;
}

bool Cluster_conn_manager::refresh(bool force, bool no_connect) {
  DBUG_ENTER("Cluster_conn_manager::refresh");

  bool outdated = check_server_version();
  if (!force && initialized && !outdated) {
    /* No need to refresh */
    DBUG_RETURN(false);
  }
  clear();

  int i, err = 0;
  /* Import mysql.servers into maps */
  for (i = ENUM_NODE_TYPE_BEGIN; i < ENUM_NODE_TYPE_END; ++i) {
    list<FOREIGN_SERVER *> server_list;
    const char *wrapper = get_wrapper_name_by_node_type((enum_node_type)i);
    DBUG_ASSERT(wrapper);
    if (unlikely(!wrapper))
      continue;
    get_server_by_wrapper(server_list, &mem_root, wrapper, false);

    FOREIGN_SERVER *server;
    list<FOREIGN_SERVER *>::iterator it;
    for (it = server_list.begin(); it != server_list.end(); ++it) {
      AUTH_INFO auth;
      string server_name;

      server = *it;
      server_name.assign(server->server_name, server->server_name_length);
      fill_auth_info(&auth, server->host, server->port, server->username,
                     server->password);
      server_auths[i][server_name] = auth;
    }
  }

  /* Build connections */
  for (i = ENUM_NODE_TYPE_BEGIN; i < ENUM_NODE_TYPE_END; ++i) {
    map<string, MYSQL *> &conn_map = server_conns[i];
    const map<string, AUTH_INFO> &auth_map = server_auths[i];
    map<string, AUTH_INFO>::const_iterator it;
    for (it = auth_map.begin(); it != auth_map.end(); ++it) {
      MYSQL *mysql;
      const string &server_name = it->first;
      const AUTH_INFO &auth = it->second;
      if (no_connect) {
        conn_map[server_name] = NULL;
        continue;
      }
      if ((mysql =
               tc_conn_connect(auth))) {
        conn_map[server_name] = mysql;
      } else {
        my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), auth.ipport_str.c_str());
        err = 1;
        break;
      }
    }
    if (err)
      break;
  }

  if (err) {
    clear();
    DBUG_RETURN(true);
  }
  spider_count = server_conns[NODE_TYPE_SPIDER].size();
  shard_count = server_conns[NODE_TYPE_REMOTE].size();
  initialized = true;
  DBUG_RETURN(false);
}

bool Cluster_conn_manager::connect(const std::string &server_name,
                                   enum_node_type type, bool passive) {
  int err;
  char errmsg[256];
  DBUG_ENTER("Cluster_conn_manager::connect");
  DBUG_PRINT("info", ("connecting to server: %s", server_name.c_str()));

  DBUG_ASSERT(initialized);
  if (unlikely(!initialized))
    DBUG_RETURN(TRUE);

  if (!server_conns[type].count(server_name)) {
    snprintf(errmsg, sizeof(errmsg), "cannot find server: %s",
             server_name.c_str());
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), errmsg);
    DBUG_RETURN(TRUE);
  }

  MYSQL *mysql = server_conns[type][server_name];
  const AUTH_INFO &auth = server_auths[type][server_name];

  if (mysql) {
    if (!(err = ping(mysql))) /* existing connection is valid, do nothing */
      DBUG_RETURN(FALSE);
    else {
      mysql_close(mysql);
      server_conns[type][server_name] = mysql = NULL;
      THD *thd = current_thd;
      if (thd) {
        /* Reset the error we got from pinging */
        thd->get_stmt_da()->reset_diagnostics_area();
        if (!thd->variables.tc_auto_fix_conns) {
          /* Auto-fix is disabled, raise an error about the lost connection */
          snprintf(errmsg, sizeof(errmsg),
                   "remote server '%s' (%s:%u) has gone away",
                   server_name.c_str(), auth.host.c_str(), auth.port);
          my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), errmsg);
          DBUG_RETURN(TRUE);
        }
      }
    }
  }

  if ((mysql = tc_conn_connect(auth))) {
    server_conns[type][server_name] = mysql;
  } else if (passive) {
    server_conns[type][server_name] = NULL;
  } else {
    my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), auth.ipport_str.c_str());
    DBUG_RETURN(TRUE);
  }

  DBUG_RETURN(FALSE);
}

bool Cluster_conn_manager::connect(const std::string &server_name, bool passive) {
  char errmsg[128];
  DBUG_ENTER("Cluster_conn_manager::connect");

  DBUG_ASSERT(initialized);
  if (unlikely(!initialized))
    DBUG_RETURN(TRUE);

  for (int i = ENUM_NODE_TYPE_BEGIN; i < ENUM_NODE_TYPE_END; ++i) {
    if (server_conns[i].count(server_name))
      DBUG_RETURN(connect(server_name, enum_node_type(i), passive));
  }

  /* failed to find the server */
  snprintf(errmsg, sizeof(errmsg), "cannot find server: %s",
           server_name.c_str());
  my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), errmsg);
  DBUG_RETURN(TRUE);
}

bool Cluster_conn_manager::connect(enum_node_type type, bool passive) {
  DBUG_ENTER("Cluster_conn_manager::connect");

  DBUG_ASSERT(initialized);
  if (unlikely(!initialized))
    DBUG_RETURN(TRUE);

  map<string, AUTH_INFO>::const_iterator it;
  for (it = server_auths[type].begin(); it != server_auths[type].end(); ++it) {
    const string &server_name = it->first;
    if (connect(server_name, type, passive))
      DBUG_RETURN(TRUE);
  }

  DBUG_RETURN(FALSE);
}

bool Cluster_conn_manager::identify_self() {
  DBUG_ENTER("Cluster_conn_manager::identify_self");

  DBUG_ASSERT(initialized);
  if (unlikely(!initialized))
    DBUG_RETURN(TRUE);

  bool found = FALSE;
  const map<string, MYSQL *> &conns = server_conns[NODE_TYPE_CTL];
  map<string, MYSQL *>::const_iterator conn_it;
  for (conn_it = conns.begin(); conn_it != conns.end(); ++conn_it) {
    MYSQL_ROW row;
    MYSQL_RES *res;
    const string &server_name = conn_it->first;
    MYSQL *mysql = conn_it->second;
    if (!mysql)
      continue;
    /* Get @@server_uuid from server */
    if (mysql_real_query(mysql, SQL_SELECT_SERVER_UUID_STR,
                         sizeof(SQL_SELECT_SERVER_UUID_STR) - 1)) {
      sql_print_error(
          "mysql_real_query() failed when getting server_uuid from %s",
          server_name.c_str());
      continue;
    }
    if (!(res = mysql_store_result(mysql))) {
      sql_print_error(
          "mysql_store_result() failed when getting server_uuid from %s",
          server_name.c_str());
      continue;
    }
    DBUG_ASSERT(mysql_num_rows(res) == 1);
    row = mysql_fetch_row(res);
    /* Check if it is the same as this server's */
    if ((found = !native_strncasecmp(row[0], server_uuid_ptr, UUID_LENGTH))) {
      my_server_name = server_name;
      break;
    }
  }

  DBUG_RETURN(!found);
}

bool Cluster_conn_manager::check_query_manager_validity(
    Query_exec_manager *query_mgr) {
  for (int i = ENUM_NODE_TYPE_BEGIN; i < ENUM_NODE_TYPE_END; ++i) {
    uint node_cnt = server_conns[i].size();
    if (node_cnt != query_mgr->exec_queries[i].size() ||
        node_cnt != query_mgr->real_queries[i].size() ||
        node_cnt != query_mgr->exec_results[i].size()) {
      return true;
    }

    std::map<string, MYSQL *>::iterator svr;
    for (svr = server_conns[i].begin(); svr != server_conns[i].end(); ++svr) {
      const string &server_name = svr->first;
      if (!query_mgr->exec_queries[i].count(server_name) ||
          !query_mgr->real_queries[i].count(server_name) ||
          !query_mgr->exec_results[i].count(server_name))
        return true;
    }
  }
  return false;
}

bool Cluster_conn_manager::check_server_version() {
  /* TODO: use atomic maybe? */
  ulong latest = get_modify_server_version();
  if (server_version != latest) {
    server_version = latest;
    return true;
  }
  return false;
}

int Cluster_conn_manager::ping(MYSQL *mysql) {
  int res;
  DBUG_ENTER("Cluster_conn_manager::ping");
  res = simple_command(mysql, COM_PING, 0, 0, 0);
  if (res == CR_SERVER_LOST && mysql->reconnect)
    res = simple_command(mysql, COM_PING, 0, 0, 0);
  DBUG_RETURN(res);
}

void free_cluster_conn_manager(THD *thd) {
  delete thd->cluster_conn_manager;
}

bool check_tc_command(bool tc_admin, LEX *lex)
{
  bool allowed = true;
  switch (lex->sql_command)
  {
    case TC_SQLCOM_CREATE_NODE:
    case TC_SQLCOM_ALTER_NODE:
    case TC_SQLCOM_DROP_NODE:
    case TC_SQLCOM_FLUSH_ROUTING:
    case TC_SQLCOM_MONITOR_INIT:
    case TC_SQLCOM_SHOW_PROCESSLIST:
    case TC_SQLCOM_SHOW_VARIABLES:
    case TC_SQLCOM_CONN_NODE_EXECUTE_SQL:
    case TC_SQLCOM_ENABLE_PRIMARY:
    case TC_SQLCOM_DISABLE_PRIMARY:
    case TC_SQLCOM_GET_PRIMARY:
      if(!tc_admin)
      {
        allowed = false;
        my_error(ER_TCADMIN_COMMAND_DISABLED, MYF(0));
      }
      break;
    default:
      break;
  }
  return allowed;
}


enum_sql_command tc_unsupport_types[] = { SQLCOM_SHOW_EVENTS, SQLCOM_SHOW_STATUS, SQLCOM_SHOW_STATUS_PROC, SQLCOM_SHOW_STATUS_FUNC,
  SQLCOM_SHOW_DATABASES, SQLCOM_SHOW_TABLES, SQLCOM_SHOW_TRIGGERS, SQLCOM_SHOW_TABLE_STATUS, SQLCOM_SHOW_OPEN_TABLES, SQLCOM_SHOW_PLUGINS,
  SQLCOM_SHOW_FIELDS, SQLCOM_SHOW_KEYS, SQLCOM_SHOW_VARIABLES, SQLCOM_SHOW_CHARSETS, SQLCOM_SHOW_COLLATIONS, SQLCOM_SHOW_STORAGE_ENGINES,
  SQLCOM_SHOW_PROFILE, SQLCOM_PREPARE, SQLCOM_EXECUTE, SQLCOM_DEALLOCATE_PREPARE, SQLCOM_EMPTY_QUERY, SQLCOM_HELP, SQLCOM_PURGE,
  SQLCOM_PURGE_BEFORE, SQLCOM_SHOW_WARNS, SQLCOM_SHOW_ERRORS, SQLCOM_SHOW_PROFILES, SQLCOM_ASSIGN_TO_KEYCACHE, SQLCOM_PRELOAD_KEYS,
  SQLCOM_SHOW_ENGINE_STATUS, SQLCOM_SHOW_ENGINE_MUTEX, SQLCOM_SHOW_BINLOGS, SQLCOM_SHOW_CREATE, SQLCOM_CHECKSUM, SQLCOM_UPDATE,
  SQLCOM_UPDATE_MULTI, SQLCOM_REPLACE, SQLCOM_INSERT, SQLCOM_REPLACE_SELECT, SQLCOM_INSERT_SELECT, SQLCOM_DELETE, SQLCOM_DELETE_MULTI,
  SQLCOM_SHOW_PROCESSLIST, SQLCOM_SHOW_ENGINE_LOGS, SQLCOM_LOAD, SQLCOM_SHOW_CREATE_DB, SQLCOM_XA_START, SQLCOM_XA_END, SQLCOM_XA_PREPARE,
  SQLCOM_XA_COMMIT, SQLCOM_XA_ROLLBACK, SQLCOM_XA_RECOVER, SQLCOM_ALTER_TABLESPACE, SQLCOM_INSTALL_PLUGIN, SQLCOM_UNINSTALL_PLUGIN,
  SQLCOM_ANALYZE, SQLCOM_CHECK, SQLCOM_OPTIMIZE, SQLCOM_REPAIR, SQLCOM_TRUNCATE, SQLCOM_SIGNAL, SQLCOM_RESIGNAL, SQLCOM_GET_DIAGNOSTICS,
  SQLCOM_CALL, SQLCOM_BINLOG_BASE64_EVENT, SQLCOM_HA_OPEN, SQLCOM_HA_CLOSE, SQLCOM_HA_READ, SQLCOM_SHOW_PRIVILEGES, SQLCOM_SHOW_CREATE_USER,
  SQLCOM_SHOW_GRANTS, SQLCOM_SHOW_PROC_CODE, SQLCOM_SHOW_FUNC_CODE, SQLCOM_SHOW_CREATE_PROC, SQLCOM_SHOW_CREATE_FUNC, SQLCOM_SHOW_CREATE_TRIGGER,
  SQLCOM_ALTER_INSTANCE, SQLCOM_CHANGE_REPLICATION_FILTER, SQLCOM_CREATE_COMPRESSION_DICTIONARY, SQLCOM_DROP_COMPRESSION_DICTIONARY,
  SQLCOM_EXPLAIN_OTHER, SQLCOM_LOCK_BINLOG_FOR_BACKUP, SQLCOM_LOCK_TABLES_FOR_BACKUP, SQLCOM_SHOW_CLIENT_STATS, SQLCOM_SHOW_INDEX_STATS,
  SQLCOM_SHOW_TABLE_STATS, SQLCOM_SHOW_THREAD_STATS, SQLCOM_SHOW_USER_STATS, SQLCOM_START_GROUP_REPLICATION, SQLCOM_STOP_GROUP_REPLICATION, SQLCOM_UNLOCK_BINLOG };

//tc_admin=1, only need distribute to spider
enum_sql_command tc_distribute_spider_types[] = { SQLCOM_CREATE_EVENT, SQLCOM_ALTER_EVENT, SQLCOM_CREATE_FUNCTION, SQLCOM_CREATE_PROCEDURE,
  SQLCOM_CREATE_SPFUNCTION, SQLCOM_ALTER_PROCEDURE, SQLCOM_ALTER_FUNCTION, SQLCOM_DROP_PROCEDURE, SQLCOM_DROP_FUNCTION,
  SQLCOM_CREATE_TRIGGER, SQLCOM_DROP_TRIGGER};

enum_sql_command tc_distribute_spider_remote_types[] = { SQLCOM_CREATE_EVENT, SQLCOM_ALTER_EVENT, SQLCOM_CREATE_FUNCTION, SQLCOM_CREATE_PROCEDURE,
  SQLCOM_CREATE_SPFUNCTION, SQLCOM_ALTER_PROCEDURE, SQLCOM_ALTER_FUNCTION, SQLCOM_DROP_PROCEDURE, SQLCOM_DROP_FUNCTION,
  SQLCOM_CREATE_TRIGGER, SQLCOM_DROP_TRIGGER, SQLCOM_CREATE_VIEW, SQLCOM_DROP_VIEW,
  SQLCOM_CREATE_USER, SQLCOM_DROP_USER, SQLCOM_ALTER_USER, SQLCOM_RENAME_USER, SQLCOM_REVOKE, SQLCOM_GRANT, SQLCOM_CREATE_SERVER, SQLCOM_ALTER_SERVER, SQLCOM_DROP_SERVER,
  SQLCOM_CREATE_INDEX, SQLCOM_DROP_INDEX
};

int tdbctl_handle_primary_cmd(THD *thd, LEX *lex) {
  int res;
  enum_sql_command cmd = lex->sql_command;

  if (cmd == TC_SQLCOM_GET_PRIMARY) {
    /* Could be NULL without a proper plugin */
    if (!tdbctl_get_primary) {
      my_error(ER_TCADMIN_EXECUTE_ERROR, MYF(0), "unsupported command");
      return TRUE;
    }
  }

  if (servers_reload(thd)) {
    my_error(ER_SERVERS_LOAD, MYF(0));
    return TRUE;
  }

  if (cmd == TC_SQLCOM_ENABLE_PRIMARY) {
    if (!(res = tdbctl_enable_primary(thd))) {
      sql_print_information("Tdbctl Primary Mode is enabled");
    }
  } else if (cmd == TC_SQLCOM_DISABLE_PRIMARY) {
    if (!(res = tdbctl_disable_primary(thd))) {
      sql_print_information("Tdbctl Primary Mode is disabled");
    }
  } else { /* TC_SQLCOM_GET_PRIMARY */
    res = tdbctl_get_primary(thd);
  }

  return res;
}


static void append_create_user(const AUTH_INFO &auth, std::string &sql) {
  sql += "CREATE USER ";
  sql += "/*!50706 IF NOT EXISTS*/ ";
  sql += TC_STR_SINGLE_QUOTED(auth.user);
  sql += "@";
  sql += TC_STR_SINGLE_QUOTED(auth.host);
  sql += " IDENTIFIED BY ";
  sql += TC_STR_SINGLE_QUOTED(auth.passwd);
  sql += TC_STR_DELIMITER;
}

static void append_grant_privileges(const AUTH_INFO &auth, uint grant,
                                    std::string &sql) {
  uint has_access = grant & ~GRANT_ACL;
  sql += "GRANT ";
  if (test_all_bits(grant, (GLOBAL_ACLS & ~GRANT_ACL))) {
    sql += "ALL PRIVILEGES";
  } else if (!has_access) {
    sql += "USAGE";
  } else {
    for (int i = 0, j = SELECT_ACL, found = 0; j <= GLOBAL_ACLS; ++i, j <<= 1) {
      if (has_access & j) {
        if (found) sql += TC_STR_COMMA;
        found = 1;
        sql.append(command_array[i], command_lengths[i]);
      }
    }
  }
  sql += " ON *.* TO "; /* generate global access by default */
  sql += TC_STR_SINGLE_QUOTED(auth.user);
  sql += "@";
  sql += TC_STR_SINGLE_QUOTED(auth.host);
  if (grant & GRANT_ACL)
    sql += " WITH GRANT OPTION";
  sql += TC_STR_DELIMITER;
}

void tc_generate_grants(Cluster_conn_manager *conn_mgr, const AUTH_INFO *auth,
                        bool all_priv, enum_node_type node_type,
                        std::string &create_user_sql, std::string &grant_sql) {
  uint grant = 0;
  std::set<std::pair<string, string>> user_set;
  create_user_sql.clear();
  grant_sql.clear();

  if (all_priv) {
    grant = GLOBAL_ACLS;
  }

  if (auth) {
    /* Generate for a single node only */
    append_create_user(*auth, create_user_sql);
    append_grant_privileges(*auth, grant, grant_sql);
  } else {
    /* Iterate through all nodes of <node_type> */
    map<string, AUTH_INFO>::const_iterator it;
    for (it = conn_mgr->server_auths[node_type].begin();
         it != conn_mgr->server_auths[node_type].end(); ++it) {
      const AUTH_INFO &tmp_auth = it->second;
      if (user_set.count(std::make_pair(tmp_auth.user, tmp_auth.host))) {
        /* Duplicates can occur when more than one node is on the same host */
        continue;
      }
      user_set.insert(std::make_pair(tmp_auth.user, tmp_auth.host));
      append_create_user(tmp_auth, create_user_sql);
      append_grant_privileges(tmp_auth, grant, grant_sql);
    }
  }
}

//currently only consider tc_amind=1
bool tc_unsupport_sql_type(int sql_type) {
  return std::any_of(std::begin(tc_unsupport_types), std::end(tc_unsupport_types), [=](int i)
    { return i == sql_type; });
}

//command distribute to spider only
bool tc_distribute_spider_only(int sql_type) {
  return std::any_of(std::begin(tc_distribute_spider_types), std::end(tc_distribute_spider_types), [=](int i)
    { return i == sql_type; });
}

//command distribute to spider and remote
bool tc_distribute_spider_and_remote(int sql_type) {
  return std::any_of(std::begin(tc_distribute_spider_remote_types), std::end(tc_distribute_spider_remote_types), [=](int i)
    { return i == sql_type; });
}
