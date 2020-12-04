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


using namespace std;

static PSI_memory_key key_memory_bases;

mutex remote_exec_mtx;
mutex spider_exec_mtx;


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
    default:
        return "unkonw type";
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
    
    const int buf_len = 16;
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
    char str_shard_cnt[] = "shard_count";
    char str_shard_func[] = "shard_func";
    char str_shard_type[] = "shard_type";
	char str_shard_key[] = "shard_key";
	char str_config_table[] = "config_table";
    char str_crc32[] = "crc32";
    char str_crc32_ci[] = "crc32_ci";
    char str_none[] = "none";
    char str_type_list[] = "list";
    char str_type_range[] = "range";
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
    parse_result_t->is_with_shard = FALSE;
    parse_result_t->is_with_autu = FALSE;
    parse_result_t->is_with_unique = FALSE;
    parse_result_t->result = TRUE;
}

void tc_parse_result_destory(TC_PARSE_RESULT *parse_result_t)
{
    parse_result_t->is_with_shard = FALSE;
    parse_result_t->is_with_autu = FALSE;
    parse_result_t->is_with_unique = FALSE;
    parse_result_t->result = FALSE;
    parse_result_t->result_info.clear();
    parse_result_t->shard_key.clear();
}


void tc_query_run(THD *thd, TC_PARSE_RESULT *parse_result_t)
{
    switch (parse_result_t->sql_type)
    {
    case SQLCOM_CREATE_TABLE:
        break;
    case SQLCOM_CREATE_DB:
        break;
    case SQLCOM_DROP_DB:
        break;
	default:
		break;
    }
    return;
}

map<string, string> tc_get_remote_create_table(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    map<string, string> map;
    ostringstream  sstr;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
    string create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string db_org1 = " " + db_name + "\\.";
    string db_org2 = "`" + db_name + "`\\.";
    regex pattern("ENGINE\\s*=\\s*spider", regex::icase);
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
        map.insert(pair<string, string>(server, remote_create_sql));
    }
    return map;
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

string tc_get_spider_create_table(
    TC_PARSE_RESULT *tc_parse_result_t, 
    int shard_count, 
    tspider_shard_func shard_func,
    tspider_shard_type shard_type,
    bool is_unsigned_key
)
{
    ostringstream  sstr;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
    string spider_create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string hash_key = tc_parse_result_t->shard_key;
    string db_name = tc_parse_result_t->db_name;
    string tb_name = tc_parse_result_t->table_name;
    string connection_string;
    string partiton_by = " partition by ";
    string spider_partition_count_str;

    regex pattern1("ENGINE\\s*=\\s*MyISAM", regex::icase);
    regex pattern2("ENGINE\\s*=\\s*InnoDB", regex::icase);
    regex pattern3("ENGINE\\s*=\\s*tokudb", regex::icase);
    regex pattern4("ROW_FORMAT\\s*=\\s*GCS_DYNAMIC", regex::icase);
    regex pattern5("ROW_FORMAT\\s*=\\s*GCS", regex::icase);
    regex pattern6("ENGINE\\s*=\\s*heap", regex::icase);
    spider_create_sql = regex_replace(spider_create_sql, pattern1, "ENGINE = spider");
    spider_create_sql = regex_replace(spider_create_sql, pattern2, "ENGINE = spider");
    spider_create_sql = regex_replace(spider_create_sql, pattern3, "ENGINE = spider");
    spider_create_sql = regex_replace(spider_create_sql, pattern4, "");
    spider_create_sql = regex_replace(spider_create_sql, pattern5, "");
    spider_create_sql = regex_replace(spider_create_sql, pattern6, "");

    sstr << shard_count;
    spider_partition_count_str = sstr.str();
    if (shard_type == tspider_shard_type_list) 
    {
        partiton_by = partiton_by + "list(";
    } 
    else 
    {   /* tspider_shard_type_range */
        partiton_by = partiton_by + "range(";
    }
    if (shard_func == tspider_shard_func_crc32) 
    {
        is_unsigned_key = true;
        partiton_by = partiton_by + "crc32(" + 
            "`" + hash_key + "`" + ")%" + spider_partition_count_str + ") (";
    }
    else if (shard_func == tspider_shard_func_crc32_ci)
    {
        is_unsigned_key = true;
        partiton_by = partiton_by + "crc32_ci(" + 
            "`" + hash_key + "`" + ")%" + spider_partition_count_str + ") (";
    }
    else 
    {   /* tspider_shard_func_none */
        partiton_by = partiton_by + 
            "`" + hash_key + "`" + "%" + spider_partition_count_str + ") (";
    }

    spider_create_sql = spider_create_sql + partiton_by;
    for (int i = 0; i < shard_count; i++)
    {
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string range_value;
        string server_info;
        string pt_sql;
        string server_name = server_name_pre + hash_value;
        server_info = "server \"" + server_name + "\"";
        if (shard_type == tspider_shard_type_list) 
        {
            pt_sql = "PARTITION pt" + hash_value + " values in (" + hash_value + ") COMMENT = 'database \"" 
                + db_name + "_" + hash_value + "\", table \"" + tb_name + "\", " + server_info +  "\' ENGINE = SPIDER";
        }
        else
        {   /* range */
            pt_sql = "PARTITION pt" + hash_value + " values less than (" + tcadmin_get_shard_range_by_index(i, shard_count, is_unsigned_key) 
                + ") COMMENT = 'database \"" 
                + db_name + "_" + hash_value + "\", table \"" + tb_name + "\", " + server_info +  "\' ENGINE = SPIDER";
        }
        if (i < shard_count - 1)
        {
            pt_sql = pt_sql + ",";
        }
        else
        {
            pt_sql = pt_sql + ");";
        }
        spider_create_sql = spider_create_sql + pt_sql;
    }
    spider_create_sql = "use " + db_name + ";" + spider_create_sql;
    sstr.clear();
    return spider_create_sql;
}

string tc_get_spider_drop_table(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    sql = "use " + db_name + ";" + sql;
    return sql;

}

map<string, string> tc_get_remote_drop_table(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    map<string, string> map;
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;


    string db_org1 = " " + db_name + "\\.";
    string db_org2 = "`" + db_name + "`\\.";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < shard_count; i++)
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
        map.insert(pair<string, string>(server, remote_sql));
    }
    return map;
}


string tc_get_spider_create_database(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    string create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    return create_sql;
}


map<string, string> tc_get_remote_create_database(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    map<string, string> map;
    ostringstream  sstr;
    string create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    string db_org1 = " " + db_name;
    string db_org2 = "`" + db_name + "`";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < shard_count; i++)
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
        map.insert(pair<string, string>(server, remote_create_sql));
    }
    return map;
}

string tc_get_spider_drop_database(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    return sql;
}

map<string, string> tc_get_remote_drop_database(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    map<string, string> map;
    ostringstream  sstr;
    string create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    string db_org1 = " " + db_name;
    string db_org2 = "`" + db_name + "`";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < shard_count; i++)
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
        map.insert(pair<string, string>(server, remote_create_sql));
    }
    return map;
}


string tc_get_spider_change_database(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    return sql;
}

map<string, string> tc_get_remote_change_database(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    map<string, string> map;
    ostringstream  sstr;
    string create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    string db_org1 = " " + db_name;
    string db_org2 = "`" + db_name + "`";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < shard_count; i++)
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
        map.insert(pair<string, string>(server, remote_create_sql));
    }
    return map;
}


string tc_get_spider_create_or_drop_index(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    sql = "use " + db_name + ";" + sql;
    return sql;
}


map<string, string> tc_get_remote_create_or_drop_index(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    map<string, string> map;
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    string db_org1 = " " + db_name + "\\.";
    string db_org2 = "`" + db_name + "`\\.";
    regex pattern1(db_org1);
    regex pattern2(db_org2);

    for (int i = 0; i < shard_count; i++)
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
        map.insert(pair<string, string>(server, remote_sql));
    }
    return map;
}


string tc_get_spider_alter_table(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
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
    return sql;
}



map<string, string> tc_get_remote_alter_table(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    map<string, string> map;
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    for (int i = 0; i < shard_count; i++)
    {
        string remote_sql = sql;
        sstr.str("");
        sstr << i;
        string hash_value = sstr.str();
        string remote_db = db_name + "_" + hash_value;
        string server = server_name_pre + hash_value;

        remote_sql = tc_dbname_replace_with_point(remote_sql, db_name, remote_db);
        remote_sql = "use " + remote_db + ";" + remote_sql;
        map.insert(pair<string, string>(server, remote_sql));
    }
    return map;
}



string tc_get_spider_rename_table(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string table_name = tc_parse_result_t->table_name;
    string new_db = tc_parse_result_t->new_db_name;
    string new_table = tc_parse_result_t->new_table_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
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
    return sql;
}


map<string, string> tc_get_remote_rename_table(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    map<string, string> map;
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string new_db = tc_parse_result_t->new_db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    for (int i = 0; i < shard_count; i++)
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
        map.insert(pair<string, string>(server, remote_sql));
    }
    return map;
}


string tc_get_spider_create_table_like(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
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

    for (int i = 0; i < shard_count; i++)
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

        if (i < shard_count - 1)
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
    return sql;
}

map<string, string> tc_get_remote_create_table_like(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
)
{
    map<string, string> map;
    ostringstream  sstr;
    string sql(tc_parse_result_t->query_string.str, 
      tc_parse_result_t->query_string.length);
    string db_name = tc_parse_result_t->db_name;
    string new_db = tc_parse_result_t->new_db_name;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;

    for (int i = 0; i < shard_count; i++)
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
        map.insert(pair<string, string>(server, remote_sql));
    }
    return map;
}



bool tc_query_parse(THD *thd, LEX *lex, TC_PARSE_RESULT *tc_parse_result_t)
{
    return TRUE;
}


/* convert common query to spider/remotedb query */
bool tc_query_convert(
  THD *thd, 
  LEX *lex, 
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count, 
  tspider_shard_func shard_func,
  tspider_shard_type shard_type,
  bool is_unsigned_key,
  string *spider_sql, 
  map<string, string> *remote_sql_map
)
{
    if (tc_parse_result_t->result)
    {// result TURE mean abnormal query
        return tc_parse_result_t->result;
    }
    switch (tc_parse_result_t->sql_type) {
        /* 1. DML or unsupported DDL or DDL don't need tcadmin to execute */
    case SQLCOM_SHOW_EVENTS:
    case SQLCOM_SHOW_STATUS:
    case SQLCOM_SHOW_STATUS_PROC:
    case SQLCOM_SHOW_STATUS_FUNC:
    case SQLCOM_SHOW_DATABASES:
    case SQLCOM_SHOW_TABLES:
    case SQLCOM_SHOW_TRIGGERS:
    case SQLCOM_SHOW_TABLE_STATUS:
    case SQLCOM_SHOW_OPEN_TABLES:
    case SQLCOM_SHOW_PLUGINS:
    case SQLCOM_SHOW_FIELDS:
    case SQLCOM_SHOW_KEYS:
    case SQLCOM_SHOW_VARIABLES:
    case SQLCOM_SHOW_CHARSETS:
    case SQLCOM_SHOW_COLLATIONS:
    case SQLCOM_SHOW_STORAGE_ENGINES:
    case SQLCOM_SHOW_PROFILE:
    case SQLCOM_SELECT:
    case SQLCOM_PREPARE:
    case SQLCOM_EXECUTE:
    case SQLCOM_DEALLOCATE_PREPARE:
    case SQLCOM_EMPTY_QUERY:
    case SQLCOM_HELP:
    case SQLCOM_PURGE:
    case SQLCOM_PURGE_BEFORE:
    case SQLCOM_SHOW_WARNS:
    case SQLCOM_SHOW_ERRORS:
    case SQLCOM_SHOW_PROFILES:
    case SQLCOM_ASSIGN_TO_KEYCACHE:
    case SQLCOM_PRELOAD_KEYS:
    case SQLCOM_SHOW_ENGINE_STATUS:
    case SQLCOM_SHOW_ENGINE_MUTEX:
    case SQLCOM_SHOW_BINLOGS:
    case SQLCOM_SHOW_CREATE:
    case SQLCOM_CHECKSUM:
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_REPLACE:
    case SQLCOM_INSERT:
    case SQLCOM_REPLACE_SELECT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
    case SQLCOM_SHOW_PROCESSLIST:
    case SQLCOM_SHOW_ENGINE_LOGS:
    case SQLCOM_LOAD:
    case SQLCOM_SHOW_CREATE_DB:
    case SQLCOM_XA_START:
    case SQLCOM_XA_END:
    case SQLCOM_XA_PREPARE:
    case SQLCOM_XA_COMMIT:
    case SQLCOM_XA_ROLLBACK:
    case SQLCOM_XA_RECOVER:
    case SQLCOM_ALTER_TABLESPACE:
    case SQLCOM_INSTALL_PLUGIN:
    case SQLCOM_UNINSTALL_PLUGIN:
    case SQLCOM_ANALYZE:
    case SQLCOM_CHECK:
    case SQLCOM_OPTIMIZE:
    case SQLCOM_REPAIR:
    case SQLCOM_TRUNCATE:
    case SQLCOM_SIGNAL:
    case SQLCOM_RESIGNAL:
    case SQLCOM_GET_DIAGNOSTICS:
    case SQLCOM_CALL:
    case SQLCOM_BINLOG_BASE64_EVENT:
    case SQLCOM_HA_OPEN:
    case SQLCOM_HA_CLOSE:
    case SQLCOM_HA_READ:
    case SQLCOM_SHOW_CREATE_PROC:
    case SQLCOM_SHOW_CREATE_FUNC:
    case SQLCOM_SHOW_PROC_CODE:
    case SQLCOM_SHOW_FUNC_CODE:
    case SQLCOM_SHOW_CREATE_TRIGGER:
    case SQLCOM_SHOW_PRIVILEGES:
    case SQLCOM_SHOW_CREATE_USER:
    case SQLCOM_SHOW_GRANTS:
    case SQLCOM_SET_OPTION:
    case SQLCOM_ALTER_INSTANCE:
    case SQLCOM_CHANGE_REPLICATION_FILTER:
    case SQLCOM_CREATE_COMPRESSION_DICTIONARY:
    case SQLCOM_DROP_COMPRESSION_DICTIONARY:
    case SQLCOM_EXPLAIN_OTHER:
    case SQLCOM_LOCK_BINLOG_FOR_BACKUP:
    case SQLCOM_LOCK_TABLES_FOR_BACKUP:
    case SQLCOM_SHOW_CLIENT_STATS:
    case SQLCOM_SHOW_INDEX_STATS:
    case SQLCOM_SHOW_TABLE_STATS:
    case SQLCOM_SHOW_THREAD_STATS:
    case SQLCOM_SHOW_USER_STATS:
    case SQLCOM_START_GROUP_REPLICATION:
    case SQLCOM_STOP_GROUP_REPLICATION:
    case SQLCOM_UNLOCK_BINLOG:
        my_error(ER_TCADMIN_UNSUPPORT_SQL_TYPE, MYF(0), 
          get_stmt_type_str(tc_parse_result_t->sql_type));
        return TRUE;

        /* 2. DDL need dispatch to each spider only */
    case SQLCOM_CREATE_EVENT:
    case SQLCOM_ALTER_EVENT:
    case SQLCOM_CREATE_FUNCTION:
    case SQLCOM_CREATE_PROCEDURE:
    case SQLCOM_CREATE_SPFUNCTION:
    case SQLCOM_ALTER_PROCEDURE:
    case SQLCOM_ALTER_FUNCTION:
    case SQLCOM_DROP_PROCEDURE:
    case SQLCOM_DROP_FUNCTION:
    case SQLCOM_CREATE_VIEW:
    case SQLCOM_DROP_VIEW:
    case SQLCOM_CREATE_TRIGGER:
    case SQLCOM_DROP_TRIGGER:
    {
        *spider_sql = tc_get_only_spider_ddl_withdb(tc_parse_result_t, shard_count);
        break;
    }
    case SQLCOM_CREATE_USER:
    case SQLCOM_DROP_USER:
    case SQLCOM_ALTER_USER:
    case SQLCOM_RENAME_USER:
    case SQLCOM_REVOKE:
    case SQLCOM_GRANT:
    case SQLCOM_CREATE_SERVER:
    case SQLCOM_ALTER_SERVER:
    case SQLCOM_DROP_SERVER:
    {
        *spider_sql = tc_get_only_spider_ddl(tc_parse_result_t, shard_count);
        break;
    }
        /* 3. DDL need dispatch to spider and remote mysql */
    case SQLCOM_CREATE_TABLE:
    {
        *spider_sql = tc_get_spider_create_table(tc_parse_result_t, shard_count, shard_func, shard_type, is_unsigned_key);
        *remote_sql_map = tc_get_remote_create_table(tc_parse_result_t, shard_count);
        break;
    }
    case SQLCOM_ALTER_TABLE:
    {
        *spider_sql = tc_get_spider_alter_table(tc_parse_result_t, shard_count);
        *remote_sql_map = tc_get_remote_alter_table(tc_parse_result_t, shard_count);
        break;
    }
    case SQLCOM_RENAME_TABLE:
    {
        *spider_sql = tc_get_spider_rename_table(tc_parse_result_t, shard_count);
        *remote_sql_map = tc_get_remote_rename_table(tc_parse_result_t, shard_count);
        break;
    }
    case SQLCOM_CREATE_INDEX:
    case SQLCOM_DROP_INDEX:
    {
        *spider_sql = tc_get_spider_create_or_drop_index(tc_parse_result_t, shard_count);
        *remote_sql_map = tc_get_remote_create_or_drop_index(tc_parse_result_t, shard_count);
        break;
    }
    case SQLCOM_DROP_TABLE:
    {
        *spider_sql = tc_get_spider_drop_table(tc_parse_result_t, shard_count);
        *remote_sql_map = tc_get_remote_drop_table(tc_parse_result_t, shard_count);
        break;
    }
    case SQLCOM_CHANGE_DB:
    {
        *spider_sql = tc_get_spider_change_database(tc_parse_result_t, shard_count);
        *remote_sql_map = tc_get_remote_change_database(tc_parse_result_t, shard_count);
        break;
    }
    case SQLCOM_CREATE_DB:
    {
        *spider_sql = tc_get_spider_create_database(tc_parse_result_t, shard_count);
        *remote_sql_map = tc_get_remote_create_database(tc_parse_result_t, shard_count);
        break;
    }
    case SQLCOM_DROP_DB:
    {
        *spider_sql = tc_get_spider_drop_database(tc_parse_result_t, shard_count);
        *remote_sql_map = tc_get_remote_drop_database(tc_parse_result_t, shard_count);
        break;
    }
    case SQLCOM_ALTER_DB:
    {
        *spider_sql = tc_get_spider_drop_database(tc_parse_result_t, shard_count);
        *remote_sql_map = tc_get_remote_drop_database(tc_parse_result_t, shard_count);
        break;
    }
    case TC_SQLCOM_CREATE_TABLE_LIKE:
    {
        *spider_sql = tc_get_spider_create_table_like(tc_parse_result_t, shard_count);
        *remote_sql_map = tc_get_remote_create_table_like(tc_parse_result_t, shard_count);
        break;
    }
    /* 4. tcadmin's management instruction */
    case SQLCOM_RESET:
    case SQLCOM_FLUSH:
    case SQLCOM_KILL:
    case SQLCOM_SHUTDOWN:
        my_error(ER_TCADMIN_UNSUPPORT_SQL_TYPE, MYF(0), get_stmt_type_str(tc_parse_result_t->sql_type));
        return TRUE;
        /* 5. other may be supported int the future */
    case SQLCOM_UNLOCK_TABLES:
    case SQLCOM_LOCK_TABLES:
    case SQLCOM_BEGIN:
    case SQLCOM_COMMIT:
    case SQLCOM_ROLLBACK:
    case SQLCOM_RELEASE_SAVEPOINT:
    case SQLCOM_ROLLBACK_TO_SAVEPOINT:
    case SQLCOM_SAVEPOINT:
    case SQLCOM_ALTER_DB_UPGRADE:
        my_error(ER_TCADMIN_UNSUPPORT_SQL_TYPE, MYF(0), get_stmt_type_str(tc_parse_result_t->sql_type));
        return TRUE;
    case TC_SQLCOM_CREATE_TABLE_WITH_SELECT:
    case TC_SQLCOM_CREATE_TABLE_WITH_CONNECT_STRING:
    case TC_SQLCOM_CREATE_TABLE_WITH_TABLE_COMMENT:
    case TC_SQLCOM_CREATE_TABLE_WITH_FIELD_CHARSET:
    case TC_SQLCOM_CREATE_OR_DROP_UNIQUE_KEY:
        my_error(ER_TCADMIN_UNSUPPORT_SQL_TYPE, MYF(0), get_stmt_type_str(tc_parse_result_t->sql_type));
        return TRUE;
	default:
		break;
    }
    return FALSE;
}


void tc_spider_real_query(
  MYSQL *mysql, 
  string sql, 
  tc_execute_result *exec_result, 
  string ipport
)
{
    int ret = mysql_real_query(mysql, sql.c_str(), sql.length());
    tc_exec_info exec_info;
    exec_info.err_code = 0;
    exec_info.err_msg = "";
    while (!ret)
    {
        ret = tc_mysql_next_result(mysql);
    }
    if (ret != -1)
    {/* error happened */
        exec_info.err_code = mysql_errno(mysql);
        exec_info.err_msg = mysql_error(mysql);
        exec_result->result = TRUE;
    }
    spider_exec_mtx.lock();
    exec_result->spider_result_info.insert(pair<string, tc_exec_info>(ipport, exec_info));
    spider_exec_mtx.unlock();
}


void tc_remote_real_query(
  MYSQL *mysql, 
  string sql, 
  tc_execute_result *exec_result, 
  string ipport
)
{
    int ret = mysql_real_query(mysql, sql.c_str(), sql.length());
    tc_exec_info exec_info;
    exec_info.err_code = 0;
    exec_info.err_msg = "";
    while (!ret)
    {
        ret = tc_mysql_next_result(mysql);
    }
    if (ret != -1)
    {/* error happened */
        exec_info.err_code = mysql_errno(mysql);
        exec_info.err_msg = mysql_error(mysql);
        exec_result->result = TRUE;
    }
    remote_exec_mtx.lock();
    exec_result->remote_result_info.insert(pair<string, tc_exec_info>(ipport, exec_info));
    remote_exec_mtx.unlock();
}


bool tc_spider_ddl_run_paral(
  string before_sql, 
  string spider_sql, 
  map<string, MYSQL*> spider_conn_map, 
  tc_execute_result *exec_result
)
{
    int spider_count = spider_conn_map.size();
    string exec_sql = before_sql + spider_sql;
    tc_exec_info exec_info;
    thread *thread_array = new thread[spider_count];
    map<string, int> ret_map;
    int i = 0;

    map<string, MYSQL*>::iterator its;
    for (its = spider_conn_map.begin(); its != spider_conn_map.end(); its++)
    {
        string ipport = its->first;
        MYSQL *mysql = its->second;
        thread tmp_t(tc_spider_real_query, mysql, exec_sql, exec_result, ipport);
        thread_array[i] = move(tmp_t);
        i++;
    }

    for (int i = 0; i < spider_count; i++)
    {
        if (thread_array[i].joinable())
            thread_array[i].join();
    }

    //for (its = spider_conn_map.begin(); its != spider_conn_map.end(); its++)
    //{
    //    string ipport = its->first;
    //    MYSQL *mysql = &(its->second);
    //    int ret;
    //    exec_info.err_code = 0;
    //    exec_info.err_msg = "";
    //    ret = mysql_errno(mysql);
    //    while (!ret)
    //    {
    //        ret = tc_mysql_next_result(mysql);
    //    }
    //    if (ret != -1)
    //    {/* error happened */
    //        exec_info.err_code = mysql_errno(mysql);
    //        exec_info.err_msg = mysql_error(mysql);
    //        exec_result->result = TRUE;
    //        result = TRUE;
    //    }
    //    exec_result->spider_result_info.insert(pair<string, tc_exec_info>(ipport, exec_info));
    //}
    delete[] thread_array;
    return exec_result->result;
}

bool tc_remotedb_ddl_run_paral(
  string before_sql, 
  map<string, string> remote_sql_map, 
  map<string, MYSQL*> remote_conn_map, 
  map<string, string> remote_ipport_map, 
  tc_execute_result *exec_result
)
{
    int remote_count = remote_conn_map.size();
    tc_exec_info exec_info;
    thread *thread_array = new thread[remote_count];
    map<string, int> ret_map;
    int i = 0;

    if (remote_sql_map.size() == 0)
    {
        exec_info.err_msg = "No Remote DB has been found.";
        exec_result->remote_result_info.insert(pair<string, tc_exec_info>("", exec_info));
        return TRUE;
    }

    map<string, string>::iterator its;
    for (its = remote_ipport_map.begin(); its != remote_ipport_map.end(); its++)
    {
        string server = its->first;
        string ipport = its->second;
        string exec_sql = before_sql + remote_sql_map[server];
        MYSQL *mysql = remote_conn_map[ipport];
        thread tmp_t(tc_remote_real_query, mysql, exec_sql, exec_result, ipport);
        thread_array[i] = move(tmp_t);
        i++;
    }

    for (int i = 0; i < remote_count; i++)
    {
        if (thread_array[i].joinable())
            thread_array[i].join();
    }

    //for (its = remote_ipport_map.begin(); its != remote_ipport_map.end(); its++)
    //{
    //    string server = its->first;
    //    string ipport = its->second;
    //    string exec_sql = before_sql + remote_sql_map[server];
    //    const char *sql = exec_sql.c_str();
    //    MYSQL *mysql = &(remote_conn_map[ipport]);
    //    int ret;
    //    exec_info.err_code = 0;
    //    exec_info.err_msg = "";
    //    ret = mysql_errno(mysql);

    //    while (!ret)
    //    {
    //        ret = tc_mysql_next_result(mysql);
    //    }
    //    if (ret != -1)
    //    {/* error happened */
    //        exec_info.err_code = mysql_errno(mysql);
    //        exec_info.err_msg = mysql_error(mysql);
    //        exec_result->result = TRUE;
    //        result = TRUE;
    //    }
    //    exec_result->remote_result_info.insert(pair<string, tc_exec_info>(ipport, exec_info));
    //}
    delete[] thread_array;
    return exec_result->result;
}


bool tc_ddl_run(
  THD *thd, 
  LEX *lex,
  string before_sql_for_spider, 
  string before_sql_for_remote, 
  string spider_sql, 
  map<string, string> remote_sql_map, 
  tc_execute_result *exec_result
)
{
    bool spider_run_first = tc_spider_run_first(thd, lex);
    exec_result->result = FALSE;
    if (spider_run_first)
    {/* drop table/database/column */
        if (!tc_spider_ddl_run_paral(
                before_sql_for_spider, 
                spider_sql, 
                thd->spider_conn_map, 
                exec_result) || 
             thd->variables.tc_force_execute)
        {
            tc_remotedb_ddl_run_paral(
               before_sql_for_remote, 
               remote_sql_map, 
               thd->remote_conn_map, 
               thd->remote_ipport_map, 
               exec_result );
         }
    }
    else
    {/* other */
        if (!tc_remotedb_ddl_run_paral(
          before_sql_for_remote, 
          remote_sql_map, 
          thd->remote_conn_map, 
          thd->remote_ipport_map, exec_result) || 
         thd->variables.tc_force_execute)
        {
            tc_spider_ddl_run_paral(
              before_sql_for_spider, 
              spider_sql, 
              thd->spider_conn_map, 
              exec_result);
        }
    }
    return FALSE;
}

bool tc_append_before_query(THD *thd, LEX *lex, string &sql_spider, string &sql_remote)
{
        const CHARSET_INFO *charset;
        sql_mode_t tmp_mode;
        LEX_STRING ls;

        /* 1. append set names */
        charset = thd->charset();
        sql_spider = sql_spider + "set names ";
        sql_spider = sql_spider + charset->csname;
        sql_spider = sql_spider + ";";

        /* 2. append sql mode */
        tmp_mode = thd->variables.sql_mode;
        sql_mode_string_representation(thd, tmp_mode, &ls);
        sql_spider = sql_spider + "set sql_mode='";
        sql_spider = sql_spider + ls.str;
        sql_spider = sql_spider + "';";
        
        sql_remote = sql_spider;

        /* 3. append for spider only */
        sql_spider = sql_spider + "set ddl_execute_by_ctl=0;";
        return FALSE;
}


set<string> get_spider_ipport_set(
  MEM_ROOT *mem, 
  map<string, string> &spider_user_map, 
  map<string, string> &spider_passwd_map,
  bool with_slave
)
{
    set<string> ipport_set;
    FOREIGN_SERVER *server;
    ostringstream  sstr;
    list<FOREIGN_SERVER*> server_list;
    string wrapper_name = tdbctl_spider_wrapper_prefix;
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
  map<string, string> &remote_passwd_map
)
{
    map<string, string> ipport_map;
    FOREIGN_SERVER *server, server_buffer;
    ostringstream  sstr;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
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
    string ipport = its->first;
    MYSQL* mysql = its->second;
    thread tmp_t(tc_exec_sql_up, mysql, exec_sql, &result_map[ipport]);
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
    tc_exec_info exec_info = its2->second;
    if (exec_info.err_code > 0)
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
            if (!tc_exec_sql_up(conn_map[ipport], exec_sql, &exec_info))
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
  All spiders should do [GRANT ALL PRIVILEGES] sql for other tdbctl, which use
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
   0: empty mysql.servers or error happened.
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

    init_sql_alloc(key_memory_bases, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
    MEM_ROOT_GUARD(mem_root);
    get_server_by_wrapper(server_list, &mem_root, TDBCTL_WRAPPER, false);

    //empty, error happened
    if (server_list.empty())
      return 0;

    //always user fist Server_name
    server_list.sort(server_compare);
    host = server_list.front()->host;
    *port = server_list.front()->port;
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
    return tdbctl_is_primary;
  }

  return ret;
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
