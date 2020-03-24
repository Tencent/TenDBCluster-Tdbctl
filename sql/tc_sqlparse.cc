#include "sql_base.h"         // open_tables, open_and_lock_tables,
#include "sql_lex.h"
#include "sp_head.h"
#include "tc_sqlparse.h"
#include "sql_servers.h"
#include "mysql.h"
#include "sql_common.h"
#include "m_string.h"
#include "handler.h"
#include "log.h"
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


using namespace std;

mutex remote_exec_mtx;
mutex spider_exec_mtx;


static string tc_dbname_replace(string sql, string spider_db_name, string remote_db_name)
{
    string db_org1 = " " + spider_db_name;
    string db_org2 = "`" + spider_db_name + "`";
    string db_dst1 = " " + remote_db_name;
    string db_dst2 = " `" + remote_db_name + "`";
    regex pattern1(db_org1);
    regex pattern2(db_org2);
    sql = regex_replace(sql, pattern1, db_dst1);
    sql = regex_replace(sql, pattern2, db_dst2);
    return sql;
}

static string tc_dbname_replace_with_point(string sql, string spider_db_name, string remote_db_name)
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
    case SQLCOM_SELECT: return "SQLCOM_SELECT";
    case SQLCOM_CREATE_TABLE: return "SQLCOM_CREATE_TABLE";
    case SQLCOM_CREATE_INDEX: return "SQLCOM_CREATE_INDEX";
    case SQLCOM_ALTER_TABLE: return "SQLCOM_ALTER_TABLE";
    case SQLCOM_UPDATE: return "SQLCOM_UPDATE";
    case SQLCOM_INSERT: return "SQLCOM_INSERT";
    case SQLCOM_INSERT_SELECT: return "SQLCOM_INSERT_SELECT";
    case SQLCOM_DELETE: return "SQLCOM_DELETE";
    case SQLCOM_TRUNCATE: return "SQLCOM_TRUNCATE";
    case SQLCOM_DROP_TABLE: return "SQLCOM_DROP_TABLE";
    case SQLCOM_DROP_INDEX: return "SQLCOM_DROP_INDEX";
    case SQLCOM_SHOW_DATABASES: return "SQLCOM_SHOW_DATABASES";
    case SQLCOM_SHOW_TABLES: return "SQLCOM_SHOW_TABLES";
    case SQLCOM_SHOW_FIELDS: return "SQLCOM_SHOW_FIELDS";
    case SQLCOM_SHOW_KEYS: return "SQLCOM_SHOW_KEYS";
    case SQLCOM_SHOW_VARIABLES: return "SQLCOM_SHOW_VARIABLES";
    case SQLCOM_SHOW_STATUS: return "SQLCOM_SHOW_STATUS";
    case SQLCOM_SHOW_ENGINE_LOGS: return "SQLCOM_SHOW_ENGINE_LOGS";
    case SQLCOM_SHOW_ENGINE_STATUS: return "SQLCOM_SHOW_ENGINE_STATUS";
    case SQLCOM_SHOW_ENGINE_MUTEX: return "SQLCOM_SHOW_ENGINE_MUTEX";
    case SQLCOM_SHOW_PROCESSLIST: return "SQLCOM_SHOW_PROCESSLIST";
    case SQLCOM_SHOW_MASTER_STAT: return "SQLCOM_SHOW_MASTER_STAT";
    case SQLCOM_SHOW_SLAVE_STAT: return "SQLCOM_SHOW_SLAVE_STAT";
    case SQLCOM_SHOW_GRANTS: return "SQLCOM_SHOW_GRANTS";
    case SQLCOM_SHOW_CREATE: return "SQLCOM_SHOW_CREATE";
    case SQLCOM_SHOW_CHARSETS: return "SQLCOM_SHOW_CHARSETS";
    case SQLCOM_SHOW_COLLATIONS: return "SQLCOM_SHOW_COLLATIONS";
    case SQLCOM_SHOW_CREATE_DB: return "SQLCOM_SHOW_CREATE_DB";
    case SQLCOM_SHOW_TABLE_STATUS: return "SQLCOM_SHOW_TABLE_STATUS";
    case SQLCOM_SHOW_TRIGGERS: return "SQLCOM_SHOW_TRIGGERS";
    case SQLCOM_LOAD: return "SQLCOM_LOAD";
    case SQLCOM_SET_OPTION: return "SQLCOM_SET_OPTION";
    case SQLCOM_LOCK_TABLES: return "SQLCOM_LOCK_TABLES";
    case SQLCOM_UNLOCK_TABLES: return "SQLCOM_UNLOCK_TABLES";
    case SQLCOM_GRANT: return "SQLCOM_GRANT";
    case SQLCOM_CHANGE_DB: return "SQLCOM_CHANGE_DB";
    case SQLCOM_CREATE_DB: return "SQLCOM_CREATE_DB";
    case SQLCOM_DROP_DB: return "SQLCOM_DROP_DB";
    case SQLCOM_ALTER_DB: return "SQLCOM_ALTER_DB";
    case SQLCOM_REPAIR: return "SQLCOM_REPAIR";
    case SQLCOM_REPLACE: return "SQLCOM_REPLACE";
    case SQLCOM_REPLACE_SELECT: return "SQLCOM_REPLACE_SELECT";
    case SQLCOM_CREATE_FUNCTION: return "SQLCOM_CREATE_FUNCTION";
    case SQLCOM_DROP_FUNCTION: return "SQLCOM_DROP_FUNCTION";
    case SQLCOM_REVOKE: return "SQLCOM_REVOKE";
    case SQLCOM_OPTIMIZE: return "SQLCOM_OPTIMIZE";
    case SQLCOM_CHECK: return "SQLCOM_CHECK";
    case SQLCOM_ASSIGN_TO_KEYCACHE: return "SQLCOM_ASSIGN_TO_KEYCACHE";
    case SQLCOM_PRELOAD_KEYS: return "SQLCOM_PRELOAD_KEYS";
    case SQLCOM_FLUSH: return "SQLCOM_FLUSH";
    case SQLCOM_KILL: return "SQLCOM_KILL";
    case SQLCOM_ANALYZE: return "SQLCOM_ANALYZE";
    case SQLCOM_ROLLBACK: return "SQLCOM_ROLLBACK";
    case SQLCOM_ROLLBACK_TO_SAVEPOINT: return "SQLCOM_ROLLBACK_TO_SAVEPOINT";
    case SQLCOM_COMMIT: return "SQLCOM_COMMIT";
    case SQLCOM_SAVEPOINT: return "SQLCOM_SAVEPOINT";
    case SQLCOM_RELEASE_SAVEPOINT: return "SQLCOM_RELEASE_SAVEPOINT";
    case SQLCOM_SLAVE_START: return "SQLCOM_SLAVE_START";
    case SQLCOM_SLAVE_STOP: return "SQLCOM_SLAVE_STOP";
    case SQLCOM_BEGIN: return "SQLCOM_BEGIN";
    case SQLCOM_CHANGE_MASTER: return "SQLCOM_CHANGE_MASTER";
    case SQLCOM_RENAME_TABLE: return "SQLCOM_RENAME_TABLE";
    case SQLCOM_RESET: return "SQLCOM_RESET";
    case SQLCOM_PURGE: return "SQLCOM_PURGE";
    case SQLCOM_PURGE_BEFORE: return "SQLCOM_PURGE_BEFORE";
    case SQLCOM_SHOW_BINLOGS: return "SQLCOM_SHOW_BINLOGS";
    case SQLCOM_SHOW_OPEN_TABLES: return "SQLCOM_SHOW_OPEN_TABLES";
    case SQLCOM_HA_OPEN: return "SQLCOM_HA_OPEN";
    case SQLCOM_HA_CLOSE: return "SQLCOM_HA_CLOSE";
    case SQLCOM_HA_READ: return "SQLCOM_HA_READ";
    case SQLCOM_SHOW_SLAVE_HOSTS: return "SQLCOM_SHOW_SLAVE_HOSTS";
    case SQLCOM_DELETE_MULTI: return "SQLCOM_DELETE_MULTI";
    case SQLCOM_UPDATE_MULTI: return "SQLCOM_UPDATE_MULTI";
    case SQLCOM_SHOW_BINLOG_EVENTS: return "SQLCOM_SHOW_BINLOG_EVENTS";
    case SQLCOM_DO: return "SQLCOM_DO";
    case SQLCOM_SHOW_WARNS: return "SQLCOM_SHOW_WARNS";
    case SQLCOM_EMPTY_QUERY: return "SQLCOM_EMPTY_QUERY";
    case SQLCOM_SHOW_ERRORS: return "SQLCOM_SHOW_ERRORS";
    case SQLCOM_SHOW_STORAGE_ENGINES: return "SQLCOM_SHOW_STORAGE_ENGINES";
    case SQLCOM_SHOW_PRIVILEGES: return "SQLCOM_SHOW_PRIVILEGES";
    case SQLCOM_HELP: return "SQLCOM_HELP";
    case SQLCOM_CREATE_USER: return "SQLCOM_CREATE_USER";
    case SQLCOM_DROP_USER: return "SQLCOM_DROP_USER";
    case SQLCOM_RENAME_USER: return "SQLCOM_RENAME_USER";
    case SQLCOM_REVOKE_ALL: return "SQLCOM_REVOKE_ALL";
    case SQLCOM_CHECKSUM: return "SQLCOM_CHECKSUM";
    case SQLCOM_CREATE_PROCEDURE: return "SQLCOM_CREATE_PROCEDURE";
    case SQLCOM_CREATE_SPFUNCTION: return "SQLCOM_CREATE_SPFUNCTION";
    case SQLCOM_CALL: return "SQLCOM_CALL";
    case SQLCOM_DROP_PROCEDURE: return "SQLCOM_DROP_PROCEDURE";
    case SQLCOM_ALTER_PROCEDURE: return "SQLCOM_ALTER_PROCEDURE";
    case SQLCOM_ALTER_FUNCTION: return "SQLCOM_ALTER_FUNCTION";
    case SQLCOM_SHOW_CREATE_PROC: return "SQLCOM_SHOW_CREATE_PROC";
    case SQLCOM_SHOW_CREATE_FUNC: return "SQLCOM_SHOW_CREATE_FUNC";
    case SQLCOM_SHOW_STATUS_PROC: return "SQLCOM_SHOW_STATUS_PROC";
    case SQLCOM_SHOW_STATUS_FUNC: return "SQLCOM_SHOW_STATUS_FUNC";
    case SQLCOM_PREPARE: return "SQLCOM_PREPARE";
    case SQLCOM_EXECUTE: return "SQLCOM_EXECUTE";
    case SQLCOM_DEALLOCATE_PREPARE: return "SQLCOM_DEALLOCATE_PREPARE";
    case SQLCOM_CREATE_VIEW: return "SQLCOM_CREATE_VIEW";
    case SQLCOM_DROP_VIEW: return "SQLCOM_DROP_VIEW";
    case SQLCOM_CREATE_TRIGGER: return "SQLCOM_CREATE_TRIGGER";
    case SQLCOM_DROP_TRIGGER: return "SQLCOM_DROP_TRIGGER";
    case SQLCOM_XA_START: return "SQLCOM_XA_START";
    case SQLCOM_XA_END: return "SQLCOM_XA_END";
    case SQLCOM_XA_PREPARE: return "SQLCOM_XA_PREPARE";
    case SQLCOM_XA_COMMIT: return "SQLCOM_XA_COMMIT";
    case SQLCOM_XA_ROLLBACK: return "SQLCOM_XA_ROLLBACK";
    case SQLCOM_XA_RECOVER: return "SQLCOM_XA_RECOVER";
    case SQLCOM_SHOW_PROC_CODE: return "SQLCOM_SHOW_PROC_CODE";
    case SQLCOM_SHOW_FUNC_CODE: return "SQLCOM_SHOW_FUNC_CODE";
    case SQLCOM_ALTER_TABLESPACE: return "SQLCOM_ALTER_TABLESPACE";
    case SQLCOM_INSTALL_PLUGIN: return "SQLCOM_INSTALL_PLUGIN";
    case SQLCOM_UNINSTALL_PLUGIN: return "SQLCOM_UNINSTALL_PLUGIN";
    case SQLCOM_BINLOG_BASE64_EVENT: return "SQLCOM_BINLOG_BASE64_EVENT";
    case SQLCOM_SHOW_PLUGINS: return "SQLCOM_SHOW_PLUGINS";
    case SQLCOM_CREATE_SERVER: return "SQLCOM_CREATE_SERVER";
    case SQLCOM_DROP_SERVER: return "SQLCOM_DROP_SERVER";
    case SQLCOM_ALTER_SERVER: return "SQLCOM_ALTER_SERVER";
    case SQLCOM_CREATE_EVENT: return "SQLCOM_CREATE_EVENT";
    case SQLCOM_ALTER_EVENT: return "SQLCOM_ALTER_EVENT";
    case SQLCOM_DROP_EVENT: return "SQLCOM_DROP_EVENT";
    case SQLCOM_SHOW_CREATE_EVENT: return "SQLCOM_SHOW_CREATE_EVENT";
    case SQLCOM_SHOW_EVENTS: return "SQLCOM_SHOW_EVENTS";
    case SQLCOM_SHOW_CREATE_TRIGGER: return "SQLCOM_SHOW_CREATE_TRIGGER";
    case SQLCOM_ALTER_DB_UPGRADE: return "SQLCOM_ALTER_DB_UPGRADE";
    case SQLCOM_SHOW_PROFILE: return "SQLCOM_SHOW_PROFILE";
    case SQLCOM_SHOW_PROFILES: return "SQLCOM_SHOW_PROFILES";
    case SQLCOM_SIGNAL: return "SQLCOM_SIGNAL";
    case SQLCOM_RESIGNAL: return "SQLCOM_RESIGNAL";
    case SQLCOM_SHOW_RELAYLOG_EVENTS: return "SQLCOM_SHOW_RELAYLOG_EVENTS";
    case SQLCOM_GET_DIAGNOSTICS: return "SQLCOM_GET_DIAGNOSTICS";
    case SQLCOM_SHUTDOWN: return "SQLCOM_SHUTDOWN";
    case SQLCOM_ALTER_USER: return "SQLCOM_ALTER_USER";
    case SQLCOM_SHOW_CREATE_USER: return "SQLCOM_SHOW_CREATE_USER";
    case SQLCOM_ALTER_INSTANCE: return "SQLCOM_ALTER_INSTANCE";
    case SQLCOM_CHANGE_REPLICATION_FILTER: return "SQLCOM_CHANGE_REPLICATION_FILTER";
    case SQLCOM_CREATE_COMPRESSION_DICTIONARY: return "SQLCOM_CREATE_COMPRESSION_DICTIONARY";
    case SQLCOM_DROP_COMPRESSION_DICTIONARY: return "SQLCOM_DROP_COMPRESSION_DICTIONARY";
    case SQLCOM_EXPLAIN_OTHER: return "SQLCOM_EXPLAIN_OTHER";
    case SQLCOM_LOCK_BINLOG_FOR_BACKUP: return "SQLCOM_LOCK_BINLOG_FOR_BACKUP";
    case SQLCOM_LOCK_TABLES_FOR_BACKUP: return "SQLCOM_LOCK_TABLES_FOR_BACKUP";
    case SQLCOM_SHOW_CLIENT_STATS: return "SQLCOM_SHOW_CLIENT_STATS";
    case SQLCOM_SHOW_INDEX_STATS: return "SQLCOM_SHOW_INDEX_STATS";
    case SQLCOM_SHOW_TABLE_STATS: return "SQLCOM_SHOW_TABLE_STATS";
    case SQLCOM_SHOW_THREAD_STATS: return "SQLCOM_SHOW_THREAD_STATS";
    case SQLCOM_SHOW_USER_STATS: return "SQLCOM_SHOW_USER_STATS";
    case SQLCOM_START_GROUP_REPLICATION: return "SQLCOM_START_GROUP_REPLICATION";
    case SQLCOM_STOP_GROUP_REPLICATION: return "SQLCOM_STOP_GROUP_REPLICATION";
    case SQLCOM_UNLOCK_BINLOG: return "SQLCOM_UNLOCK_BINLOG";
    case TC_SQLCOM_CREATE_TABLE_WITH_SELECT: return "TC_SQLCOM_CREATE_TABLE_WITH_SELECT";
    case TC_SQLCOM_CREATE_TABLE_WITH_CONNECT_STRING: return "TC_SQLCOM_CREATE_TABLE_WITH_CONNECT_STRING";
    case TC_SQLCOM_CREATE_TABLE_WITH_TABLE_COMMENT: return "TC_SQLCOM_CREATE_TABLE_WITH_TABLE_COMMENT";
    case TC_SQLCOM_CREATE_TABLE_WITH_FIELD_CHARSET: return "TC_SQLCOM_CREATE_TABLE_WITH_FIELD_CHARSET";
    case TC_SQLCOM_CREATE_TABLE_LIKE: return "TC_SQLCOM_CREATE_TABLE_LIKE";
    case TC_SQLCOM_CREATE_OR_DROP_UNIQUE_KEY: return "TC_SQLCOM_CREATE_OR_DROP_UNIQUE_KEY";
    case TC_SQLCOM_ALTER_TABLE_UNSUPPORT: return "TC_SQLCOM_ALTER_TABLE_UNSUPPORT";
    default:
        return "unkonw type";
    }
    return "";
}



void gettype_create_filed(Create_field *cr_field, String &res)
{
    const CHARSET_INFO *cs = res.charset();
    ulonglong field_length = cr_field->length;
    ulong length;
    bool unsigned_flag = cr_field->flags & UNSIGNED_FLAG;
    bool zerofill_flag = cr_field->flags & ZEROFILL_FLAG;
    ulonglong tmp = field_length;

    switch (cr_field->field->type())
    {
    case MYSQL_TYPE_DECIMAL:
        tmp = cr_field->length;
        if (!unsigned_flag)
            tmp--;
        if (cr_field->decimals)
            tmp--;
        res.length(cs->cset->snprintf(cs, (char*)res.ptr(), res.alloced_length(),
            "decimal(%d,%d)", tmp, cr_field->decimals));
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
            "int(%d)", field_length));
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
            "decimal(%ld,%d)", cr_field->length - (cr_field->decimals>0 ? 1 : 0) - (unsigned_flag || !cr_field->length ? 0 : 1),
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
        const char *str;
        uint length;
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
            /* TODO,   这处的mbmaxlen是什么含义 ？！ varchar(10)的时候，mbmaxlen为3，为什么 ？
            length= cs->cset->snprintf(cs,(char*) res.ptr(), res.alloced_length(), "%s(%d)",
            ((cr_field->charset && cr_field->charset->csname) ? "varchar" : "varbinary"),
            (int) field_length / cr_field->charset->mbmaxlen);
            */
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

// buf_len means length of key_name ... result, etc
bool tc_parse_getkey_for_spider(THD *thd, char *key_name, char *result, int buf_len, bool *is_unique_key)
{
    LEX* lex = thd->lex;
    TABLE_LIST* table_list = lex->query_tables;
    List_iterator<Create_field> it_field = lex->alter_info.create_list;
    Create_field *field;
    List_iterator<Key> key_iterator(lex->alter_info.key_list);
    Key *key;
    //const char *shard_key_str = "AS TSPIDER SHARD KEY";
    bool has_shard_key = false;
    Key_part_spec *column;
    int is_key_part = 0; 
    int level = 0;  // first part of the common key，level is 1;  first part of the unique key，level is 2; first part of the primary key, level is 3
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
            while (field = list_field++)
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
    while (key = key_iterator++)
    {
        List_iterator<Key_part_spec> cols(key->columns);
        column = cols++;

        switch (key->type)
        {
        case keytype::KEYTYPE_PRIMARY:
        case keytype::KEYTYPE_UNIQUE:
        {

            if (has_shard_key)
            {/* 存在shard_key, 可以是多个唯一键的共同部分; 如果不在某个唯一键中，则报错 */
                int has_flag = 0;
                Key_part_spec *tmp_column;
                cols.rewind();
                while (tmp_column = cols++)
                {
                    if (!strcmp(key_name, tmp_column->field_name.str))
                    {
                        has_flag = 1;
                        is_key_part = 1;
                    }
                }
                if (!has_flag)
                {/* 如果不是某个唯一键的一部分 */
                    snprintf(result, buf_len, "ERROR: %s as TSpider key, but not in some unique key", key_name);
                    strcpy(key_name, "");
                    return TRUE;
                }
            }
            else
            {
                if (level > 1 && strcmp(key_name, column->field_name.str))
                {// 多个unique, 如果前缀不同则报错
                    snprintf(result, buf_len, "%s", "ERROR: too more unique key with the different pre key");
                    strcpy(key_name, "");
                    return 1;
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
            {/* 不存在指定的shard key，且前面无unique，则在key中取一个做为partition key */
                strcpy(key_name, column->field_name.str);
                level = 1;
            }

            if (has_shard_key)
            {/* 存在shard_key, 是不是普通索引的一部分 */
                Key_part_spec *tmp_column;
                cols.rewind();
                while (tmp_column = cols++)
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

    /* 如果只有普通key，且没有显示指定shard key， 却key个数大于1，报错 */
    if (!has_shard_key && level == 1 && lex->alter_info.key_list.elements > 1)
    {
        //strcpy(key_name, "");  // key_name为第一个key
        snprintf(result, buf_len, "%s", "ERROR: too many key more than 1, but without unique key or set shard key");
        return TRUE;
    }

    if (has_shard_key && level <= 1 && is_key_part == 0)
    {/* 指定shard key,不存在唯一键，但shard_key却不是普通索引的一部分 */
        snprintf(result, buf_len, "%s", "ERROR: shard_key must be part of key");
        return TRUE;
    }

    it_field.rewind();
    while ((has_shard_key || level == 1 || level == 2) && !!(field = it_field++))
    {/* key对应的字段必须指定为not null, 主键默认是not null的，因此不需要考虑； flag记录的只是建表语句中的option信息 */
        uint flags = field->flags;
        if (!strcmp(field->field_name, key_name) && !(flags & NOT_NULL_FLAG))
        {
            snprintf(result, buf_len, "%s", "ERROR: the key must default not null");
            return TRUE;
        }
    }


    // 指定了shard_key或者包含索引
    if (has_shard_key || level > 0)
        return FALSE;

    strcpy(key_name, "");
    snprintf(result, buf_len, "%s", "ERROR: no key");
    return TRUE;
}


bool is_add_or_drop_unique_key(THD *thd, LEX *lex)
{
    TABLE_LIST* table_list = lex->query_tables;
    List_iterator<Key> key_iterator(lex->alter_info.key_list);
    List_iterator<Alter_drop> it_drop_field = lex->alter_info.drop_list;
    Alter_drop *alter_drop_field;
    Key *key;
    ulonglong flags = lex->alter_info.flags;
    if (flags & Alter_info::ALTER_ADD_INDEX)
    {
        while (key = key_iterator++)
        {
            if (key->type == keytype::KEYTYPE_PRIMARY || key->type == keytype::KEYTYPE_UNIQUE)
            {
                return true;
            }
        }
    }
    if (flags & Alter_info::ALTER_DROP_INDEX)
    {
        while (alter_drop_field = it_drop_field++)
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

string tc_get_spider_create_table(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    ostringstream  sstr;
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
    string spider_create_sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string hash_key = tc_parse_result_t->shard_key;
    string db_name = tc_parse_result_t->db_name;
    string tb_name = tc_parse_result_t->table_name;
    string connection_string;
    string partiton_hash_by = " partition by list(crc32(";
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
    partiton_hash_by = partiton_hash_by + "`" + hash_key + "`" + ")%" + spider_partition_count_str + ") (";

    spider_create_sql = spider_create_sql + partiton_hash_by;
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
            + db_name + "_" + hash_value + "\", table \"" + tb_name + "\", " + server_info +  "\' ENGINE = SPIDER";

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


string tc_get_spider_create_table_like(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
{
    string sql(tc_parse_result_t->query_string.str, tc_parse_result_t->query_string.length);
    string server_name_pre = tdbctl_mysql_wrapper_prefix;
    string db_name = tc_parse_result_t->db_name;
    string table_name = tc_parse_result_t->table_name;
    string new_db = tc_parse_result_t->new_db_name;
    string new_table = tc_parse_result_t->new_table_name;
    ostringstream  sstr;
    string reorganize_partition_sql = "";
    string  partition_sql = "";
    sql = sql + "; alter table " + db_name + "." + table_name + " reorganize partition ";

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
            + db_name + "_" + hash_value + "\", table \"" + table_name + "\", " + server_info + "\' ENGINE = SPIDER";

        if (i < shard_count - 1)
        {
            reorganize_partition_sql = reorganize_partition_sql + "pt" + hash_value + ",";
            pt_sql = pt_sql + ",";
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

map<string, string> tc_get_remote_create_table_like(TC_PARSE_RESULT *tc_parse_result_t, int shard_count)
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



bool tc_query_parse(THD *thd, LEX *lex, TC_PARSE_RESULT *tc_parse_result_t)
{
    return TRUE;
}


/* convert common query to spider/remotedb query */
bool tc_query_convert(THD *thd, LEX *lex, TC_PARSE_RESULT *tc_parse_result_t, int shard_count, string *spider_sql, map<string, string> *remote_sql_map)
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
        my_error(ER_TCADMIN_UNSUPPORT_SQL_TYPE, MYF(0), get_stmt_type_str(tc_parse_result_t->sql_type));
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
        *spider_sql = tc_get_spider_create_table(tc_parse_result_t, shard_count);
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
    }
    return FALSE;
}


void tc_spider_real_query(MYSQL *mysql, string sql, tc_execute_result *exec_result, string ipport)
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


void tc_remote_real_query(MYSQL *mysql, string sql, tc_execute_result *exec_result, string ipport)
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


bool tc_spider_ddl_run_paral(string before_sql, string spider_sql, 
                             map<string, MYSQL*> spider_conn_map, 
                             tc_execute_result *exec_result)
{
    int spider_count = spider_conn_map.size();
    string exec_sql = before_sql + spider_sql;
    tc_exec_info exec_info;
    thread *thread_array = new thread[spider_count];
    map<string, int> ret_map;
    int i = 0;
    bool result = FALSE;

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
    return result;
}

bool tc_remotedb_ddl_run_paral(string before_sql, map<string, string> remote_sql_map, 
                               map<string, MYSQL*> remote_conn_map, map<string, string> remote_ipport_map, 
                               tc_execute_result *exec_result)
{
    int remote_count = remote_conn_map.size();
    tc_exec_info exec_info;
    thread *thread_array = new thread[remote_count];
    map<string, int> ret_map;
    int i = 0;
    bool result = FALSE;

    if (remote_sql_map.size() == 0)
        return result;

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
    return result;
}


bool tc_ddl_run(THD *thd, string before_sql_for_spider, string before_sql_for_remote, 
                string spider_sql, map<string, string> remote_sql_map, 
                tc_execute_result *exec_result)
{
    bool spider_run_first = FALSE;
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
        if (!tc_remotedb_ddl_run_paral(before_sql_for_remote, remote_sql_map, thd->remote_conn_map, thd->remote_ipport_map, exec_result) || 
            thd->variables.tc_force_execute)
        {
            tc_spider_ddl_run_paral(before_sql_for_spider, spider_sql, thd->spider_conn_map, exec_result);
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
        if (server = get_server_by_name(mem, server_name.c_str(), &server_buffer))
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
    ulong connect_retry_interval = 1000;
    uint real_connect_option = 0;
    MYSQL* mysql;

    while (connect_retry_count-- > 0)
    {
        mysql = mysql_init(NULL);
        mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &read_timeout);
        mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &write_timeout);
        mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
        real_connect_option = CLIENT_INTERACTIVE | CLIENT_MULTI_STATEMENTS;
        if (!mysql_real_connect(mysql, hosts.c_str(), user.c_str(), passwd.c_str(), "", port, NULL, real_connect_option))
        {
            sql_print_warning("tc connnect fail: error code is %d, error message: %s", mysql_errno(mysql), mysql_error(mysql));
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

map<string, MYSQL*> tc_spider_conn_connect(int &ret, set<string> spider_ipport_set, 
                                          map<string, string> spider_user_map, 
                                          map<string, string> spider_passwd_map)
{
    map<int, string> ipport_map;
    map<string, MYSQL*> conn_map;
    set<string>::iterator its;
    int read_timeout = 600;
    int write_timeout = 600;
    int connect_timeout = 60;
    for (its = spider_ipport_set.begin(); its != spider_ipport_set.end(); its++)
    {// ipport_c must like 1.1.1.1#3306
        string ipport = (*its);
        MYSQL* mysql;
        if(mysql = tc_conn_connect(ipport, spider_user_map[ipport], spider_passwd_map[ipport])) 
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


map<string, MYSQL*> tc_remote_conn_connect(int &ret, map<string, string> remote_ipport_map, map<string, string> remote_user_map, map<string, string> remote_passwd_map)
{
    map<int, string> ipport_map;
    map<string, MYSQL*> conn_map;
    map<string, string>::iterator its2;
    int read_timeout = 600;
    int write_timeout = 600;
    int connect_timeout = 60;

    for (its2 = remote_ipport_map.begin(); its2 != remote_ipport_map.end(); its2++)
    {
        string ipport = its2->second;
        ulong pos = ipport.find("#");
        string hosts = ipport.substr(0, pos);
        string ports = ipport.substr(pos + 1);
        uint port = atoi(ports.c_str());
        MYSQL* mysql;
        if (mysql = tc_conn_connect(ipport, remote_user_map[ipport], remote_passwd_map[ipport]))
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
    return FALSE;
}
