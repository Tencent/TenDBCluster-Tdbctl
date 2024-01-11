/*
  Copyright (C) 2020 THL A29 Limited, a Tencent company. All rights reserved.
*/
#include <map>
#include <string>
#include <vector>

#include "auth_common.h"
#include "sql_class.h"
#include "sql_db.h"
#include "sql_show.h" // IS_COLUMNS_* indices

#include "tc_base.h"

using std::string;
using std::map;

/* Select all fields for scalability concerns */
#define SQL_SELECT_IS_COLUMNS                                                  \
  "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' AND "      \
  "TABLE_NAME='%s'"
#define SQL_SELECT_IS_TABLES                                                   \
  "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' AND "       \
  "TABLE_NAME='%s'"

/* Get table names for multi-table checks */
#define SQL_GET_ALL_TABLES_IN_DB                                               \
  "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s'"
#define SQL_GET_TABLES_IN_DB_LIKE                                              \
  "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' "  \
  "AND TABLE_NAME LIKE '%s'"

#define IS_TABLES_TABLE_SCHEMA          1
#define IS_TABLES_TABLE_NAME            2
#define IS_TABLES_TABLE_ENGINE          4
#define IS_TABLES_TABLE_COLLATION      17

#define EMPTY_STRING std::string()
#define DATA_STRING(A, B, C) ((A)[(C)] ? std::string((A)[(C)], (B)[(C)]) : EMPTY_STRING)

#define COLUMN_DIFF_COLUMN_NAME           (1 << 0)
#define COLUMN_DIFF_ORDINAL_POSITION      (1 << 1)
#define COLUMN_DIFF_DATA_TYPE             (1 << 2)
#define COLUMN_DIFF_COLUMN_LENGTH         (1 << 3)
#define COLUMN_DIFF_COLLATION             (1 << 4)
#define COLUMN_DIFF_COLUMN_KEY            (1 << 5)

struct Column_record;
struct Table_info;

typedef std::map<std::string, Column_record> Column_records;

static std::string fix_db_name(const std::string &db_name,
                               const std::string &server_name,
                               enum_node_type node_type);
static bool get_column_records_for_server(THD *thd, const std::string &db_name,
                                          const std::string &table_name,
                                          const std::string &server_name,
                                          MYSQL *mysql, Column_records &records,
                                          enum_node_type node_type);
static bool get_table_info_for_server(THD *thd, const std::string &db_name,
                                      const std::string &table_name,
                                      const std::string &server_name,
                                      MYSQL *mysql, Table_info &info,
                                      enum_node_type node_type);
static bool fill_tables_list(MYSQL *conn, const char *db, String *wild,
                            List<std::string> &tables);
static bool do_send_metadata(THD *thd);
static bool do_check_one_table(THD *thd, Cluster_conn_manager *conn_mgr,
                               const string &db_name, const string &table_name, bool do_send);
static bool validate_db_grants(THD *thd, const char *db);

static const int max_message_length = 512;

/*
  Only store some selected fields that are considered important.
*/
struct Column_record {
  string table_schema;
  string table_name;
  string column_name;
  ulong ordinal_position;
  string data_type;
  string collation_name;
  string column_key;
};

struct Table_info {
  bool exists;
  string table_schema;
  string table_name;
  string table_engine;
  string table_collation;
};

static ulonglong column_record_cmp(const Column_record &a,
                                   const Column_record &b) {
  ulonglong ret = 0;
  if (a.column_name != b.column_name)
    ret |= COLUMN_DIFF_COLUMN_NAME;
  if (a.ordinal_position != b.ordinal_position)
    ret |= COLUMN_DIFF_ORDINAL_POSITION;
  if (a.data_type != b.data_type)
    ret |= COLUMN_DIFF_DATA_TYPE;
  if (a.collation_name != b.collation_name)
    ret |= COLUMN_DIFF_COLLATION;
  if (a.column_key != b.column_key)
    ret |= COLUMN_DIFF_COLUMN_KEY;
  return ret;
}

#define write_column_diff_error(W, F, ...)                                     \
  do {                                                                         \
    (W).write(Message_writer::MSG_LEVEL_ERROR, "Field '%s': " F, __VA_ARGS__); \
  } while (0)

struct Message_writer {
  enum enum_msg_level {
    MSG_LEVEL_INFO = 0,
    MSG_LEVEL_WARN = 1,
    MSG_LEVEL_ERROR = 2
  };

  Message_writer() : message_str(), idx(0), error(false) {}

  void write(enum_msg_level level, const char *fmt, ...) {
    char buff[512];
    va_list args;

    if (level == MSG_LEVEL_ERROR) error = true;

    va_start(args, fmt);
    vsnprintf(buff, sizeof(buff), fmt, args);
    va_end(args);

    message_str.append("(");
    message_str.append(std::to_string(++idx));
    message_str.append(")");
    message_str.append(buff);
  }

  const char *raw_str() const { return message_str.c_str(); }
  size_t length() const { return message_str.length(); }
  bool has_error() const { return error; }

private:
  string message_str;
  int idx;
  bool error;
};

class Table_MDL_lock_guard {
public:
  Table_MDL_lock_guard(THD *thd, const std::string &db,
                       const std::string &table)
      : m_thd(thd) {
    string name = db + "#" + table;

    MDL_REQUEST_INIT(&mdl_request, MDL_key::USER_LEVEL_LOCK, "", name.c_str(),
                     MDL_SHARED, MDL_STATEMENT);
    locked = !m_thd->mdl_context.acquire_lock(&mdl_request,
                                              thd->variables.lock_wait_timeout);
  }

  ~Table_MDL_lock_guard() {
    if (locked) {
      m_thd->mdl_context.release_all_locks_for_name(mdl_request.ticket);
    }
  }

  bool lock_successful() const { return locked; }

private:
  Table_MDL_lock_guard() {}

  bool locked;

  THD *m_thd;

  MDL_request mdl_request;
};

static string fix_db_name(const string &db_name, const string &server_name,
                          enum_node_type node_type) {
  string ret = db_name;
  if (node_type == NODE_TYPE_REMOTE || node_type == NODE_TYPE_REMOTE_SLAVE) {
    uint prefix_len = strlen(tdbctl_mysql_wrapper_prefix);
    /* Append the numeric suffix of this server */
    ret.push_back('_');
    ret.append(server_name.substr(prefix_len));
  }
  return ret;
}

static bool get_column_records_for_server(THD *thd,
                                          const string &db_name,
                                          const string &table_name, const string &server_name,
                                          MYSQL *mysql,
                                          Column_records &records,
                                          enum_node_type node_type) {
  char query_buff[512];
  MYSQL_ROW row;
  MYSQL_RES *res;
  Cluster_conn_manager *conn_mgr;
  Query_exec_manager query_mgr(thd);
  DBUG_ENTER("get_column_records_for_server");

  snprintf(query_buff, sizeof(query_buff), SQL_SELECT_IS_COLUMNS, db_name.c_str(), table_name.c_str());

  conn_mgr = thd->cluster_conn_manager;
  query_mgr.build_server_maps(conn_mgr);
  query_mgr.store_exec_query(server_name, string(query_buff), node_type);
  query_mgr.reset_error();

  tc_real_query(&query_mgr, server_name, mysql, node_type);
  if (query_mgr.get_error() || !(res = mysql_store_result(mysql))) {
    DBUG_RETURN(TRUE);
  }
  
  records.clear();
  while ((row = mysql_fetch_row(res))) {
    Column_record rec;
    rec.table_schema = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_TABLE_SCHEMA);
    rec.table_name = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_TABLE_NAME);
    rec.column_name = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_COLUMN_NAME);
    rec.ordinal_position = strtoul(row[IS_COLUMNS_ORDINAL_POSITION], NULL, 10);
    rec.data_type = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_DATA_TYPE);
    rec.collation_name = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_COLLATION_NAME);
    rec.column_key = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_COLUMN_KEY);
    records[rec.column_name] = rec;
  }
  mysql_free_result(res);

  DBUG_RETURN(FALSE);
}

static bool get_table_info_for_server(THD *thd, const string &db_name,
                                        const string &table_name,
                                        const string &server_name,
                                        MYSQL *mysql,
                                        Table_info &info,
                                        enum_node_type node_type) {
  char query_buff[512];
  MYSQL_ROW row;
  MYSQL_RES *res;
  Cluster_conn_manager *conn_mgr;
  Query_exec_manager query_mgr(thd);
  DBUG_ENTER("get_table_info_for_server");

  snprintf(query_buff, sizeof(query_buff), SQL_SELECT_IS_TABLES,
           db_name.c_str(), table_name.c_str());

  conn_mgr = thd->cluster_conn_manager;
  query_mgr.build_server_maps(conn_mgr);
  query_mgr.store_exec_query(server_name, string(query_buff), node_type);
  query_mgr.reset_error();

  tc_real_query(&query_mgr, server_name, mysql, node_type);
  if (query_mgr.get_error() || !(res = mysql_store_result(mysql))) {
    DBUG_RETURN(TRUE);
  }

  info.exists = FALSE;
  if ((row = mysql_fetch_row(res))) {
    info.exists = TRUE;
    info.table_schema = DATA_STRING(row, mysql_fetch_lengths(res), IS_TABLES_TABLE_SCHEMA);
    info.table_name = DATA_STRING(row, mysql_fetch_lengths(res), IS_TABLES_TABLE_NAME);
    info.table_engine = DATA_STRING(row, mysql_fetch_lengths(res), IS_TABLES_TABLE_ENGINE);
    info.table_collation = DATA_STRING(row, mysql_fetch_lengths(res), IS_TABLES_TABLE_COLLATION);
  }
  mysql_free_result(res);

  DBUG_RETURN(FALSE);
}

/**
 * Send metadata for check-table results.
 *
 * Fields:
 *  - SERVER_NAME   VARCHAR(64)         Server's identifier
 *  - DB            VARCHAR(64)         Corresponding db on the server
 *  - TABLE         VARCHAR(64*3*2)     Corresponding table on the server
 *  - STATUS        CHAR(10)            Check result: [OK, Error]
 *  - MESSAGE       VARCHAR(512)        Error message
 *
 * @retval True on error, False on success
 * */
static bool do_send_metadata(THD *thd) {
  List<Item> field_list;
  Item *item;
  DBUG_ENTER("do_send_metadata");

  field_list.push_back(item =
                           new Item_empty_string("Server_name", NAME_CHAR_LEN));
  field_list.push_back(item = new Item_empty_string("Db", NAME_CHAR_LEN));
  field_list.push_back(item = new Item_empty_string("Table", NAME_LEN * 2));
  field_list.push_back(item = new Item_empty_string("Status", 10));
  field_list.push_back(
      item = new Item_empty_string("Message", max_message_length));
  if (thd->send_result_metadata(&field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(TRUE);

  DBUG_RETURN(FALSE);
}

/**
 * Get names of tables in specified database that match the filter.
 *
 * @param conn: Connection to this Tdbctl server
 * @param db: Specified database
 * @param wild: Table name wildcard (fetch all tables if null)
 * @param[out] tables: List of results
 *
 * @retval True on error, False on success
 * */
static bool fill_tables_list(MYSQL *conn, const char *db, String *wild,
                            List<std::string> &tables) {
  char query_buff[512];
  MYSQL_ROW row;
  MYSQL_RES *res;
  DBUG_ENTER("fill_tables_list");

  if (wild) {
    snprintf(query_buff, sizeof(query_buff), SQL_GET_TABLES_IN_DB_LIKE, db,
             wild->ptr());
  } else {
    /* No wildcards: get all tables in <db> */
    snprintf(query_buff, sizeof(query_buff), SQL_GET_ALL_TABLES_IN_DB, db);
  }

  if (mysql_real_query(conn, query_buff, strlen(query_buff)) ||
      !(res = mysql_store_result(conn)))
    DBUG_RETURN(TRUE);

  while ((row = mysql_fetch_row(res))) {
    tables.push_back(new string(row[0], mysql_fetch_lengths(res)[0]));
  }

  DBUG_RETURN(FALSE);
}

/**
 * Check a table.
 *
 * @param thd: Thread handler
 * @param tables: A table list with only the target table
 *
 * @retval True on error, False on success
 * */
bool tdbctl_check_table(THD *thd, TABLE_LIST *tables) {
  Cluster_conn_manager *conn_mgr;
  string db_name, table_name;
  DBUG_ENTER("tdbctl_check_table");

  if (check_some_access(thd, SHOW_CREATE_TABLE_ACLS, tables) ||
      !(tables->grant.privilege & SHOW_CREATE_TABLE_ACLS)) {
    my_error(ER_TABLEACCESS_DENIED_ERROR, MYF(0), "TDBCTL CHECK TABLE",
             thd->security_context()->priv_user().str,
             thd->security_context()->host_or_ip().str, tables->alias);
    DBUG_RETURN(TRUE);
  }

  /*
    DB name on this Tdbctl server should be considered a base name. A suffix is
    needed when checking the table on remotes.
  */
  db_name = string(tables->db, tables->db_length);
  table_name = string(tables->table_name, tables->table_name_length);

  if (init_cluster_conn_manager(thd, TRUE, FALSE, TRUE))
    DBUG_RETURN(TRUE);
  conn_mgr = thd->cluster_conn_manager;

  /* Send metadata for check results */
  if (do_send_metadata(thd))
    DBUG_RETURN(TRUE);

  if (do_check_one_table(thd, conn_mgr, db_name, table_name, TRUE))
    DBUG_RETURN(TRUE);

  my_eof(thd);
  DBUG_RETURN(FALSE);
}

/**
 * Check multiple tables.
 *
 * @param thd: Thread handler
 * @param db: Target database, use current db if null
 * @param wild: Table name wildcard, null value means all tables in target db
 *
 * @retval True on error, False on success
 * */
int tdbctl_check_tables(THD *thd, const char *db, String *wild) {
  MYSQL *my_conn;
  Cluster_conn_manager *conn_mgr;
  List<string> tables;
  List_iterator<string> table_it;
  string *table, db_name;
  DBUG_ENTER("tdbctl_check_tables");

  if (db) {
    db_name.assign(db);
  } else {
    LEX_STRING tmp;
    if (thd->copy_db_to(&tmp.str, &tmp.length))
      /* ERROR: No database selected */
      DBUG_RETURN(TRUE);
    db_name.assign(tmp.str, tmp.length);
  }

  /*
    Since we are checking multiple (or could be all) tables in the same db. The
    user is required to have access to the entire db.
  */
  if (validate_db_grants(thd, db_name.c_str()))
    DBUG_RETURN(TRUE);

  if (init_cluster_conn_manager(thd, TRUE, FALSE, TRUE))
    DBUG_RETURN(TRUE);
  conn_mgr = thd->cluster_conn_manager;

  /* Send metadata for check results */
  if (do_send_metadata(thd))
    DBUG_RETURN(TRUE);

  my_conn =
      conn_mgr->get_conn_map(NODE_TYPE_CTL).at(conn_mgr->get_my_server_name());
  if (fill_tables_list(my_conn, db_name.c_str(), wild, tables)) {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), "failed to fetch tables");
    DBUG_RETURN(TRUE);
  }

  table_it.init(tables);
  while ((table = table_it++)) {
    if (do_check_one_table(thd, conn_mgr, db_name, *table, TRUE))
      DBUG_RETURN(TRUE);
  }

  my_eof(thd);
  DBUG_RETURN(FALSE);
}

/**
 * Helper function that actually does the checking.
 *
 * @retval True on error, False on success
 * */
static bool do_check_one_table(THD *thd, Cluster_conn_manager *conn_mgr,
                               const string &db_name, const string &table_name,
                              bool do_send) {
  Protocol *protocol = (do_send ? thd->get_protocol() : NULL);
  DBUG_ENTER("do_check_one_table");

  /* Lock against possible DDL actions */
  Table_MDL_lock_guard guard(thd, db_name, table_name);
  if (!guard.lock_successful())
    DBUG_RETURN(TRUE);

  /* Get column records for the table on this server */
  string my_server = conn_mgr->get_my_server_name();
  MYSQL *my_conn = conn_mgr->get_conn_map(NODE_TYPE_CTL).at(my_server);
  Table_info my_table_info;
  Column_records my_records;
  if (get_table_info_for_server(thd, db_name, table_name, my_server, my_conn,
                                my_table_info, NODE_TYPE_CTL) ||
      get_column_records_for_server(thd, db_name, table_name, my_server,
                                    my_conn, my_records, NODE_TYPE_CTL)) {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0),
             "failed to get table definition from current server");
    DBUG_RETURN(TRUE);
  }
  if (!my_table_info.exists) {
    string tmp_tbl_name = db_name + "." + table_name;
    my_error(ER_BAD_TABLE_ERROR, MYF(0), tmp_tbl_name.c_str());
    DBUG_RETURN(TRUE);
  }

  /*
    Compare table & column definition from each Spider/Remote server against
    this server's.
  */
  enum_node_type type = NODE_TYPE_SPIDER;
  while (type != NODE_TYPE_END) {
    map<string, MYSQL *>::const_iterator conn_it;
    map<string, MYSQL *> conns = conn_mgr->get_conn_map(type);
    for (conn_it = conns.begin(); conn_it != conns.end(); ++conn_it) {
      const string &server_name = conn_it->first;
      MYSQL *mysql = conn_it->second;
      Table_info table_info;
      Column_records records;
      const char *status;
      Message_writer writer;

      /* For Remotes, add a numeric suffix according to their server_name */
      string fixed_db_name = fix_db_name(db_name, server_name, type);

      if (do_send) {
        protocol->start_row();
        protocol->store(server_name.c_str(), server_name.length(),
                        system_charset_info);
        protocol->store(fixed_db_name.c_str(), fixed_db_name.length(),
                        system_charset_info);
        protocol->store(table_name.c_str(), table_name.length(),
                        system_charset_info);
      }

      /* STAGE 1: check table info */
      if (get_table_info_for_server(thd, fixed_db_name, table_name, server_name,
                                    mysql, table_info, type)) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "failed to get table info");
        goto send_row;
      }

      if (!table_info.exists) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "table '%s.%s' does not exist", fixed_db_name.c_str(),
                     table_name.c_str());
        goto send_row;
      }

      /* Compare default charsets */
      if (my_table_info.table_collation != table_info.table_collation) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "inconsistent table collation '%s', should be '%s'",
                     table_info.table_collation.c_str(),
                     my_table_info.table_collation.c_str());
      }

      /* Compare engines (skip Spiders) */
      if (type != NODE_TYPE_SPIDER &&
          my_table_info.table_engine != table_info.table_engine) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "inconsistent table engine '%s', should be '%s'",
                     table_info.table_engine.c_str(),
                     my_table_info.table_engine.c_str());
      }

      /* STAGE 2: check column definitions */
      if (get_column_records_for_server(thd, fixed_db_name, table_name,
                                        server_name, mysql, records, type)) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "failed to get field definitions");
        goto send_row;
      }

      if (my_records.size() != records.size()) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "inconsistent field count, got %lu, should be %lu",
                     records.size(), my_records.size());
        goto send_row;
      }

      for (map<string, Column_record>::iterator it = my_records.begin();
           it != my_records.end(); ++it) {
        const string &column_name = it->first;
        const Column_record &my_rec = it->second;
        ulonglong flags;
        if (!records.count(column_name)) {
          writer.write(Message_writer::MSG_LEVEL_ERROR, "missing field '%s'",
                       column_name.c_str());
        } else {
          const Column_record &tmp_rec = records[column_name];
          if (!(flags = column_record_cmp(my_rec, tmp_rec)))
            /* consistent definition, skip */
            continue;
          if (flags & COLUMN_DIFF_DATA_TYPE)
            write_column_diff_error(writer, "data type is '%s' instead of '%s'",
                                    column_name.c_str(),
                                    tmp_rec.data_type.c_str(),
                                    my_rec.data_type.c_str());
          if (flags & COLUMN_DIFF_ORDINAL_POSITION)
            write_column_diff_error(
                writer, "at position %lu instead of %lu", column_name.c_str(),
                tmp_rec.ordinal_position, my_rec.ordinal_position);
          if (flags & COLUMN_DIFF_COLLATION)
            write_column_diff_error(writer, "collation is '%s' instead of '%s'",
                                    column_name.c_str(),
                                    tmp_rec.collation_name.c_str(),
                                    my_rec.collation_name.c_str());
          if (flags & COLUMN_DIFF_COLUMN_KEY)
            write_column_diff_error(writer, "inconsistent key definition '%s'",
                                    column_name.c_str(),
                                    tmp_rec.column_key.c_str());
        }
      }

    send_row:
      if (do_send) {
        status = (writer.has_error() ? "Error" : "OK");
        protocol->store(status, strlen(status), system_charset_info);
        protocol->store(writer.raw_str(),
                        MY_MIN(writer.length(), max_message_length),
                        system_charset_info);
        if (protocol->end_row())
          DBUG_RETURN(TRUE);
      }
    }
    /* Once Spiders are finished, move on to Remotes */
    if (type == NODE_TYPE_SPIDER)
      type = NODE_TYPE_REMOTE;
    else /* Remotes are done, end the loop */
      type = NODE_TYPE_END;
  }

  DBUG_RETURN(FALSE);
}

static bool validate_db_grants(THD *thd, const char *db) {
  char db_name[NAME_LEN];
  Security_context *sctx = thd->security_context();
  uint db_access;

  strcpy(db_name, db);
  if (lower_case_table_names)
    my_casedn_str(files_charset_info, db_name);

#ifndef NO_EMBEDDED_ACCESS_CHECKS
  if (sctx->check_access(DB_ACLS))
    db_access = DB_ACLS;
  else
    db_access = (acl_get(sctx->host().str, sctx->ip().str,
                         sctx->priv_user().str, db_name, 0) |
                 sctx->master_access());
  if (!(db_access & DB_ACLS)) {
    my_error(ER_DBACCESS_DENIED_ERROR, MYF(0), sctx->priv_user().str,
             sctx->host_or_ip().str, db_name);
    return TRUE;
  }
#endif

  if (!is_infoschema_db(db_name) && check_db_dir_existence(db_name)) {
    /* ERROR: Unknown database */
    my_error(ER_BAD_DB_ERROR, MYF(0), db_name);
    return TRUE;
  }

  return FALSE;
}
