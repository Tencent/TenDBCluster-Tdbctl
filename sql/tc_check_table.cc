/*
  Copyright (C) 2020 THL A29 Limited, a Tencent company. All rights reserved.
*/
#include <map>
#include <string>
#include <vector>

#include "sql_class.h"
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

#define IS_TABLES_TABLE_SCHEMA          1
#define IS_TABLES_TABLE_NAME            2
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
    info.table_collation = DATA_STRING(row, mysql_fetch_lengths(res), IS_TABLES_TABLE_COLLATION);
  }
  mysql_free_result(res);

  DBUG_RETURN(FALSE);
}

/**
 *
 * Result Set:
 *  - SERVER_NAME   VARCHAR(64)   Server's identifier
 *  - DB            VARCHAR(64)   Corresponding db on the server
 *  - TABLE         VARCHAR(?)    Corresponding table on the server
 *  - STATUS        CHAR(10)      Check result: [OK, Error]
 *  - MESSAGE       VARCHAR(512)  Error message
 *
 * */
bool tdbctl_check_table(THD *thd, TABLE_LIST *tables) {
  List<Item> field_list;
  Item *item;
  Protocol *protocol = thd->get_protocol();
  Cluster_conn_manager *conn_mgr;
  Query_exec_manager query_mgr(thd);
  string db_name, table_name;
  DBUG_ENTER("tdbctl_check_table");

  const uint max_message_length = 512;
  field_list.push_back(item =
                           new Item_empty_string("Server_name", NAME_CHAR_LEN));
  field_list.push_back(item = new Item_empty_string("Db", NAME_CHAR_LEN));
  field_list.push_back(item = new Item_empty_string("Table", NAME_LEN * 2));
  field_list.push_back(item = new Item_empty_string("Status", 10));
  field_list.push_back(item = new Item_empty_string("Message", max_message_length));
  if (thd->send_result_metadata(&field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(TRUE);

  /*
    DB name on this Tdbctl server should be considered a base name. A suffix is
    needed when checking the table on remotes.
  */
  db_name = string(tables->db, tables->db_length);
  table_name = string(tables->table_name, tables->table_name_length);

  DBUG_ASSERT(thd->cluster_conn_manager);
  conn_mgr = thd->cluster_conn_manager;

  /* TODO: allow specification of node types */
  if (conn_mgr->refresh(TRUE, FALSE))
    DBUG_RETURN(TRUE);
  if (conn_mgr->identify_self()) {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0),
             "cannot identify current server");
    DBUG_RETURN(TRUE);
  }

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

      protocol->start_row();
      protocol->store(server_name.c_str(), server_name.length(),
                      system_charset_info);
      protocol->store(fixed_db_name.c_str(), fixed_db_name.length(), system_charset_info);
      protocol->store(table_name.c_str(), table_name.length(),
                      system_charset_info);

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
                     "inconsistent table collation '%s'",
                     table_info.table_collation.c_str());
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
      status = (writer.has_error() ? "Error" : "OK");
      protocol->store(status, strlen(status), system_charset_info);
      protocol->store(writer.raw_str(),
                      MY_MIN(writer.length(), max_message_length),
                      system_charset_info);
      if (protocol->end_row())
        DBUG_RETURN(TRUE);
    }
    /* Once Spiders are finished, move on to Remotes */
    if (type == NODE_TYPE_SPIDER)
      type = NODE_TYPE_REMOTE;
    else /* Remotes are done, end the loop */
      type = NODE_TYPE_END;
  }

  my_eof(thd);
  DBUG_RETURN(FALSE);
}
