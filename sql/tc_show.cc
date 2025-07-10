/*
    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

/*
Add for node's show 
*/

#include "log.h"
#include "tc_show.h"
#include "tc_base.h"
#include "mysql.h"
#include "protocol.h"                       // Protocol

using namespace std;

#define VARNAME_SPIDER_AUTO_INCREMENT_MODE_SWITCH                              \
  "SPIDER_AUTO_INCREMENT_MODE_SWITCH"
#define VARNAME_SPIDER_AUTO_INCREMENT_MODE_VALUE                               \
  "SPIDER_AUTO_INCREMENT_MODE_VALUE"
#define VARNAME_SPIDER_AUTO_INCREMENT_STEP "SPIDER_AUTO_INCREMENT_STEP"

#define MAX_VAR_NAME_LEN 64
#define MAX_VAR_VALUE_LEN 1024

ST_FIELD_INFO spider_autoinc_fields_info[] = {
    {"SERVER_NAME", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"SPIDER_AUTO_INCREMENT_MODE_SWITCH", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0,
     0, 0, SKIP_OPEN_TABLE},
    {"SPIDER_AUTO_INCREMENT_MODE_VALUE", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0,
     0, SKIP_OPEN_TABLE},
    {"SPIDER_AUTO_INCREMENT_STEP", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0,
     SKIP_OPEN_TABLE},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE}
};

ST_FIELD_INFO cluster_processlist_fields_info[] = {
    {"SERVER_NAME", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"ID", 21, MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, "Id", SKIP_OPEN_TABLE},
    {"USER", USERNAME_CHAR_LENGTH, MYSQL_TYPE_STRING, 0, 0, "User",
     SKIP_OPEN_TABLE},
    {"HOST", LIST_PROCESS_HOST_LEN, MYSQL_TYPE_STRING, 0, 0, "Host",
     SKIP_OPEN_TABLE},
    {"DB", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, MY_I_S_MAYBE_NULL, "Db",
     SKIP_OPEN_TABLE},
    {"COMMAND", 16, MYSQL_TYPE_STRING, 0, 0, "Command", SKIP_OPEN_TABLE},
    {"TIME", 7, MYSQL_TYPE_LONG, 0, 0, "Time", SKIP_OPEN_TABLE},
    {"STATE", 64, MYSQL_TYPE_STRING, 0, MY_I_S_MAYBE_NULL, "State",
     SKIP_OPEN_TABLE},
    {"INFO", PROCESS_LIST_INFO_WIDTH, MYSQL_TYPE_STRING, 0, MY_I_S_MAYBE_NULL,
     "Info", SKIP_OPEN_TABLE},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE}
};

ST_FIELD_INFO server_cache_fields_info[] = {
    {"Server_name", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"Host", LIST_PROCESS_HOST_LEN, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"Db", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"Username", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"Password", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"Port", 21, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
    {"Socket", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"Wrapper", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"Owner", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * @brief Get SHOW PROCESSLIST results from cluster nodes.
 *
 * @param thd Thread handler
 * @param verbose Whether to apply full display width for INFO column
 * @param from_server Server name of the target server, if not empty, only get
 * results from the target server, otherwise get from all
 *
 * @retval 0 on success
 * @retval 1 on error
 * */
int tc_show_processlist(THD *thd, bool verbose, LEX_CSTRING from_server) {
  bool finished = FALSE;
  string target_server(from_server.str, from_server.length);
  string query =
      (verbose ? "SELECT ID,USER,HOST,DB,COMMAND,TIME,STATE,INFO FROM "
                 "INFORMATION_SCHEMA.PROCESSLIST"
               : "SELECT "
                 "ID,USER,HOST,DB,COMMAND,TIME,STATE,SUBSTRING(INFO,1,100) "
                 "FROM INFORMATION_SCHEMA.PROCESSLIST");

  Item *field;
  List<Item> field_list;
  size_t max_query_length =
      (verbose ? thd->variables.max_allowed_packet : PROCESS_LIST_WIDTH);
  Protocol *protocol = thd->get_protocol();
  Cluster_conn_manager *conn_mgr;
  Query_exec_manager query_mgr(thd);
  bool no_connect = target_server.length();
  DBUG_ENTER("tc_show_processlist");

  if (!thd->cluster_conn_manager) {
    thd->cluster_conn_manager = new Cluster_conn_manager();
  }
  /* Force the refresh to build all connections (when no_connect is FALSE) */
  if (thd->cluster_conn_manager->refresh(TRUE, no_connect))
    DBUG_RETURN(1);
  if (no_connect && thd->cluster_conn_manager->connect(target_server, FALSE))
    DBUG_RETURN(1);
  conn_mgr = thd->cluster_conn_manager;
  query_mgr.build_server_maps(thd->cluster_conn_manager);
  query_mgr.reset_error();

  field_list.push_back(new Item_empty_string("Server_name", NAME_CHAR_LEN));
  field_list.push_back(
      new Item_int(NAME_STRING("Id"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_empty_string("User", USERNAME_CHAR_LENGTH));
  field_list.push_back(new Item_empty_string("Host", LIST_PROCESS_HOST_LEN));
  field_list.push_back(field = new Item_empty_string("db", NAME_CHAR_LEN));
  field->maybe_null = 1;
  field_list.push_back(new Item_empty_string("Command", 16));
  field_list.push_back(field = new Item_return_int("Time", 7, MYSQL_TYPE_LONG));
  field->unsigned_flag = 0;
  field_list.push_back(field = new Item_empty_string("State", 30));
  field->maybe_null = 1;
  field_list.push_back(field = new Item_empty_string("Info", max_query_length));
  field->maybe_null = 1;
  if (thd->send_result_metadata(&field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(1);

  for (int i = ENUM_NODE_TYPE_BEGIN; !finished && i < ENUM_NODE_TYPE_END; ++i) {
    enum_node_type node_type = (enum_node_type)i;
    const map<string, MYSQL *> &conns = conn_mgr->get_conn_map(node_type);
    map<string, MYSQL *>::const_iterator conn_it;

    for (conn_it = conns.begin(); !finished && conn_it != conns.end();
         ++conn_it) {
      string server_name = conn_it->first;
      if (target_server.length() &&
          my_strcasecmp_mb(system_charset_info, server_name.c_str(),
                           target_server.c_str())) {
        /* Only send to target server, skip this one */
        continue;
      }

      MYSQL_ROW row;
      MYSQL_FIELD *fld;
      MYSQL_RES *res;
      MYSQL *mysql = conn_it->second;

      /* Do SHOW PROCESSLIST */
      query_mgr.store_exec_query(server_name, query, node_type);
      tc_real_query(&query_mgr, server_name, mysql, node_type);
      if (query_mgr.get_error() || !(res = mysql_store_result(mysql))) {
        /* encountered error */
        char buf[MYSQL_ERRMSG_SIZE];
        my_snprintf(buf, sizeof(buf),
                    "failed to SHOW PROCESSLIST from server: %s, "
                    "error: %u, errmsg: %s",
                    server_name.c_str(), mysql_errno(mysql),
                    mysql_error(mysql));
        my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), buf);
        DBUG_RETURN(1);
      }

      /* Process results and send to client */
      DBUG_ASSERT(mysql_num_fields(res) == 8);
      while ((row = mysql_fetch_row(res))) {
        protocol->start_row();
        protocol->store(server_name.c_str(), server_name.length(),
                        system_charset_info);
        for (uint idx = 0; idx < mysql_num_fields(res); ++idx) {
          fld = &res->fields[idx];
          protocol_store_field(protocol, *fld, row[idx], mysql_fetch_lengths(res)[idx]);
        }
        if (protocol->end_row()) {
          finished = TRUE;
          break;
        }
      }
      mysql_free_result(res);

      if (target_server.length())
        /* Break the loops for we have found target server and finished SHOW */
        finished = TRUE;
    }
  }

  my_eof(thd);
  DBUG_RETURN(0);
}

/**
 * @brief Get SHOW VARIABLES results from cluster nodes.
 *
 * @param thd Thread handler
 * @param type Type of SHOW, either OPT_DEFAULT, OPT_SESSION or OPT_GLOBAL
 * @param wild Variable name wildcard from the LIKE '?' statements
 * @param from_server Server name of the target server, if not empty, only get
 * results from the target server, otherwise get from all
 *
 * @retval 0 on success
 * @retval 1 on error
 * */
int tc_show_variables(THD *thd, enum_var_type type, String *wild,
                      LEX_CSTRING from_server) {
  stringstream q_stream;
  string query;
  string target_server(from_server.str, from_server.length);
  Item *field;
  List<Item> field_list;
  Protocol *protocol = thd->get_protocol();
  Cluster_conn_manager *conn_mgr;
  Query_exec_manager query_mgr(thd);
  bool finished = FALSE;
  bool no_connect = target_server.length();

  DBUG_ENTER("tc_show_variables");

  if (!thd->cluster_conn_manager) {
    thd->cluster_conn_manager = new Cluster_conn_manager();
  }
  /* Force the refresh to build all connections (when no_connect is FALSE) */
  if (thd->cluster_conn_manager->refresh(TRUE, no_connect))
    DBUG_RETURN(1);
  if (no_connect && thd->cluster_conn_manager->connect(target_server, FALSE))
    DBUG_RETURN(1);
  conn_mgr = thd->cluster_conn_manager;
  query_mgr.build_server_maps(thd->cluster_conn_manager);
  query_mgr.reset_error();

  q_stream << "SHOW ";
  if (type == OPT_SESSION) {
    q_stream << "SESSION ";
  } else if (type == OPT_GLOBAL) {
    q_stream << "GLOBAL ";
  } /* else: append nothing */
  q_stream << "VARIABLES";
  if (wild) {
    q_stream << " LIKE '" << wild->ptr() << "'";
  }
  query = q_stream.str();

  field_list.push_back(new Item_empty_string("Server_name", NAME_CHAR_LEN));
  field_list.push_back(
      new Item_empty_string("Variable_name", MAX_VAR_NAME_LEN));
  field_list.push_back(field =
                           new Item_empty_string("Value", MAX_VAR_VALUE_LEN));
  field->maybe_null = 1;
  if (thd->send_result_metadata(&field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(1);

  for (int i = ENUM_NODE_TYPE_BEGIN; !finished && i < ENUM_NODE_TYPE_END; ++i) {
    enum_node_type node_type = (enum_node_type)i;
    const map<string, MYSQL *> &conns = conn_mgr->get_conn_map(node_type);
    map<string, MYSQL *>::const_iterator conn_it;

    for (conn_it = conns.begin(); !finished && conn_it != conns.end();
         ++conn_it) {
      string server_name = conn_it->first;
      if (target_server.length() &&
          my_strcasecmp_mb(system_charset_info, server_name.c_str(),
                           target_server.c_str())) {
        /* Only send to target server, skip this one */
        continue;
      }

      MYSQL_ROW row;
      MYSQL_RES *res;
      MYSQL *mysql = conn_it->second;

      /* Do SHOW VARIABLES */
      query_mgr.store_exec_query(server_name, query, node_type);
      tc_real_query(&query_mgr, server_name, mysql, node_type);
      if (query_mgr.get_error() || !(res = mysql_store_result(mysql))) {
        /* encountered error */
        char buf[MYSQL_ERRMSG_SIZE];
        my_snprintf(buf, sizeof(buf),
                    "failed to SHOW VARIABLES from server: %s, "
                    "error: %u, errmsg: %s",
                    server_name.c_str(), mysql_errno(mysql),
                    mysql_error(mysql));
        my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), buf);
        DBUG_RETURN(1);
      }

      /* Process results and send to client */
      DBUG_ASSERT(mysql_num_fields(res) == 2);
      while ((row = mysql_fetch_row(res))) {
        protocol->start_row();
        protocol->store(server_name.c_str(), server_name.length(),
                        system_charset_info);
        /* VARIABLE_NAME */
        protocol->store(row[0], mysql_fetch_lengths(res)[0],
                        system_charset_info);
        /* VALUE */
        if (likely(row[1]))
          protocol->store(row[1], mysql_fetch_lengths(res)[1],
                          system_charset_info);
        else
          protocol->store_null();
        if (protocol->end_row()) {
          finished = TRUE;
          break;
        }
      }
      mysql_free_result(res);

      if (target_server.length())
        /* Break the loops for we have found target server and finished SHOW */
        finished = TRUE;
    }
  }

  my_eof(thd);
  DBUG_RETURN(0);
}

int fill_schema_spider_autoinc(THD *thd, TABLE_LIST *tables, Item *cond) {
  DBUG_ENTER("fill_schema_spider_autoinc");

  TABLE *table;
  map<string, MYSQL_RES *> result_map;
  Cluster_conn_manager *conn_mgr;
  char query[512];
  const char *get_vars_fmt = "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM "
                             "INFORMATION_SCHEMA.GLOBAL_VARIABLES "
                             "WHERE VARIABLE_NAME IN ('%s','%s','%s')";
  Query_exec_manager query_mgr(thd);

  if (!thd->cluster_conn_manager) {
    thd->cluster_conn_manager = new Cluster_conn_manager();
  }
  /* Only Spider connections are needed */
  if (thd->cluster_conn_manager->refresh(FALSE, TRUE) ||
      thd->cluster_conn_manager->connect(NODE_TYPE_SPIDER, FALSE))
    DBUG_RETURN(1);
  conn_mgr = thd->cluster_conn_manager;

  query_mgr.build_server_maps(thd->cluster_conn_manager);
  query_mgr.reset_error();

  my_snprintf(query, sizeof(query), get_vars_fmt,
              VARNAME_SPIDER_AUTO_INCREMENT_MODE_SWITCH,
              VARNAME_SPIDER_AUTO_INCREMENT_MODE_VALUE,
              VARNAME_SPIDER_AUTO_INCREMENT_STEP);
  query_mgr.store_exec_query(query, NODE_TYPE_SPIDER);

  const map<string, MYSQL *> &spider_conns = conn_mgr->get_spider_conn_map();
  map<string, MYSQL *>::const_iterator conn_it;
  for (conn_it = spider_conns.begin(); conn_it != spider_conns.end();
       conn_it++) {
    MYSQL_RES *res;
    tc_real_query(&query_mgr, conn_it->first, conn_it->second,
                  NODE_TYPE_SPIDER);
    if (query_mgr.get_error() || !(res = mysql_store_result(conn_it->second))) {
      /* encountered error */
      char buf[256];
      my_snprintf(
          buf, sizeof(buf),
          "failed to retrieve AUTOINC variables from server: %s, error: %u",
          conn_it->first.c_str(), mysql_errno(conn_it->second));
      my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), buf);
      DBUG_RETURN(1);
    }
    result_map[conn_it->first] = res;
  }

  table = tables->table;
  map<string, MYSQL_RES *>::iterator res_it;
  for (res_it = result_map.begin(); res_it != result_map.end(); res_it++) {
    MYSQL_ROW row;
    MYSQL_RES *res = res_it->second;

    DBUG_ASSERT(mysql_num_fields(res) == 2);
    int row_cnt = 0;
    restore_record(table, s->default_values);
    /* SERVER NAME */
    table->field[0]->store(res_it->first.c_str(), res_it->first.length(),
                           &my_charset_bin);
    while ((row = mysql_fetch_row(res))) {
      ulong length = mysql_fetch_lengths(res)[1];
      ++row_cnt;
      if (!strcasecmp(row[0], VARNAME_SPIDER_AUTO_INCREMENT_MODE_SWITCH)) {
        table->field[1]->store(row[1], length, &my_charset_bin);
      } else if (!strcasecmp(row[0],
                             VARNAME_SPIDER_AUTO_INCREMENT_MODE_VALUE)) {
        table->field[2]->store(row[1], length, &my_charset_bin);
      } else if (!strcasecmp(row[0], VARNAME_SPIDER_AUTO_INCREMENT_STEP)) {
        table->field[3]->store(row[1], length, &my_charset_bin);
      }
    }
    DBUG_ASSERT(row_cnt == 3);
    if (unlikely(row_cnt != 3)) {
      sql_print_error(
          "expected 3 SPIDER_AUTO_INCREMENT variables from server %s, got %d",
          res_it->first.c_str(), row_cnt);
    }
    schema_table_store_record(thd, table);
    mysql_free_result(res);
  }

  DBUG_RETURN(0);
}

int fill_schema_cluster_processlist(THD *thd, TABLE_LIST *tables, Item *cond) {
  DBUG_ENTER("fill_schema_cluster_processlist");

  TABLE *table;
  map<string, MYSQL_RES *> result_map;
  Cluster_conn_manager *conn_mgr;
  const char *query = "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO "
                      "FROM INFORMATION_SCHEMA.PROCESSLIST";
  Query_exec_manager query_mgr(thd);

  if (!thd->cluster_conn_manager) {
    thd->cluster_conn_manager = new Cluster_conn_manager();
  }
  /* Force the refresh to build all connections */
  if (thd->cluster_conn_manager->refresh(TRUE, FALSE))
    DBUG_RETURN(1);
  conn_mgr = thd->cluster_conn_manager;

  query_mgr.build_server_maps(thd->cluster_conn_manager);
  query_mgr.reset_error();
  table = tables->table;
  for (int i = ENUM_NODE_TYPE_BEGIN; i < ENUM_NODE_TYPE_END; ++i) {
    enum_node_type node_type = (enum_node_type)i;
    const map<string, MYSQL *> &conns = conn_mgr->get_conn_map(node_type);
    map<string, MYSQL *>::const_iterator conn_it;
    query_mgr.store_exec_query(query, node_type);

    for (conn_it = conns.begin(); conn_it != conns.end(); ++conn_it) {
      MYSQL_ROW row;
      MYSQL_RES *res;
      tc_real_query(&query_mgr, conn_it->first, conn_it->second, node_type);
      if (query_mgr.get_error() ||
          !(res = mysql_store_result(conn_it->second))) {
        /* encountered error */
        char buf[MYSQL_ERRMSG_SIZE];
        my_snprintf(buf, sizeof(buf),
                    "failed to retrieve PROCESSLIST from server: %s, "
                    "error: %u, errmsg: %s",
                    conn_it->first.c_str(), mysql_errno(conn_it->second),
                    mysql_error(conn_it->second));
        my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), buf);
        DBUG_RETURN(1);
      }

      while ((row = mysql_fetch_row(res))) {
        restore_record(table, s->default_values);
        /* SERVER_NAME */
        table->field[0]->store(conn_it->first.c_str(), conn_it->first.length(),
                               system_charset_info);
        /* ID */
        table->field[1]->store(strtoull(row[0], NULL, 10));
        /* USER */
        table->field[2]->store(row[1], mysql_fetch_lengths(res)[1],
                               system_charset_info);
        /* HOST */
        table->field[3]->store(row[2], mysql_fetch_lengths(res)[2],
                               system_charset_info);
        /* DB */
        if (row[3]) {
          table->field[4]->store(row[3], mysql_fetch_lengths(res)[3],
                                 system_charset_info);
          table->field[4]->set_notnull();
        }
        /* COMMAND */
        table->field[5]->store(row[4], mysql_fetch_lengths(res)[4],
                               system_charset_info);
        /* TIME */
        table->field[6]->store(strtoll(row[5], NULL, 10));
        /* STATE */
        if (row[6]) {
          table->field[7]->store(row[6], mysql_fetch_lengths(res)[6],
                                 system_charset_info);
          table->field[7]->set_notnull();
        }
        /* INFO */
        if (row[7]) {
          table->field[8]->store(row[7], mysql_fetch_lengths(res)[7],
                                 system_charset_info);
          table->field[8]->set_notnull();
        }
        schema_table_store_record(thd, table);
      }
      mysql_free_result(res);
    }
  }

  DBUG_RETURN(0);
}

int fill_schema_server_cache(THD *thd, TABLE_LIST *tables, Item *cond) 
{
  DBUG_ENTER("fill_schema_server_cache");

  TABLE *table = tables->table;
  list<FOREIGN_SERVER *> server_list;
  get_server_by_wrapper(server_list, thd->mem_root, NULL_WRAPPER, FALSE);

  auto put_str_field = [&table](int idx, const char *value) {
    if (value) {
      table->field[idx]->store(value, strlen(value), system_charset_info);
    }
  };

  for (const auto &server : server_list) {
    if(server) {
      restore_record(table, s->default_values);
      /* Server_name */
      put_str_field(0, server->server_name);
      /* Host */
      put_str_field(1, server->host);
      /* Db */
      put_str_field(2, server->db);
      /* Username */
      put_str_field(3, server->username);
      /* Password */
      put_str_field(4, server->password);
      /* Port */
      table->field[5]->store(server->port);
      /* Socket */
      put_str_field(6, server->socket);
      /* Wrapper */
      put_str_field(7, server->scheme);
      /* Owner */
      put_str_field(8, server->owner);

      schema_table_store_record(thd, table);
    }
  }

  DBUG_RETURN(0);
}
