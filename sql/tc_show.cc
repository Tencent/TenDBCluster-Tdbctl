/*
    Copyright (C) 2020 Tencent.  All rights reserved.
*/

/*
Add for node's show 
*/

#include "log.h"
#include "tc_show.h"
#include "tc_base.h"
#include "mysql.h"
#include "protocol.h"                       // Protocol
#include <thread>

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

ST_FIELD_INFO cluster_nodes_fields_info[] = {
    {"SERVER_NAME", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"HOST", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"PORT", 21, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
    {"USERNAME", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"PASSWORD", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"WRAPPER", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"VERSION", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"STATUS", 64, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
    {"FEATURE_INFO", 1024, MYSQL_TYPE_JSON, 0, 0, 0, SKIP_OPEN_TABLE},
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

int i_s_cluster_nodes_fill(THD *thd, TABLE_LIST *tables, Item *cond)
{
  DBUG_ENTER("i_s_cluster_nodes_fill");

  TABLE *table = tables->table;
  std::vector<tc_node_info> node_infos;
  tc_get_cluster_nodes_info(thd, true, node_infos);

  auto put_str_field = [&table](int idx, const char *value) {
    if (value) {
      table->field[idx]->store(value, strlen(value), system_charset_info);
    }
  };

  std::string feature_info_json;
  for (const tc_node_info &one_node : node_infos) {
    restore_record(table, s->default_values);
    /* Server_name */
    put_str_field(0, one_node.server_name.c_str());
    /* Host */
    put_str_field(1, one_node.host.c_str());
    /* Port */
    table->field[2]->store(one_node.port);
    /* Username */
    put_str_field(3, one_node.user.c_str());
    /* Password */
    put_str_field(4, one_node.passwd.c_str());
    /* Wrapper */
    put_str_field(5, one_node.wrapper.c_str());
    /* Version */
    put_str_field(6, one_node.version.c_str());
    /* Status */
    put_str_field(7, one_node.get_node_status().c_str());
    /* Feature_info */
    one_node.make_feature_info_json(feature_info_json);
    put_str_field(8, feature_info_json.c_str());

    schema_table_store_record(thd, table);
  }

  DBUG_RETURN(0);
}

const char *tc_node_info::NODE_INFO_UNKNOWN_STR = "unknown";
const char *tc_node_info::NODE_INFO_QUERY_FAILED_STR = "query failed";
const char *tc_node_info::NODE_INFO_WRONG_FILED_INDEX = "wrong field index";
const char *tc_node_info::NODE_INFO_KEY_SLAVE_STATUS = "SLAVE_STATUS";
const char *tc_node_info::NODE_INFO_KEY_MASTER_NAME = "MASTER_NAME";

std::string tc_node_info::get_node_status() const
{
  switch(status) {
  case NODE_STATUS_UNREACHABLE:
    return "Unreachable";
  case NODE_STATUS_ONLINE:
    return "Online";
  case NODE_STATUS_UNKNOWN:
  default:
    return "Unknown";
  }
}

void tc_node_info::make_feature_info_json(std::string &feature_info_json) const
{
  feature_info_json = "{";
  for (size_t i = 0; i < feature_info.size(); ++i) 
  {
    if(feature_info[i].first == NODE_INFO_KEY_SLAVE_STATUS) {  // json string no need to wrap with ""
      feature_info_json += "\"" + feature_info[i].first + "\": " + feature_info[i].second;
    }
    else 
    {
      feature_info_json += "\"" + feature_info[i].first + "\": \"" + feature_info[i].second + "\"";      
    }

    if (i < feature_info.size() - 1) {
      feature_info_json += ", ";
    }
  }
  feature_info_json += "}";
}

/*
   To ensure consistency with INFORMATION_SCHEMA.TDBCTL_NODES, 
   the examine_tdbctl_node function defined in dbm.cc is reused here.
*/
extern void examine_tdbctl_node(THD *thd, Cluster_conn_manager *conn_mgr,
                                const std::string &server_name, MYSQL *mysql,
                                std::string &repl_master_name,
                                std::string &cluster_role, std::string &status,
                                std::string &message,
                                std::string &repl_info);

std::string get_repl_master_name(Cluster_conn_manager *conn_mgr, 
                                const std::string &master_host,
                                const std::string &master_port)
{
  for(int node_type = ENUM_NODE_TYPE_BEGIN; node_type < NODE_TYPE_END; ++node_type) {
    const auto &auth_map = conn_mgr->get_auth_map((enum_node_type)node_type);
    for (const auto &it : auth_map) {
      if(it.second.ipport_str == master_host + "#" + master_port) {
        return it.first;
      }
    }
  }
  return "<unknown_server>";
}

/* Indices of columns from SHOW SLAVE STATUS results */
#define MASTER_HOST_IDX 1        /* Master_Host */
#define MASTER_PORT_IDX 3        /* Master_Port */
#define RELAY_MASTER_LOG_FILE_IDX 9 /* Relay_Master_Log_File */
#define SLAVE_IO_RUNNING_IDX 10  /* Slave_IO_Running */
#define SLAVE_SQL_RUNNING_IDX 11 /* Slave_SQL_Running */
#define EXEC_MASTER_LOG_POS_IDX 21 /* Exec_Master_Log_Pos */

void tc_fill_node_slave_status(MYSQL *mysql, 
                              Cluster_conn_manager *conn_mgr, 
                              tc_node_info &node_info)
{
  DBUG_ENTER("tc_fill_node_slave_status");

  if(mysql != NULL) {
    /* Get slave status */
    MYSQL_RES * slave_status_res = tc_exec_sql_with_result(mysql, "SHOW SLAVE STATUS");
    MYSQL_ROW row;
    if(slave_status_res) 
    {
      if((row = mysql_fetch_row(slave_status_res))) {
        uint n_fileds = mysql_num_fields(slave_status_res);
        string master_host = MASTER_HOST_IDX < n_fileds ? 
                            row[MASTER_HOST_IDX] : tc_node_info::NODE_INFO_WRONG_FILED_INDEX;
        string master_port = MASTER_PORT_IDX < n_fileds ? 
                            row[MASTER_PORT_IDX] : tc_node_info::NODE_INFO_WRONG_FILED_INDEX;
        string slave_io_running = SLAVE_IO_RUNNING_IDX < n_fileds ? 
                            row[SLAVE_IO_RUNNING_IDX] : tc_node_info::NODE_INFO_WRONG_FILED_INDEX;
        string slave_sql_running = SLAVE_SQL_RUNNING_IDX < n_fileds ? 
                            row[SLAVE_SQL_RUNNING_IDX] : tc_node_info::NODE_INFO_WRONG_FILED_INDEX;
        string relay_master_log_file = RELAY_MASTER_LOG_FILE_IDX < n_fileds ? 
                            row[RELAY_MASTER_LOG_FILE_IDX] : tc_node_info::NODE_INFO_WRONG_FILED_INDEX;
        string exec_master_log_pos = EXEC_MASTER_LOG_POS_IDX < n_fileds ? 
                            row[EXEC_MASTER_LOG_POS_IDX] : tc_node_info::NODE_INFO_WRONG_FILED_INDEX;
        string master_name = get_repl_master_name(conn_mgr, master_host, master_port);

        string slave_status_json_str;
        tc_node_info::make_slave_status_json(master_host, 
                                            master_port, 
                                            slave_io_running, 
                                            slave_sql_running, 
                                            relay_master_log_file, 
                                            exec_master_log_pos, 
                                            slave_status_json_str);
        
        node_info.feature_info.emplace_back(tc_node_info::NODE_INFO_KEY_MASTER_NAME, master_name);
        node_info.feature_info.emplace_back(tc_node_info::NODE_INFO_KEY_SLAVE_STATUS, slave_status_json_str);
      }
      
      mysql_free_result(slave_status_res);
    }
    else 
    {
      node_info.feature_info.emplace_back(tc_node_info::NODE_INFO_KEY_MASTER_NAME, 
                                          tc_node_info::NODE_INFO_UNKNOWN_STR);
      node_info.feature_info.emplace_back(tc_node_info::NODE_INFO_KEY_SLAVE_STATUS, 
                                          tc_node_info::NODE_INFO_QUERY_FAILED_STR);
    }
  }

  DBUG_VOID_RETURN;
}

/**
 * @brief Fills the node information for a single server.
 * 
 * This function is designed to be thread-safe and is typically used in a multi-threaded context.
 * The `conn_mgr` parameter must only be used for read operations to ensure thread safety.
 * 
 * @param conn_mgr Pointer to the cluster connection manager (read-only operations only).
 * @param server_name Name of the server.
 * @param host Host address of the server.
 * @param port Port number of the server.
 * @param user Username for authentication.
 * @param passwd Password for authentication.
 * @param wrapper Wrapper type of the server.
 * @param verbose Flag to enable detailed information collection.
 * @param node_info Reference to the node_info object to be filled.
 */
void tc_fill_one_node_info(Cluster_conn_manager *conn_mgr,
                          const string &server_name,
                          const string &host, 
                          uint port, 
                          const string &user,
                          const string &passwd, 
                          const string &wrapper,
                          bool verbose,
                          tc_node_info &node_info) 
{
  DBUG_ENTER("tc_get_one_node_info");

  node_info.set_basic_info(server_name, host, port, user, passwd, wrapper);

  /* Connect to the node */
  MYSQL *mysql = tc_conn_connect(host, port, user, passwd, wrapper);
  if (mysql == NULL) {
    node_info.status = tc_node_info::NODE_STATUS_UNREACHABLE;
  } else {
    node_info.status = tc_node_info::NODE_STATUS_ONLINE;
  }

  MYSQL_GUARD(mysql);
  MYSQL_ROW row;

  /* Get version info */
  if(mysql != NULL) {
    MYSQL_RES * version_res = tc_exec_sql_with_result(mysql, "SELECT version()");
    if(version_res) {
      row = mysql_fetch_row(version_res);
      node_info.version = row ? row[0] : tc_node_info::NODE_INFO_QUERY_FAILED_STR;
      mysql_free_result(version_res);
    } else {
      node_info.version = tc_node_info::NODE_INFO_QUERY_FAILED_STR;
    }
  }

  if (verbose) {
    if(strcasecmp(wrapper.c_str(), TDBCTL_WRAPPER) == 0)  /* tdbctl feature info */
    {
      /*
        For the status information of Tdbctl nodes, to maintain consistency
        with the older version of INFORMATION_SCHEMA.TDBCTL_NODES,
        we call the DBM plugin's examine_tdbctl_node function to retrieve 
        the status information and reformat it.
      */
      string master_name, tdbctl_role, tdbctl_status, message, repl_info;
      examine_tdbctl_node(NULL, conn_mgr, server_name, mysql, master_name,
                        tdbctl_role, tdbctl_status, message, repl_info);
      node_info.feature_info.emplace_back("TDBCTL_ROLE", tdbctl_role);
      node_info.feature_info.emplace_back("TDBCTL_STATUS", tdbctl_status);
      if(!master_name.empty()) {
        node_info.feature_info.emplace_back(tc_node_info::NODE_INFO_KEY_MASTER_NAME, master_name);
      }
      if(!repl_info.empty()) {
        node_info.feature_info.emplace_back(tc_node_info::NODE_INFO_KEY_SLAVE_STATUS, repl_info);
      }
      if(!message.empty()) {
        node_info.feature_info.emplace_back("MESSAGE", message);
      }
    }
    else
    {
      tc_fill_node_slave_status(mysql, conn_mgr, node_info);
    }
  }
  
  DBUG_VOID_RETURN;
}

/**
 * @brief Retrieves and fills information for all nodes in the cluster.
 * 
 * @param thd Pointer to the thread handler.
 * @param verbose Flag to enable detailed information collection.
 * @param node_infos Vector to store the node information for all servers.
 */
void tc_get_cluster_nodes_info(THD *thd, bool verbose, std::vector<tc_node_info> &node_infos)
{
  DBUG_ENTER("tc_get_cluster_nodes_info");

  /* Get server list */
  list<FOREIGN_SERVER *> server_list;
  get_server_by_wrapper(server_list, thd->mem_root, NULL_WRAPPER, FALSE);
  size_t server_count = server_list.size();

  std::unique_ptr<Cluster_conn_manager> conn_mgr(new Cluster_conn_manager());
  conn_mgr->refresh(true, true);

  const size_t MAX_THREADS = 128;
  std::vector<std::thread> threads(MAX_THREADS);
  node_infos.clear();
  node_infos.resize(server_count);
  size_t n_epoches = server_count > 0 ? (server_count - 1) / MAX_THREADS + 1 : 0;
  auto server_iter = server_list.begin();

  /* Fill node info */
  for(size_t epoch = 0; epoch < n_epoches; ++epoch) {
    size_t start = epoch * MAX_THREADS;
    size_t end = std::min(start + MAX_THREADS, server_count);
    for (size_t i = start; i < end; ++i) {
      threads[i - start] = std::thread(tc_fill_one_node_info, 
                                      conn_mgr.get(),
                                      (*server_iter)->server_name,
                                      (*server_iter)->host,
                                      static_cast<uint>((*server_iter)->port),
                                      (*server_iter)->username,
                                      (*server_iter)->password,
                                      (*server_iter)->scheme,
                                      verbose,
                                      std::ref(node_infos[i]));
      ++server_iter;
    }
    for (size_t i = start; i < end; ++i) {
      threads[i - start].join();
    }
  }

  DBUG_VOID_RETURN;
}

bool tc_show_cluster_nodes(THD *thd, bool verbose)
{
  DBUG_ENTER("tc_show_cluster_nodes");

  Item *field;
  List<Item> field_list;
  Protocol *protocol = thd->get_protocol();

  /* Send field metadata */
  field_list.push_back(new Item_empty_string("Server_name", 64));
  field_list.push_back(new Item_empty_string("Host", 64));
  field_list.push_back(field = new Item_return_int("Port", 7, MYSQL_TYPE_LONG));
  field->unsigned_flag = 1;
  if(verbose) {
    field_list.push_back(new Item_empty_string("Username", 64));
    field_list.push_back(new Item_empty_string("Password", 64));
  }
  field_list.push_back(new Item_empty_string("Wrapper", 64));
  field_list.push_back(new Item_empty_string("Version", 64));
  field_list.push_back(new Item_empty_string("Status", 64));
  if(verbose) {
    field_list.push_back(new Item_empty_string("Feature_info", 1024));
  }
  if (thd->send_result_metadata(&field_list, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(TRUE);

  /* Get cluster nodes info */
  std::vector<tc_node_info> node_infos;
  tc_get_cluster_nodes_info(thd, verbose, node_infos);

  /* Send result */
  for(const tc_node_info &one_node : node_infos) {
    protocol->start_row();
    protocol->store(one_node.server_name.c_str(), system_charset_info);
    protocol->store(one_node.host.c_str(), system_charset_info);
    protocol->store(one_node.port);
    if(verbose) {
      protocol->store(one_node.user.c_str(), system_charset_info);
      protocol->store(one_node.passwd.c_str(), system_charset_info);
    }
    protocol->store(one_node.wrapper.c_str(), system_charset_info);
    protocol->store(one_node.version.c_str(), system_charset_info);
    protocol->store(one_node.get_node_status().c_str(), system_charset_info);
    if(verbose) {
      std::string feature_info_json;
      one_node.make_feature_info_json(feature_info_json);
      protocol->store(feature_info_json.c_str(), system_charset_info);
    }
    if (protocol->end_row())
      DBUG_RETURN(TRUE);
  }

  my_eof(thd);
  DBUG_RETURN(FALSE);
}