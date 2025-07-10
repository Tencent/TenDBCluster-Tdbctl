/*
  Copyright (C) 2020 Tencent. All rights reserved.
*/
#include <map>
#include <string>
#include <vector>
#include "sql_class.h"
#include "sql_show.h" // IS_COLUMNS_* indices

#include "tc_base.h"

using std::string;
using std::map;

#define SQL_SELECT_MYSQL_SERVERS  "SELECT * FROM mysql.servers"

#define EMPTY_STRING std::string()
#define DATA_STRING(A, B, C) ((A)[(C)] ? std::string((A)[(C)], (B)[(C)]) : EMPTY_STRING)

#define DIFF_SERVER_NAME (1 << 0)
#define DIFF_HOST (1 << 1)
#define DIFF_DB (1 << 2)
#define DIFF_USER_NAME (1 << 3)
#define DIFF_PASSWORD (1 << 4)
#define DIFF_PORT (1 << 5)
#define DIFF_SOCKET (1 << 6)
#define DIFF_WRAPPER (1 << 7)
#define DIFF_OWNER (1 << 8)

struct Mysql_server_record;
typedef std::map<std::string, Mysql_server_record> Server_records;

struct Mysql_server_record {
    string server_name;
    string host;
    string db;
    string user_name;
    string password;
    ulong port;
    string socket;
    string wrapper;
    string owner;
};

static ulonglong server_record_cmp(const Mysql_server_record &a,
                                   const Mysql_server_record &b) {
  ulonglong ret = 0;
  if (a.server_name != b.server_name)
    ret |= DIFF_SERVER_NAME;
  if (a.host != b.host)
    ret |= DIFF_HOST;
  if (a.db != b.db)
    ret |= DIFF_DB;
  if (a.user_name != b.user_name)
    ret |= DIFF_USER_NAME;
  if (a.password != b.password)
    ret |= DIFF_PASSWORD;
  if (a.port != b.port)
    ret |= DIFF_PORT;
  if (a.socket != b.socket)
    ret |= DIFF_SOCKET;
  if (a.wrapper != b.wrapper)
    ret |= DIFF_WRAPPER;
  if (a.owner != b.owner)
    ret |= DIFF_OWNER;
  return ret;
}

static bool get_mysql_server_records_for_server(THD *thd,
                                                const string &server_name,
                                                MYSQL *mysql,
                                                Server_records &records,
                                                enum_node_type node_type){
  char query_buff[512];
  MYSQL_ROW row;
  MYSQL_RES *res;
  Cluster_conn_manager *conn_mgr;
  Query_exec_manager query_mgr(thd);
  DBUG_ENTER("get_mysql_server_records_for_server");

  snprintf(query_buff, sizeof(query_buff), SQL_SELECT_MYSQL_SERVERS);

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
    Mysql_server_record rec;
    rec.server_name = DATA_STRING(row, mysql_fetch_lengths(res), 0);
    rec.host = DATA_STRING(row, mysql_fetch_lengths(res), 1);
    rec.db = DATA_STRING(row, mysql_fetch_lengths(res), 2);
    rec.user_name = DATA_STRING(row, mysql_fetch_lengths(res), 3);
    rec.password = DATA_STRING(row, mysql_fetch_lengths(res), 4);
    rec.port = strtoul(row[5], NULL, 10);
    rec.socket = DATA_STRING(row, mysql_fetch_lengths(res), 6);
    rec.wrapper = DATA_STRING(row, mysql_fetch_lengths(res), 7);
    rec.owner  = DATA_STRING(row, mysql_fetch_lengths(res), 8);
    records[rec.server_name] = rec;
  }
  mysql_free_result(res);

  DBUG_RETURN(FALSE);
}

bool cmp_between_curr_ctl_and_other_nodes(Server_records& my_recs, Server_records& tmp_recs,
                                          enum_node_type node_type, string my_server_name) {
  bool success = true;
  for(map<string, Mysql_server_record>::iterator it = my_recs.begin();
     it != my_recs.end(); ++it) {
    const string &server_name = it->first;
    const Mysql_server_record &rec = it->second;
    ulonglong flags;
    // compare the routing info of tdbtcl nodes with tdbtcl primary node
    if (node_type == NODE_TYPE_CTL)
    {
      if (!(flags = server_record_cmp(rec, tmp_recs[server_name])))
        /* consistent definition, skip */
        continue;
      else {
        success = false;
      }
    } // compare the routing info of spider_master nodes with tdbtcl primary node
    else if (node_type == NODE_TYPE_SPIDER &&
             (rec.wrapper == string(get_wrapper_name_by_node_type(NODE_TYPE_SPIDER))
              || rec.wrapper == string(get_wrapper_name_by_node_type(NODE_TYPE_REMOTE))
              || rec.server_name == my_server_name))
    {
      if (!(flags = server_record_cmp(rec, tmp_recs[server_name])))
        /* consistent definition, skip */
        continue;
      else {
        success = false;
      }
    } // compare the routing info of spider_slave nodes with tdbtcl primary node
    else if (node_type == NODE_TYPE_SPIDER_SLAVE &&
               (rec.wrapper == string(get_wrapper_name_by_node_type(NODE_TYPE_SPIDER_SLAVE))
                || rec.wrapper == string(get_wrapper_name_by_node_type(NODE_TYPE_REMOTE_SLAVE))
                || rec.server_name == my_server_name))
    {
      string tmp_server_name = server_name;
      if (rec.wrapper == string(get_wrapper_name_by_node_type(NODE_TYPE_REMOTE_SLAVE))){
        trim_server_name_slave_suffix(tmp_server_name);
      }
      if (!(flags = server_record_cmp(rec, tmp_recs[tmp_server_name])))
        /* consistent definition, skip */
        continue;
      else {
        // Because the sever_name and wrapper stored into the mysql.server of spider_slave nodes are
        // converted. SPT_SLAVEn -> SPTn, mysql_slave -> mysql
        // We need to perform a conversion before comparing them.
        if (flags & (DIFF_SERVER_NAME | DIFF_WRAPPER)){
          string tmp_wrapper = rec.wrapper;
          trim_wrapper_name_slave_suffix(tmp_wrapper);
          if(tmp_server_name == tmp_recs[tmp_server_name].server_name
             && tmp_wrapper == tmp_recs[tmp_server_name].wrapper)
            continue;
          else {
            success = false;
          }
        } else {
          success = false;
        }
      }
    }
  }

  if (!success) {
    return true;
  }
  return false;
}

bool tdbctl_check_routing(THD *thd) {
  Cluster_conn_manager *conn_mgr;
  Query_exec_manager query_mgr(thd);
  DBUG_ENTER("tdbctl_check_routing");

  DBUG_ASSERT(thd->cluster_conn_manager);
  conn_mgr = thd->cluster_conn_manager;
  string err_str = "The routing info of the following nodes is inconsistent with tdbctl primary: ";
  bool success = true;

  /* TODO: allow specification of node types */
  if (conn_mgr->refresh(TRUE, FALSE))
    DBUG_RETURN(TRUE);
  if (conn_mgr->identify_self()) {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0),
             "cannot identify current server");
    DBUG_RETURN(TRUE);
  }

  string my_server = conn_mgr->get_my_server_name();
  MYSQL *my_conn = conn_mgr->get_conn_map(NODE_TYPE_CTL).at(my_server);
  Server_records my_records;

  if(get_mysql_server_records_for_server(thd, my_server, my_conn, my_records, NODE_TYPE_CTL)){
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0),
             "failed to get mysql.server records from current server");
    DBUG_RETURN(TRUE);
  }

  enum_node_type type = NODE_TYPE_SPIDER;
  while (type != NODE_TYPE_END) {
    map<string, MYSQL *>::const_iterator conn_it;
    map<string, MYSQL *> conns = conn_mgr->get_conn_map(type);
    for (conn_it = conns.begin(); conn_it != conns.end(); ++conn_it) {
      const string &server_name = conn_it->first;
      MYSQL *mysql = conn_it->second;

      Server_records cmp_records;

      if(get_mysql_server_records_for_server(thd, server_name, mysql, cmp_records, type)){
        my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0),
                 "failed to get mysql.server records from server for comparison");
        DBUG_RETURN(TRUE);
      }

      if(cmp_between_curr_ctl_and_other_nodes(my_records, cmp_records, type, my_server))
      {
        success = false;
        err_str.append(server_name + " ");
      }
    }

    if (type == NODE_TYPE_SPIDER)
      type = NODE_TYPE_SPIDER_SLAVE;
    else if (type == NODE_TYPE_SPIDER_SLAVE)
      type = NODE_TYPE_CTL;
    else
      type = NODE_TYPE_END;
  }

  if (!success) {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0),
             err_str.c_str());
    DBUG_RETURN(TRUE);
  }

  DBUG_RETURN(FALSE);
}
