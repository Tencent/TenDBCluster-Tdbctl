/*
  Copyright (C) 2020 THL A29 Limited, a Tencent company. All rights reserved.
*/

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif
#include <my_global.h>
#include <my_thread.h>
#include <sql_class.h>
#include <table.h>
#include <mysql/plugin.h>
#include <mysql.h>
#include "log.h"

#include <map>
#include <string>

#include "tc_base.h"

#include "dbm.h"

using std::map;
using std::string;

#define SQL_SHOW_SLAVE_STATUS_STR "SHOW SLAVE STATUS"
#define SQL_SHOW_TC_IS_PRIMARY_STR "SHOW GLOBAL STATUS LIKE 'Tc_is_primary'"

#define ERR_IDENTIFY_SELF_STR                                                  \
  "failed to identify current server; check mysql.servers for possible causes"
#define ERR_CANNOT_FIND_PRIMARY_STR                                            \
  "cannot find a valid Primary node; check "                                   \
  "information_schema." DBM_TDBCTL_NODES_I_S_NAME " for possible causes"

/* Role: Primary */
#define CLUSTER_ROLE_PRIMARY_STR "Primary"
/* Role: a replica of the Primary node */
#define CLUSTER_ROLE_SECONDARY_STR "Secondary"
/* Role: a Primary node that is a replica, which normally should not happen */
#define CLUSTER_ROLE_FALSE_PRIMARY_STR "FalsePrimary"
/* Role: not a Primary & not a replica of anyone */
#define CLUSTER_ROLE_STANDALONE_STR "Standalone"
/* Role: cannot identify */
#define CLUSTER_ROLE_UNKNOWN_STR "Unknown"

/* Status: OK */
#define NODE_STATUS_ONLINE_STR "Online"
/* Status: for secondary nodes, replication is not working correctly */
#define NODE_STATUS_OFFLINE_STR "Offline"
/* Status: cannot connect to node */
#define NODE_STATUS_UNREACHABLE_STR "Unreachable"
/* Status: unexpected error */
#define NODE_STATUS_ERROR_STR "Error"

/* Indices of columns from SHOW SLAVE STATUS results */
#define MASTER_HOST_IDX 1        /* Master_Host */
#define MASTER_PORT_IDX 3        /* Master_Port */
#define SLAVE_IO_RUNNING_IDX 10  /* Slave_IO_Running */
#define SLAVE_SQL_RUNNING_IDX 11 /* Slave_SQL_Running */

#define enable_primary_error(A, B)                                             \
  do {                                                                         \
    if ((A)) {                                                                 \
      my_error(ER_TCADMIN_ENABLE_PRIMARY, MYF(0), (B));                        \
    } else {                                                                   \
      sql_print_warning((B));                                                  \
    }                                                                          \
  } while (0)

static inline std::string get_error_msg(const tc_exec_info &exec_info) {
  stringstream ss;
  if (exec_info.err_code)
    ss << "Error " << exec_info.err_code << ": " << exec_info.err_msg;
  return ss.str();
}

ST_FIELD_INFO tdbctl_nodes_fields_info[] = {
    {"SERVER_NAME", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, "Server_name",
     SKIP_OPEN_TABLE},
    {"HOST", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, "Host", SKIP_OPEN_TABLE},
    {"PORT", MY_INT32_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONG, 0, 0, "Port",
     SKIP_OPEN_TABLE},
    {"REPLICATION_MASTER", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0,
     "Replication_master", SKIP_OPEN_TABLE},
    {"CLUSTER_ROLE", 64, MYSQL_TYPE_STRING, 0, 0, "Cluster_role",
     SKIP_OPEN_TABLE},
    {"STATUS", 64, MYSQL_TYPE_STRING, 0, 0, "Status", SKIP_OPEN_TABLE},
    {"MESSAGE", 512, MYSQL_TYPE_STRING, 0, 0, "Message", SKIP_OPEN_TABLE},
    {0, 0, MYSQL_TYPE_NULL, 0, 0, 0, 0}};

/*
  Compare host and port, figure out who's the master and return the master's
  server_name.
*/
static std::string
get_repl_master_name(const std::map<std::string, AUTH_INFO> &auths,
                     const std::string &host, uint port) {
  map<string, AUTH_INFO>::const_iterator it;
  for (it = auths.begin(); it != auths.end(); ++it) {
    AUTH_INFO auth = it->second;
    if (!my_strcasecmp(system_charset_info, host.c_str(), auth.host.c_str()) &&
        port == auth.port)
      return it->first;
  }
  return "<unknown_server>";
}

/*
  A Secondary node is ONLINE when BOTH conditions are satisfied:
    1. Slave_IO_Running:  Yes
    2. Slave_SQL_Running: Yes
  Otherwise, we consider it OFFLINE.
*/
static std::string get_secondary_status(const std::string &slave_io_status,
                                        const std::string &slave_sql_status) {
  const char *ok = "Yes";
  if (!my_strcasecmp(system_charset_info, slave_io_status.c_str(), ok) &&
      !my_strcasecmp(system_charset_info, slave_sql_status.c_str(), ok))
    return NODE_STATUS_ONLINE_STR;
  return NODE_STATUS_OFFLINE_STR;
}

/**
 * @brief Examine a TDBCTL node
 *
 * @param thd Thread handle (could be NULL)
 * @param conn_mgr Connection Manager, with all TDBCTL connections built
 * @param server_name Identifier of the target node
 * @param mysql Connection to the target node
 * @param[out] repl_master_name Server_name of the target node's master if it has
 * a replication master, would be an empty string otherwise
 * @param[out] cluster_role Role of the target node within the cluster, see
 * CLUSTER_ROLE_* macros for all possible values
 * @param[out] status Status of the target node, see NODE_STATUS_* macros for all
 * possible values
 * @param[out] message Error message if there's an error
 *
 * Examples:
 *     @repl_master_name   @cluster_role   @status
 * 1)                      Primary         Online      (a normally working Primary node)
 * 2)                      Standalone      Online      (a non-Primary node without a replication master)
 * 3)  TDBCTL0             Secondary       Offline     (a Secondary, a non-Primary node with a replication
 *                                                       master, but the replication has stopped due to errors)
 * 4)  TDBCTL0             Secondary       Online      (a normally working Secondary node)
 * 5)  TDBCTL0             FalsePrimary    Online      (a Primary node with a replication master,
 *                                                       this normally should not happen)
 * 6)                                      Unreachable (an unreachable node, cannot identify)
*/
static void examine_tdbctl_node(THD *thd, Cluster_conn_manager *conn_mgr,
                                const std::string &server_name, MYSQL *mysql,
                                std::string &repl_master_name,
                                std::string &cluster_role, std::string &status,
                                std::string &message) {
  MYSQL_ROW row;
  MYSQL_RES *res;
  ulonglong row_count;
  long tc_is_primary_val;
  tc_exec_info exec_info;
  Query_exec_manager slave_sts_mgr(thd), check_primary_mgr(thd);
  const map<string, AUTH_INFO> &auths = conn_mgr->get_auth_map(NODE_TYPE_CTL);

  DBUG_ENTER("examine_tdbctl_node");
  repl_master_name.clear();
  cluster_role.clear();
  status.clear();
  message.clear();

  if (mysql) {
    /* STAGE 1: Execute SHOW SLAVE STATUS to gain replication info */
    slave_sts_mgr.build_server_maps(conn_mgr);
    slave_sts_mgr.reset_error();
    slave_sts_mgr.store_exec_query(server_name, SQL_SHOW_SLAVE_STATUS_STR,
                                   NODE_TYPE_CTL);
    tc_real_query(&slave_sts_mgr, server_name, mysql, NODE_TYPE_CTL);
    if (slave_sts_mgr.get_error() || !(res = mysql_store_result(mysql))) {
      status = NODE_STATUS_ERROR_STR;
      slave_sts_mgr.get_exec_info(server_name, exec_info, NODE_TYPE_CTL);
      message = get_error_msg(exec_info);
      DBUG_VOID_RETURN;
    } else {
      row_count = mysql_num_rows(res);
      DBUG_ASSERT(row_count <= 1);
      if (row_count) {
        /* This is a replica */
        row = mysql_fetch_row(res);
        repl_master_name = get_repl_master_name(
            auths,
            string(row[MASTER_HOST_IDX],
                   mysql_fetch_lengths(res)[MASTER_HOST_IDX]),
            strtoul(row[MASTER_PORT_IDX], NULL, 10));
        status = get_secondary_status(
            string(row[SLAVE_IO_RUNNING_IDX],
                   mysql_fetch_lengths(res)[SLAVE_IO_RUNNING_IDX]),
            string(row[SLAVE_SQL_RUNNING_IDX],
                   mysql_fetch_lengths(res)[SLAVE_SQL_RUNNING_IDX]));
      }
      mysql_free_result(res);
    }

    /* STAGE 2: Check global status 'Tc_is_primary' */
    check_primary_mgr.build_server_maps(conn_mgr);
    check_primary_mgr.reset_error();
    check_primary_mgr.store_exec_query(server_name, SQL_SHOW_TC_IS_PRIMARY_STR,
                                       NODE_TYPE_CTL);
    tc_real_query(&check_primary_mgr, server_name, mysql, NODE_TYPE_CTL);
    if (check_primary_mgr.get_error() || !(res = mysql_store_result(mysql))) {
      status = NODE_STATUS_ERROR_STR;
      check_primary_mgr.get_exec_info(server_name, exec_info, NODE_TYPE_CTL);
      message = get_error_msg(exec_info);
      DBUG_VOID_RETURN;
    } else {
      row_count = mysql_num_rows(res);
      DBUG_ASSERT(row_count == 1);
      if (likely(row_count)) {
        row = mysql_fetch_row(res);
        tc_is_primary_val = strtol(row[1], NULL, 10);
        if (repl_master_name.empty()) {
          status = NODE_STATUS_ONLINE_STR;
          cluster_role = tc_is_primary_val ? CLUSTER_ROLE_PRIMARY_STR
                                           : CLUSTER_ROLE_STANDALONE_STR;
        } else { /* this is a replica */
          /* Status is already set in STAGE 1, no need to set again */
          /*
            When a node is a replica, it should not have tc_is_primary=1; if
            unfortunately that happens, we call its role False Primary.
          */
          cluster_role = tc_is_primary_val ? CLUSTER_ROLE_FALSE_PRIMARY_STR
                                           : CLUSTER_ROLE_SECONDARY_STR;
        }
      } else {
        cluster_role = CLUSTER_ROLE_UNKNOWN_STR;
        status = NODE_STATUS_ERROR_STR;
        message = "Global Status 'Tc_is_primary' does not exist";
      }
      mysql_free_result(res);
    }
  } else {
    status = NODE_STATUS_UNREACHABLE_STR;
    DBUG_VOID_RETURN;
  }

  DBUG_VOID_RETURN;
}

/**
 * @brief Check if current node is valid to enable Primary Mode
 *
 * @retval 0 OK to enable Primary Mode
 * @retval 1 Failed to pass all Primary checks
 * */
static int check_primary_conditions(THD *thd, Cluster_conn_manager *conn_mgr) {
  char errmsg[512];
  map<string, AUTH_INFO> auths;
  map<string, MYSQL *> conns;
  map<string, MYSQL *>::const_iterator conn_it;
  uint replica_cnt = 0;
  string this_server;
  string repl_master_name, cluster_role, status, message;

  if (conn_mgr->identify_self()) {
    enable_primary_error(thd, ERR_IDENTIFY_SELF_STR);
    return 1;
  }
  this_server = conn_mgr->get_my_server_name();
  auths = conn_mgr->get_auth_map(NODE_TYPE_CTL);
  conns = conn_mgr->get_conn_map(NODE_TYPE_CTL);

  /* Check self first */
  {
    examine_tdbctl_node(thd, conn_mgr, this_server, conns[this_server],
                        repl_master_name, cluster_role, status, message);
    if (cluster_role == CLUSTER_ROLE_PRIMARY_STR)
      /* Already a Primary, do nothing */
      return 0;

    /* ERROR: Current server is a replica */
    if (repl_master_name.length()) {
      snprintf(errmsg, sizeof(errmsg),
               "current server is a replica of %s; RESET SLAVE first",
               repl_master_name.c_str());
      enable_primary_error(thd, errmsg);
      return 1;
    }

    if (status != NODE_STATUS_ONLINE_STR) {
      snprintf(errmsg, sizeof(errmsg),
               "current server's status: %s, errmsg: %s", status.c_str(),
               (message.length() ? message.c_str() : "<empty>"));
      enable_primary_error(thd, errmsg);
      return 1;
    }
  }

  for (conn_it = conns.begin(); conn_it != conns.end(); ++conn_it) {
    const string &server_name = conn_it->first;
    MYSQL *mysql = conn_it->second;
    if (!strcasecmp(server_name.c_str(), this_server.c_str()))
      continue;

    examine_tdbctl_node(thd, conn_mgr, server_name, mysql, repl_master_name,
                        cluster_role, status, message);

    /* ERROR: Already exists another Primary */
    if (cluster_role == CLUSTER_ROLE_PRIMARY_STR ||
        cluster_role == CLUSTER_ROLE_FALSE_PRIMARY_STR) {
      snprintf(errmsg, sizeof(errmsg),
               "existing Primary node: %s; disable it first",
               server_name.c_str());
      enable_primary_error(thd, errmsg);
      return 1;
    }

    /* Count online replicas */
    if (repl_master_name.length() &&
        !strcasecmp(repl_master_name.c_str(), this_server.c_str()))
      ++replica_cnt;
  }

  /* ERROR: No replicas */
  if (!replica_cnt && !opt_allow_standalone_primary) {
    enable_primary_error(
        thd, "with dbm_allow_standalone_primary=OFF, current server needs at "
             "least 1 ONLINE replica to enable Primary Mode");
    return 1;
  }

  return 0;
}

int i_s_tdbctl_nodes_fill(THD *thd, TABLE_LIST *tables, Item *cond) {
  TABLE *table;
  Cluster_conn_manager *conn_mgr;
  map<string, AUTH_INFO> auths;
  map<string, MYSQL *> conns;
  map<string, MYSQL *>::const_iterator conn_it;

  /* To make sure the view is real-time, reload servers first. */
  if (servers_reload(thd)) {
    my_error(ER_SERVERS_LOAD, MYF(0));
    return 1;
  }

  if (!thd->cluster_conn_manager) {
    thd->cluster_conn_manager = new Cluster_conn_manager();
  }
  conn_mgr = thd->cluster_conn_manager;
  if (conn_mgr->refresh(FALSE, TRUE) || conn_mgr->connect(NODE_TYPE_CTL, TRUE))
    return 1;

  table = tables->table;
  auths = conn_mgr->get_auth_map(NODE_TYPE_CTL);
  conns = conn_mgr->get_conn_map(NODE_TYPE_CTL);
  for (conn_it = conns.begin(); conn_it != conns.end(); ++conn_it) {
    string repl_master_name, cluster_role, status, message;
    const string &server_name = conn_it->first;
    MYSQL *mysql = conn_it->second;
    AUTH_INFO auth = auths[server_name];

    examine_tdbctl_node(thd, conn_mgr, server_name, mysql, repl_master_name,
                        cluster_role, status, message);

    restore_record(table, s->default_values);
    /* SERVER_NAME */
    table->field[0]->store(server_name.c_str(), server_name.length(),
                           system_charset_info);
    /* HOST */
    table->field[1]->store(auth.host.c_str(), auth.host.length(),
                           system_charset_info);
    /* PORT */
    table->field[2]->store(auth.port);
    /* REPLICATION_MASTER (could be empty) */
    table->field[3]->store(repl_master_name.c_str(), repl_master_name.length(),
                           system_charset_info);
    /* CLUSTER_ROLE */
    table->field[4]->store(cluster_role.c_str(), cluster_role.length(),
                           system_charset_info);
    /* STATUS */
    table->field[5]->store(status.c_str(), status.length(),
                           system_charset_info);
    /* MESSAGE (could be empty) */
    table->field[6]->store(message.c_str(), message.length(),
                           system_charset_info);
    schema_table_store_record(thd, table);
  }

  return 0;
}

/* Enable Primary Mode */
int dbm_enable_primary(THD *thd) {
  Cluster_conn_manager *conn_mgr;

  if (thd->lex->tc_force)
    /* No checks are needed */
    goto ok;

  if (!thd->cluster_conn_manager) {
    thd->cluster_conn_manager = new Cluster_conn_manager();
  }
  conn_mgr = thd->cluster_conn_manager;
  if (conn_mgr->refresh(FALSE, TRUE) || conn_mgr->connect(NODE_TYPE_CTL, TRUE))
    return 1;
  if (check_primary_conditions(thd, conn_mgr))
    return 1;

ok:
  TDBCTL_SET_PRIMARY_MODE_ON;
  my_ok(thd);
  return 0;
}

/* Disable Primary Mode */
int dbm_disable_primary(THD *thd) {
  TDBCTL_SET_PRIMARY_MODE_OFF;
  my_ok(thd);
  return 0;
}

/* Get connect info of the Primary node within the cluster */
int dbm_get_primary(THD *thd) {
  Item *field;
  List<Item> field_list;
  Protocol *protocol = thd->get_protocol();
  Cluster_conn_manager *conn_mgr;
  map<string, AUTH_INFO> auths;
  map<string, MYSQL *> conns;
  map<string, MYSQL *>::const_iterator conn_it, found_primary;
  string this_server;
  string repl_master_name, cluster_role, status, message;

  field_list.push_back(new Item_empty_string("SERVER_NAME", NAME_CHAR_LEN));
  field_list.push_back(new Item_empty_string("HOST", NAME_CHAR_LEN));
  field_list.push_back(field = new Item_return_int("PORT", 7, MYSQL_TYPE_LONG));
  field->unsigned_flag = 1;
  field_list.push_back(
      field = new Item_return_int("IS_THIS_SERVER", 4, MYSQL_TYPE_LONG));
  field->unsigned_flag = 0;
  if (thd->send_result_metadata(&field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    return 1;

  if (!thd->cluster_conn_manager) {
    thd->cluster_conn_manager = new Cluster_conn_manager();
  }
  conn_mgr = thd->cluster_conn_manager;
  if (conn_mgr->refresh(FALSE, TRUE) || conn_mgr->connect(NODE_TYPE_CTL, TRUE))
    return 1;
  if (conn_mgr->identify_self()) {
    my_error(ER_TCADMIN_GET_PRIMARY, MYF(0), ERR_IDENTIFY_SELF_STR);
    return 1;
  }
  this_server = conn_mgr->get_my_server_name();

  auths = conn_mgr->get_auth_map(NODE_TYPE_CTL);
  conns = conn_mgr->get_conn_map(NODE_TYPE_CTL);
  for (conn_it = conns.begin(), found_primary = conns.end();
       conn_it != conns.end(); ++conn_it) {
    const string &server_name = conn_it->first;
    MYSQL *mysql = conn_it->second;

    examine_tdbctl_node(thd, conn_mgr, server_name, mysql, repl_master_name,
                        cluster_role, status, message);

    if (cluster_role == CLUSTER_ROLE_PRIMARY_STR &&
        status == NODE_STATUS_ONLINE_STR) { /* Found a valid Primary */
      if (found_primary != conns.end()) {
        /* Error: found a second primary */
        my_error(ER_TCADMIN_GET_PRIMARY, MYF(0), "found more than one Primary");
        return 1;
      }
      found_primary = conn_it;
    }
  }

  if (found_primary != conns.end()) { /* Found one and only valid Primary */
    const string &server_name = found_primary->first;
    const AUTH_INFO &auth = auths[server_name];
    protocol->start_row();
    /* SERVER_NAME */
    protocol->store(server_name.c_str(), server_name.length(),
                    system_charset_info);
    /* HOST */
    protocol->store(auth.host.c_str(), auth.host.length(), system_charset_info);
    /* PORT */
    protocol->store(auth.port);
    /* IS_THIS_SERVER */
    protocol->store(static_cast<int>(
        !strcasecmp(server_name.c_str(), this_server.c_str())));

    protocol->end_row();
    my_eof(thd);
    return 0;
  }

  my_error(ER_TCADMIN_GET_PRIMARY, MYF(0), ERR_CANNOT_FIND_PRIMARY_STR);
  return 1;
}

/* Enable Primary Mode on server startup */
int dbm_startup_enable_primary() {
  int err = 0;
  Cluster_conn_manager *conn_mgr = new Cluster_conn_manager;

  if (conn_mgr->refresh(FALSE, TRUE) ||
      conn_mgr->connect(NODE_TYPE_CTL, TRUE) ||
      check_primary_conditions(NULL, conn_mgr))
    err = 1;

  if (!err) { /* OK */
    TDBCTL_SET_PRIMARY_MODE_ON;
  }
  delete conn_mgr;
  return err;
}
