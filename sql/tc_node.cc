/*
     Copyright (C) 2020 Tencent.  All rights reserved.
*/

/*
Add for node's control
*/
#include "tc_node.h"
#include "my_global.h"
#include "set_var.h"
#include "sql_base.h"         // open_tables, open_and_lock_tables,
#include "log.h"
#include "sql_servers.h"
#include "tc_base.h"
#include "tc_dump.h"
#include "tc_restore.h"
#include <thread>
#include <fstream>
#include <iostream>
#include <functional>

/**
 use mysqldump to backup node's schema
 @returns
    0 is success
    1 is error
*/
int tc_dump_node_schema(
        const char *host,
        uint port,
        const char *user,
        const char *password,
        const char *file,
        const char *err_file,
        const char *wrapper)
{
  string space = " ";
  string dump_cmd, dump_bin, dump_options;

#if defined (_WIN32)
  dump_bin = "mysqldump";
#else
  dump_bin = mysql_home_ptr;
  dump_bin += "/bin/mysqldump";
#endif
  dump_options = "--single-transaction --no-autocommit=FALSE  --skip-opt --create-options --routines "
                "--quick --no-data --all-databases";
  dump_options += space + "-r" + file + space + "--log-error=" + file + ".err";
	dump_options += space + "-u" + user + space + "-p" + password;
  size_t pwd_len = strlen(password);
  size_t pwd_pos = dump_options.size() - pwd_len;
  dump_options += space + "-P" + to_string(port) + space+ "-h" + host;
  
  // tdbctl enable gtid mode, therefore we need to get gtid info
  if (strcasecmp(wrapper, TDBCTL_WRAPPER) == 0)
  {
    dump_options += space + "--set-gtid-purged=auto --print-tc-admin-info";
  }
  if (tc_skip_dump_db_list)
  {
    size_t pos = 0;
    string dbs = tc_skip_dump_db_list;
    string delimiter = ",";
    string token;
    while ((pos = dbs.find(delimiter)) != std::string::npos) {
      token = dbs.substr(0, pos);
      dump_options += space + "--ignore-database=" + token;
      dbs.erase(0, pos + delimiter.length());
    }
    token = dbs;
    dump_options += space + "--ignore-database=" + token;
  }

  MYSQL *conn = tc_conn_connect(host, port, user, password, wrapper);
  if (conn == NULL)
  {
    std::string ipport = std::string(host) + "#" + std::to_string(port);
    my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
    return 1;
  }
  MYSQL_GUARD(conn);
  string charset;
  if(tc_get_variable_value(conn, "@@character_set_server", charset)) {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), "Failed to get value of variable \'character_set_server\'");
    return 1;
  }
  dump_options += space + "--default-character-set=" + charset;
  std::string dump_options_log = dump_options;
  dump_options_log.replace(pwd_pos, pwd_len, "xxxxx");

  dump_cmd = dump_bin + space + dump_options;
  std::string dump_cmd_log = dump_bin + space + dump_options_log;

  if (tc_system(dump_cmd.c_str(), dump_cmd_log.c_str()) != 0)
  {
    my_error(ER_TCADMIN_DUMP_NODE_ERROR, MYF(0), file, host, port, err_file);
    return 1;
  }

  sql_print_information("Successfully dump schema from %s#%u to file %s.", 
                        host, port, file);

  return 0;
}

/**
  use mysqlconn to dump grant info from node

  @returns
  0 represents success
  1 represents error
*/
int tc_dump_node_grant(
    const char *host,
    uint port,
    const char *user,
    const char *password,
    const char *file,
    const char *wrapper)
{
  MYSQL_RES *res, *user_res, *grant_res;
  ofstream outfile;
  outfile.open(file);
  MYSQL *conn = tc_conn_connect(host, port, user, password, wrapper);
  if (!conn) 
  {
    std::string ipport = std::string(host) + "#" + std::to_string(port);
    my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
    return 1;
  }
  MYSQL_GUARD(conn);
  std::string sql = "select user, host from mysql.user where user not in ('ADMIN','yw','dba_bak_all_sel')";
  res = tc_exec_sql_with_result(conn, sql);
  MYSQL_RES_GUARD(res);

  if (res)
  {
    MYSQL_ROW row = NULL;
    while ((row = mysql_fetch_row(res)))
    {
      std::string user_str, host_str;
      MYSQL_ROW create_user_row = NULL, show_grant_row = NULL;
      host_str = row[1];
      user_str = row[0];
      sql = "show create user_str `" + user_str + "`@`" + host_str + "`";
      user_res = tc_exec_sql_with_result(conn, sql);
      if (user_res && (create_user_row = mysql_fetch_row(user_res)))
      {
        std::string create_user_sql = create_user_row[0];
        std::string sub1 = "CREATE USER IF NOT EXISTS";
        std::string sub2 = "CREATE USER";
        size_t pos = create_user_sql.find(sub1);
        if (pos == std::string::npos)
        {
          pos = create_user_sql.find(sub2);
          create_user_sql.replace(pos, sub2.length(), sub1);
        }
        outfile << create_user_sql << ";" << endl;
      }
      MYSQL_RES_GUARD(user_res);

      sql = "show grants for `" + user_str + "`@`" + host_str + "`";
      grant_res = tc_exec_sql_with_result(conn, sql);
      if(grant_res && (show_grant_row = mysql_fetch_row(grant_res)))
      {
        outfile << show_grant_row[0] << ";" << endl;
      }
      MYSQL_RES_GUARD(grant_res);
    }
  }
  outfile.close();

  sql_print_information("success dump grant to file %s.", file);

  return 0;
}

/**
 use mysql client to restore node's schema/grant
 
  @returns
  0 is success
  1 is error
*/
int tc_restore_to_node(
        const char *host,
        uint port,
        const char *user,
        const char *password,
        const char *file,
        const char *wrapper)
{
  MYSQL *conn = tc_conn_connect(host, port, user, password, wrapper);
  MYSQL_GUARD(conn);
  if (!conn) 
  {
    std::string ipport = std::string(host) + "#" + std::to_string(port);
    my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
    return 1;
  }
  std::string sql;
  tc_exec_info exec_info;
  exec_info.err_code = 0;
  exec_info.err_msg = "";
  
  // for spider node, we need to disable ddl_execute_by_ctl feature of the spider
  bool has_variable = true;
  if (strcasecmp(wrapper, SPIDER_WRAPPER) == 0)
  {
    sql = "/*!50600 set @old_ddl_execute_by_ctl = @@ddl_execute_by_ctl*/";
    if(tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      /*
        Ignore the error if it's due to an unknown system variable (ER_UNKNOWN_SYSTEM_VARIABLE),
        which may occur for version compatibility. For other errors, treat as execution failure.
      */
      if (exec_info.err_code == ER_UNKNOWN_SYSTEM_VARIABLE) {
        has_variable = false;
      } else {
        my_error(ER_TCADMIN_SEND_SQL_ERR, MYF(0), exec_info.err_msg.c_str());
        return 1;
      }
    }
    sql = "/*!50600 set global ddl_execute_by_ctl=0*/";
    if(has_variable && tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      my_error(ER_TCADMIN_SEND_SQL_ERR, MYF(0), exec_info.err_msg.c_str());
      return 1;
    }
  }

  string space = " ";
  string restore_cmd, restore_bin, restore_options;
#if defined (_WIN32)
  restore_bin = "mysql";
#else
  restore_bin = mysql_home_ptr;
  restore_bin += "/bin/mysql";
#endif
  restore_options += space + "-u" + user + space + "-p" + password;
  size_t pwd_len = strlen(password);
  size_t pwd_pos = restore_options.size() - pwd_len;

  restore_options += space + "-P" + to_string(port) + space + "-h" + host + "<" + file;
  std::string restore_options_log = restore_options;
  restore_options_log.replace(pwd_pos, pwd_len, "xxxxx");

  restore_cmd = restore_bin + restore_options + space + ">&" + file + ".err";
  std::string restore_cmd_log = restore_bin + restore_options_log + space + ">&" + file + ".err";

  std::string err_file;
  err_file = std::string(file) + ".err"; 
  
  if (tc_system(restore_cmd.c_str(), restore_cmd_log.c_str()) != 0)
  {
    my_error(ER_TCADMIN_RESTORE_NODE_ERROR, MYF(0), host, port);
    return 1;
  }

  if (strcasecmp(wrapper, SPIDER_WRAPPER) == 0)
  {
    sql = "/*!50600 set global ddl_execute_by_ctl = @old_ddl_execute_by_ctl */";
    if(has_variable && tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      my_error(ER_TCADMIN_SEND_SQL_ERR, MYF(0), exec_info.err_msg.c_str());
      return 1;
    }
  }

  sql_print_information("success restore %s to node %s#%d", file, host, port);
  return 0;
}

bool tc_load_schema_to_new_node(THD *thd, LEX *lex, FOREIGN_SERVER *dump_server)
{
  // sql_yacc.yy had filter wrapper name according to tc_with_schema option
  DBUG_ASSERT((strcasecmp(lex->server_options.get_scheme(), SPIDER_WRAPPER) == 0) ||
               (strcasecmp(lex->server_options.get_scheme(), SPIDER_SLAVE_WRAPPER) == 0) ||
               (strcasecmp(lex->server_options.get_scheme(), TDBCTL_WRAPPER) == 0));

  // disable internal dump/restore
  if (!tc_enable_internal_dump)
  {
    return false;
  }

  if (dump_server == NULL)
  {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), "Dump source server is not specified");
    return true;
  }

  /*
    Construct filename for mysqldump output
    Format: wrapper_from_ip_port_to_ip_port_YYYYmmdd_HHMMSS.ms_tid.sql
  */
  char schema_path[FN_REFLEN + 1];
  char *p1 = my_stpnmov(schema_path, mysql_tmpdir, sizeof(schema_path));
  QUERY_START_TIME_INFO query_time_info;
  thd->get_time(&query_time_info);
  string query_time_str = timeval_to_str(query_time_info.start_time, "%Y%m%d_%H%M%S", 6);
  my_snprintf(p1, sizeof(schema_path) - (p1 - schema_path), "/%s_from_%s_%ld_to_%s_%ld_%s_%u.sql",
              dump_server->scheme, dump_server->host, (unsigned int)dump_server->port, 
              lex->server_options.get_host(), (unsigned int)lex->server_options.get_port(), 
              query_time_str.c_str(), thd->thread_id());
  
  std::string err_file = std::string(schema_path) + ".err";

  if (tc_dump_node_schema(
          dump_server->host,
          dump_server->port,
          dump_server->username,
          dump_server->password,
          schema_path,
          err_file.c_str(),
          dump_server->scheme))
  {
    return true;
  }

  if (tc_restore_to_node(lex->server_options.get_host(),
                         lex->server_options.get_port(),
                         lex->server_options.get_username(),
                         lex->server_options.get_password(),
                         schema_path,
                         lex->server_options.get_scheme()))
  {
    return true;
  }

  return false;
}

/**
 * Find an appropriate server node to dump schema from
 * 
 * This function selects a suitable cluster server node to use as the source for schema dumping.
 * The selection follows a priority order: 1) Not the newly added node, 2) Local server, 
 * 3) Alphabetically first server.
 *
 * @param thd   The current thread handler
 * @param lex   The LEX structure containing server options
 * @return      A pair containing:
 *              - FOREIGN_SERVER*: Pointer to selected server node (NULL if none found)
 *              - std::string: Error message if any (empty if successful)
 */
std::pair<FOREIGN_SERVER *, std::string> tc_find_dump_source_node(THD *thd, LEX *lex)
{
  // Verify the wrapper type is valid (SPIDER, SPIDER_SLAVE or TDBCTL)
  DBUG_ASSERT((strcasecmp(lex->server_options.get_scheme(), SPIDER_WRAPPER) == 0) ||
               (strcasecmp(lex->server_options.get_scheme(), SPIDER_SLAVE_WRAPPER) == 0) ||
               (strcasecmp(lex->server_options.get_scheme(), TDBCTL_WRAPPER) == 0));

  const char *warning_format = "'TDBCTL CREATE NODE WITH SCHEMA' failed to find dump source: %s";
  FOREIGN_SERVER* dump_server = NULL;
  std::string msg = "";
  list<FOREIGN_SERVER *> server_list;

  /*
    get spider_list or tdbctl_list from mysql.servers, exclude slave spiders.
    For slave_spider nodes, we use spider nodes's schema.
    (use spider_slave's schema for spider_slave node, maybe not consistent with master?)
  */
  if ((strcasecmp(lex->server_options.get_scheme(), SPIDER_WRAPPER) == 0) ||
      (strcasecmp(lex->server_options.get_scheme(), SPIDER_SLAVE_WRAPPER) == 0))
    get_server_by_wrapper(server_list, thd->mem_root, SPIDER_WRAPPER, FALSE);
  else
    get_server_by_wrapper(server_list, thd->mem_root, lex->server_options.get_scheme(), FALSE);

  if (server_list.empty())
  {
    msg = "No server to dump schema";
    sql_print_warning(warning_format, msg.c_str());
    return {NULL, msg};
  }

  /*
    Get current server's IP address from cluster connection manager
  */
  auto tdbctl_auth_map = thd->cluster_conn_manager->get_auth_map(NODE_TYPE_CTL);
  std::string my_server_name = thd->cluster_conn_manager->get_my_server_name();
  // Validate cluster connection manager has cached current server info
  if((my_server_name == Cluster_conn_manager::UNKOWN_SERVER_NAME) || 
     (tdbctl_auth_map.find(my_server_name) == tdbctl_auth_map.end())) {
    msg = "Failed to get current server IP address from cluster connection manager";
    sql_print_warning(warning_format, msg.c_str());
    return {NULL, msg};
  }
  std::string local_ip = tdbctl_auth_map.at(my_server_name).host;

  /*
    Node selection algorithm with priority:
    1) Exclude the newly added node (host/port doesn't match)
    2) Prefer local server (IP matches current node)
    3) Fall back to first server in alphabetical order
  */
  for(auto it = server_list.begin(); it != server_list.end(); it++) {
    // Skip the newly added node (current operation target)
    if(!(strcasecmp((*it)->host, lex->server_options.get_host()) == 0 &&
       (*it)->port == lex->server_options.get_port())) {
      // First candidate server
      if (dump_server == NULL) {
        dump_server = *it;
      } 
      // Prefer local server (localhost/127.0.0.1 or matching IP)
      if (!strcasecmp((*it)->host, local_ip.c_str()) || 
          !strcasecmp((*it)->host, "127.0.0.1") ||
          !strcasecmp((*it)->host, "localhost")) {
        dump_server = *it;
        break;  // Found optimal candidate, stop searching
      }
    }
  }

  if(!dump_server) {
    msg = "No valid dump source node was found by the selection strategy";
    sql_print_warning(warning_format, msg.c_str());
    return {NULL, msg};
  }

  return {dump_server, msg};
}

/**
 * Performs schema backup from a source node to a specified file.
 * 
 * @param thd          Thread handler containing session context
 * @param lex          LEX structure with server connection options
 * @param dump_server  Source server to backup from (must not be NULL)
 * 
 * @retval pair.first  bool: false if backup succeeded or was skipped (with warning)
 *                     true if backup failed
 * @retval pair.second string: Backup file path if succeeded, empty otherwise
 * 
 * @note
 * - Backup file format: from_[server_name]_[host]_[port]_[timestamp]_[thread_id].sql
 * - Uses mysql_tmpdir as the base directory for backup files
 * - For skipped backups (tc_enable_internal_dump=false), only a warning is raised
 */
std::pair<bool, std::string> tc_backup_from_source_node(THD *thd, LEX *lex, const FOREIGN_SERVER *dump_server) 
{
  // Validate wrapper type (pre-filtered by sql_yacc.yy)
  DBUG_ASSERT((strcasecmp(lex->server_options.get_scheme(), SPIDER_WRAPPER) == 0) ||
               (strcasecmp(lex->server_options.get_scheme(), SPIDER_SLAVE_WRAPPER) == 0) ||
               (strcasecmp(lex->server_options.get_scheme(), TDBCTL_WRAPPER) == 0));

  // Skip backup if internal dump is disabled (only show warning)
  if (!tc_enable_internal_dump)
  {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_TCADMIN_CREATE_NODE_ERROR,
                        "backup was skipped: %s", "tc_enable_internal_dump is disabled");
    return {false, ""};
  }

  if (dump_server == NULL)
  {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), "Dump source server is not specified");
    return {true, ""};
  }

  /* Construct backup filename with format:
   * from_[server_name]_[host]_[port]_[YYYYmmdd_HHMMSS.ms]_[thread_id].sql
   */
  char schema_path[FN_REFLEN + 1];
  char *p1 = my_stpnmov(schema_path, mysql_tmpdir, sizeof(schema_path));
  struct timeval current_time;
  my_micro_time_to_timeval(my_micro_time(), &current_time);
  string cur_time_str = timeval_to_str(current_time, "%Y%m%d_%H%M%S", 6);
  my_snprintf(p1, sizeof(schema_path) - (p1 - schema_path), "/from_%s_%s_%ld_%s_%u.sql",
              dump_server->server_name, dump_server->host, (unsigned int)dump_server->port, 
              cur_time_str.c_str(), thd ? thd->thread_id() : 0);
  
  std::string dump_err_file(schema_path);
  dump_err_file += ".err";

  /*
    Connect to dump source node
  */
  MYSQL *conn = tc_conn_connect(dump_server->host, dump_server->port, 
                                dump_server->username, dump_server->password, 
                                dump_server->scheme);
  char err_buf[1024];
  if (!conn) {
    my_snprintf(err_buf, sizeof(err_buf), "failed to connect to the dump source node %s", 
                dump_server->server_name);
    my_error(ER_TCADMIN_CREATE_NODE_ERROR, MYF(0), err_buf);
    return {true, ""};
  }
  MYSQL_GUARD(conn);

  const string sys_var_open_cache = "@@GLOBAL.table_open_cache";
  const string sys_var_def_cache = "@@GLOBAL.table_definition_cache";
  string old_open_cache, old_def_cache;

  /*
    Get table_open_cache and table_definition_cache of the dump source node
  */
  if(tc_get_variable_value(conn, sys_var_open_cache, old_open_cache) || 
      tc_get_variable_value(conn, sys_var_def_cache, old_def_cache)) {
    my_snprintf(err_buf, sizeof(err_buf), "failed to get table_open_cache or "
                "table_definition_cache of the dump source node %s", 
                dump_server->server_name);  
    my_error(ER_TCADMIN_CREATE_NODE_ERROR, MYF(0), err_buf);
    return {true, ""};
  }

  /*
    Reset table_open_cache and table_definition_cache of the dump source node
    when finished
  */
  uint err_code;
  string err_msg;
  bool sys_vars_need_reset = true;
  bool reset_vars_failed = false;
  std::function<void()> reset_sys_vars_func = [&]() {
    if (!sys_vars_need_reset) {
      return;
    }
    sys_vars_need_reset = false;
    err_code = tc_set_variable_value(conn, sys_var_open_cache, 
                              old_open_cache, err_msg);
    if (err_code) {
      reset_vars_failed = true;
      sql_print_error("failed to reset table_open_cache of node %s, "
                      "err_code: %u, err_msg: %s",
                      dump_server->server_name, err_code, err_msg.c_str());
    }
    
    err_code = tc_set_variable_value(conn, sys_var_def_cache, 
                              old_def_cache, err_msg);
    if (err_code) {
      reset_vars_failed = true;
      sql_print_error("failed to reset table_definition_cache of node %s, "
                      "err_code: %u, err_msg: %s",
                      dump_server->server_name, err_code, err_msg.c_str());
    }
    if (reset_vars_failed) {
      my_snprintf(err_buf, sizeof(err_buf), "failed to reset table_open_cache or "
                "table_definition_cache of the dump source node %s", 
                dump_server->server_name);  
      my_error(ER_TCADMIN_CREATE_NODE_ERROR, MYF(0), err_buf);
    }
  };

  auto sys_vars_guard = TC_SCOPE_EXIT<std::function<void()>>(reset_sys_vars_func);

  /*
    Set table_open_cache and table_definition_cache of the dump source node
    to the values of the current primary tdbctl to minimize the risk of OOM.
  */
  string my_table_open_cache = to_string(table_cache_size);
  string my_table_def_cache = to_string(table_def_size);
  err_code = tc_set_variable_value(conn, sys_var_open_cache, 
                                  my_table_open_cache, err_msg);
  if (err_code) {
    my_snprintf(err_buf, sizeof(err_buf), "failed to set table_open_cache of "
              "the dump source node %s, err_code: %u, err_msg: %s", 
              dump_server->server_name, err_code, err_msg.c_str());  
    my_error(ER_TCADMIN_CREATE_NODE_ERROR, MYF(0), err_buf);
    return {true, ""};
  }
  err_code = tc_set_variable_value(conn, sys_var_def_cache, 
                                  my_table_def_cache, err_msg);
  if (err_code) {
    my_snprintf(err_buf, sizeof(err_buf), "failed to set table_definition_cache of "
              "the dump source node %s, err_code: %u, err_msg: %s", 
              dump_server->server_name, err_code, err_msg.c_str());
    my_error(ER_TCADMIN_CREATE_NODE_ERROR, MYF(0), err_buf);
    return {true, ""};
  }

  /*
    Dump schema from the dump source node
  */
  bool dumper_failed = false;
  if((thd == NULL) || thd->variables.tc_use_internal_backup_tool) {
    dumper_failed = Dump_Handler::tc_dump_node_schema(thd, 
                          dump_server, 
                          schema_path, 
                          dump_err_file);
  } else {
    dumper_failed = (tc_dump_node_schema(
                          dump_server->host,
                          dump_server->port,
                          dump_server->username,
                          dump_server->password,
                          schema_path,
                          dump_err_file.c_str(),
                          dump_server->scheme) != 0);
  }

  if (dumper_failed)
  {
    return {true, ""};  // Backup failed
  }

  reset_sys_vars_func();
  if(reset_vars_failed) {
    return {true, ""};
  }

  return {false, schema_path};   // Backup succeeded
}

/**
 * @brief Restores data from a backup file to a specified database node
 * 
 * This function is designed to be called as a worker thread in a multi-threaded
 * data import system. It handles the complete restore process including:
 * - Establishing connection to target node
 * - Handling special configurations for Spider nodes
 * - Executing the actual restore command
 * - Error handling and status reporting
 * 
 * @param thd             Thread handle
 * @param one_server  Server options for the target node            
 * @param file            Path to the backup file to restore
 * @param restore_result  Output parameter for restore status:
 *                       - first: bool (true if restore failed)
 *                       - second: string (error message if failed)
 */
void tc_restore_to_node_worker(
        THD *thd,
        const Server_options &one_server,
        const char *src_file,
        std::pair<bool, std::string> &restore_result)
{
  restore_result.first = false;
  restore_result.second = "";

  const char *host = one_server.get_host() ? one_server.get_host() : "";
  uint port = (uint)one_server.get_port();
  const char *user = one_server.get_username() ? one_server.get_username() : "";
  const char *password = one_server.get_password() ? one_server.get_password() : "";
  const char *wrapper = one_server.get_scheme() ? one_server.get_scheme() : "";

  MYSQL *conn = tc_conn_connect(host, port, user, password, wrapper);
  MYSQL_GUARD(conn);
  if (!conn) 
  {
    std::string ipport = std::string(host) + "#" + std::to_string(port);
    restore_result.first = true;
    restore_result.second = tc_get_error_msg(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
    return;
  }
  std::string sql;
  tc_exec_info exec_info;
  exec_info.err_code = 0;
  exec_info.err_msg = "";
  
  // for spider/spider_slave node, we need to disable ddl_execute_by_ctl feature of the spider
  bool has_variable = true;
  if ((strcasecmp(wrapper, SPIDER_WRAPPER) == 0) || (strcasecmp(wrapper, SPIDER_SLAVE_WRAPPER) == 0))
  {
    sql = "/*!50600 set @old_ddl_execute_by_ctl = @@global.ddl_execute_by_ctl*/";
    if(tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      /*
        Ignore the error if it's due to an unknown system variable (ER_UNKNOWN_SYSTEM_VARIABLE),
        which may occur for version compatibility. For other errors, treat as execution failure.
      */
      if (exec_info.err_code == ER_UNKNOWN_SYSTEM_VARIABLE) {
        has_variable = false;
      } else {
        restore_result.first = true;
        restore_result.second = tc_get_error_msg(ER_TCADMIN_SEND_SQL_ERR, MYF(0), exec_info.err_msg.c_str());
        return;
      }
    }
    sql = "/*!50600 set global ddl_execute_by_ctl=0*/";
    if(has_variable && tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      restore_result.first = true;
      restore_result.second = tc_get_error_msg(ER_TCADMIN_SEND_SQL_ERR, MYF(0), exec_info.err_msg.c_str());
      return;
    }
  }

  /* Construct restore error filename with format:
   * to_[wrapper]_[host]_[port]_[YYYYmmdd_HHMMSS.ms]_[thread_id].err
   */
  char schema_path[FN_REFLEN + 1];
  char *p1 = my_stpnmov(schema_path, mysql_tmpdir, sizeof(schema_path));
  struct timeval current_time;
  my_micro_time_to_timeval(my_micro_time(), &current_time);
  string cur_time_str = timeval_to_str(current_time, "%Y%m%d_%H%M%S", 6);
  my_snprintf(p1, sizeof(schema_path) - (p1 - schema_path), "/to_%s_%s_%ld_%s_%u.err",
              wrapper, host, port, cur_time_str.c_str(), thd ? thd->thread_id() : 0);
  std::string output_file = schema_path;


  bool restore_failed = false;
  if((thd == NULL) || thd->variables.tc_use_internal_restore_tool) 
  {
    Restore_Handler restore_handler(one_server, src_file, output_file);
    std::string errmsg;
    restore_failed = (restore_handler.import_data_main(errmsg) != 0);
    if (restore_failed) {
      sql_print_error("TDBCTL Restore Handler: %s", errmsg.c_str());
    }
  } 
  else 
  {
    string space = " ";
    string restore_cmd, restore_bin, restore_options;
    #if defined (_WIN32)
      restore_bin = "mysql";
    #else
      restore_bin = mysql_home_ptr;
      restore_bin += "/bin/mysql";
    #endif

    restore_options += space + "-u" + user + space + "-p" + password;
    size_t pwd_len = strlen(password);
    size_t pwd_pos = restore_options.size() - pwd_len;

    restore_options += space + "-P" + to_string(port) + space + "-h" + host + "<" + src_file;
    std::string restore_options_log = restore_options;
    restore_options_log.replace(pwd_pos, pwd_len, "xxxxx");

    restore_cmd = restore_bin + restore_options + space + ">&" + output_file;
    std::string restore_cmd_log = restore_bin + restore_options_log + space + ">&" + output_file;

    restore_failed = 
      (tc_system(restore_cmd.c_str(), restore_cmd_log.c_str()) != 0);
  }

  if (restore_failed)
  {
    restore_result.first = true;
    restore_result.second = tc_get_error_msg(ER_TCADMIN_RESTORE_NODE_ERROR, MYF(0), 
                                            src_file, host, port, output_file.c_str());
    /*
      Notice: we don't return here, because we need to restore the variable
    */
  }

  if ((strcasecmp(wrapper, SPIDER_WRAPPER) == 0) || (strcasecmp(wrapper, SPIDER_SLAVE_WRAPPER) == 0))
  {
    sql = "/*!50600 set global ddl_execute_by_ctl = @old_ddl_execute_by_ctl */";
    if(has_variable && tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      if(restore_result.first == false) {
        restore_result.first = true;
        restore_result.second = tc_get_error_msg(ER_TCADMIN_SEND_SQL_ERR, MYF(0), 
                                                exec_info.err_msg.c_str());
      }
      else {
        restore_result.second += "; ";
        restore_result.second += tc_get_error_msg(ER_TCADMIN_SEND_SQL_ERR, MYF(0), 
                                                exec_info.err_msg.c_str());
      }
    }
  }

  if(restore_result.first == false) {
    sql_print_information("Successfully restore the file to the node %s#%d: %s.", host, port, src_file);
  }

  return;
}

/**
 * @brief Loads schema from a backup file to multiple new database nodes in parallel
 * 
 * 
 * @param thd           Thread handler containing session context
 * @param lex           LEX structure with server connection options
 * @param schema_path   Path to the schema backup file to restore
 * 
 * @retval false        All restores completed successfully or were skipped (with warning)
 * @retval true         Any restore failed (error details aggregated in error message)
 */
bool tc_load_schema_to_multiple_new_nodes(THD *thd, LEX *lex, const std::string &schema_path)
{
  // Validate wrapper type (pre-filtered by sql_yacc.yy)
  DBUG_ASSERT((strcasecmp(lex->server_options.get_scheme(), SPIDER_WRAPPER) == 0) ||
               (strcasecmp(lex->server_options.get_scheme(), SPIDER_SLAVE_WRAPPER) == 0) ||
               (strcasecmp(lex->server_options.get_scheme(), TDBCTL_WRAPPER) == 0));

  // Skip restore if internal dump is disabled (only show warning)
  if (!tc_enable_internal_dump)
  {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_TCADMIN_CREATE_NODE_ERROR,
                        "restore was skipped: %s", "tc_enable_internal_dump is disabled");
    return false;
  }

  if(access(schema_path.c_str(), F_OK) != 0)
  {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), "schema file %s does not exist", schema_path.c_str());
    return true;
  }

  size_t server_count = lex->server_options_list.size();
  std::vector<std::thread> threads;
  std::vector<std::pair<bool, std::string>> restore_results(server_count);

  for (size_t i = 0; i < server_count; ++i) {
    const Server_options &one_server = lex->server_options_list[i];
    threads.emplace_back(tc_restore_to_node_worker, 
                        thd,
                        std::cref(one_server),
                        schema_path.c_str(),
                        std::ref(restore_results[i]));
  }

  bool error_occur = false;
  std::string error_msg = "when restore schema to new nodes, error occur:";
  for (size_t i = 0; i < threads.size(); ++i) {
    threads[i].join();
    if (restore_results[i].first) {
      error_occur = true;
      std::string server_name = lex->server_options_list[i].m_server_name.str;
      error_msg += "\n" + server_name + ": " + restore_results[i].second;
    }
  }

  if(error_occur) {
    error_msg += "\nNote: Prior to retry, remember to purge all partially imported data from the pending new nodes";
    my_error(ER_TCADMIN_CREATE_NODE_ERROR, MYF(0), error_msg.c_str());
    return true;
  }

  return false;
}