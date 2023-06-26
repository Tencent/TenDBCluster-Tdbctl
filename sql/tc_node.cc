/*
     Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

/*
Add for node's control
*/
#include "tc_node.h"
#include "sql_base.h"         // open_tables, open_and_lock_tables,
#include "log.h"
#include "tc_base.h"
#include <thread>
#include <fstream>
#include <iostream>

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
	dump_options += space + "-u" + user + space + "-p" + password + space + "-P" + to_string(port) + space+ "-h" + host;
  std::string err_file;
  err_file = std::string(file) + ".err"; 
  
  // tdbctl enable gtid mode, therefore we need to get gtid info
  if (strcasecmp(wrapper, TDBCTL_WRAPPER) == 0)
  {
    dump_options += space + "--set-gtid-purged=auto";
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

  MYSQL *conn = tc_conn_connect(host, port, user, password);
  if (conn == NULL)
  {
    std::string ipport = std::string(host) + "#" + std::to_string(port);
    my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
    return 1;
  }
  MYSQL_GUARD(conn);
  string charset = tc_get_variable_value(conn, "character_set_server");
  dump_options += space + "--default-character-set=" + charset;

  dump_cmd = dump_bin + space + dump_options;
  if (system(dump_cmd.c_str()) != 0)
  {
    my_error(ER_TCADMIN_DUMP_NODE_ERROR, MYF(0), file, host, port, err_file.c_str());
    return 1;
  }

  sql_print_information("success dump schema to file %s.", file);

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
    const char *file)
{
  MYSQL_RES *res, *user_res, *grant_res;
  ofstream outfile;
  outfile.open(file);
  MYSQL *conn = tc_conn_connect(host, port, user, password);
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
      std::string user, host;
      MYSQL_ROW create_user_row = NULL, show_grant_row = NULL;
      host = row[1];
      user = row[0];
      sql = "show create user `" + user + "`@`" + host + "`";
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

      sql = "show grants for `" + user + "`@`" + host + "`";
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
  MYSQL *conn = tc_conn_connect(host, port, user, password);
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
  if (strcasecmp(wrapper, SPIDER_WRAPPER) == 0)
  {
    sql = "/*!50600 set @old_ddl_execute_by_ctl = @@ddl_execute_by_ctl*/";
    if(tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      my_error(ER_TCADMIN_SEND_SQL_ERR, MYF(0), exec_info.err_msg.c_str());
      return 1;
    }
    sql = "/*!set global ddl_execute_by_ctl=0*/";
    if(tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      my_error(ER_TCADMIN_SEND_SQL_ERR, MYF(0), exec_info.err_msg.c_str());
      return 1;
    }
  }
  else if (strcasecmp(wrapper, TDBCTL_WRAPPER) == 0)
  {
    sql = "set @old_tc_admin = @@tc_admin;set global tc_admin = 0";
    if(tc_exec_sql_without_result(conn, sql, &exec_info))
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
  restore_options += space + "-u" + user + space + "-p" + password + space + "-P" + to_string(port) + space + "-h" + host + "<" + file;
  restore_cmd = restore_bin + restore_options + space + ">&" + file + ".err";
  std::string err_file;
  err_file = std::string(file) + ".err"; 
  
  if (system(restore_cmd.c_str()) != 0)
  {
    my_error(ER_TCADMIN_RESTORE_NODE_ERROR, MYF(0), file, host, port, err_file.c_str());
    return 1;
  }

  if (strcasecmp(wrapper, SPIDER_WRAPPER) == 0)
  {
    sql = "/*!50600 set global ddl_execute_by_ctl = @old_ddl_execute_by_ctl */";
    if(tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      my_error(ER_TCADMIN_SEND_SQL_ERR, MYF(0), exec_info.err_msg.c_str());
      return 1;
    }
  }
  else if(strcasecmp(wrapper, TDBCTL_WRAPPER) == 0)
  {
    sql = "set global tc_admin=@old_tc_admin";
    if(tc_exec_sql_without_result(conn, sql, &exec_info))
    {
      my_error(ER_TCADMIN_SEND_SQL_ERR, MYF(0), exec_info.err_msg.c_str());
      return 1;
    }
  }

  sql_print_information("success restore %s to node %s#%d", file, host, port);
  return 0;
}

bool tc_load_schema_to_new_node(THD *thd, LEX *lex)
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

  // string server_name, add_address;
  list<FOREIGN_SERVER *> server_list;
  char schema_path[FN_REFLEN + 1];
  // char grant_path[FN_REFLEN + 1];
  char *p1 = my_stpnmov(schema_path, mysql_tmpdir, sizeof(schema_path));
  // char *p2 = my_stpnmov(grant_path, mysql_tmpdir, sizeof(grant_path));

  my_snprintf(p1, sizeof(schema_path) - (p1 - schema_path), "/%s_%lu%lx_%lx_schema.sql",
              tmp_file_prefix, current_thd->query_start(), current_pid,
              thd->thread_id());
  /*my_snprintf(p2, sizeof(grant_path) - (p2 - grant_path), "/%s_%lu%lx_%lx_grant.sql",
              tmp_file_prefix, current_thd->query_start(), current_pid,
              thd->thread_id());*/

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
  // the nodes of the specified type should not empty.
  DBUG_ASSERT(server_list.empty() != true);
  if (server_list.empty() ||
      (server_list.size() == 1 && (strcasecmp(lex->server_options.get_scheme(), SPIDER_WRAPPER) == 0||
                                   strcasecmp(lex->server_options.get_scheme(), TDBCTL_WRAPPER) == 0)))
  {
    // the first node of the specified type, no need to dump/restore schema/grant, only add to mysql.servers
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_TCADMIN_CREATE_NODE_ERROR,
                        "the created node is the first %s node, skip dump/restore schema/grant",
                        lex->server_options.get_scheme());
    return false;
  }

  /*
    dump node's schema from first spider/tdbctl node.
    we can't dump schema from the newly created node
    *Note*: dump tdbctl schema will dump schema from local node generally.
  */
  DBUG_ASSERT(!(strcasecmp(server_list.front()->host, lex->server_options.get_host()) == 0 &&
              server_list.front()->port == lex->server_options.get_port()));

  if (tc_dump_node_schema(
          server_list.front()->host,
          server_list.front()->port,
          server_list.front()->username,
          server_list.front()->password,
          schema_path,
          server_list.front()->scheme))
  {
    Sql_cmd_drop_server *drop_node = new Sql_cmd_drop_server(lex->server_options.m_server_name, true);
    drop_node->execute(thd);
    return true;
  }

  /*
  if (tc_dump_node_grant(
          server_list.front()->host,
          server_list.front()->port,
          server_list.front()->username,
          server_list.front()->password,
          grant_path))
  {
    Sql_cmd_drop_server *drop_node = new Sql_cmd_drop_server(lex->server_options.m_server_name, true);
    drop_node->execute(thd);
    goto error;
  }*/

  if (tc_restore_to_node(lex->server_options.get_host(),
                         lex->server_options.get_port(),
                         lex->server_options.get_username(),
                         lex->server_options.get_password(),
                         schema_path,
                         lex->server_options.get_scheme()))
  {
    Sql_cmd_drop_server *drop_node = new Sql_cmd_drop_server(lex->server_options.m_server_name, true);
    drop_node->execute(thd);
    return true;
  }

  /*
  if (tc_restore_to_node(lex->server_options.get_host(),
                         lex->server_options.get_port(),
                         lex->server_options.get_username(),
                         lex->server_options.get_password(),
                         grant_path))
  {
    Sql_cmd_drop_server *drop_node = new Sql_cmd_drop_server(lex->server_options.m_server_name, true);
    drop_node->execute(thd);
    goto error;
  }*/
  return false;
}