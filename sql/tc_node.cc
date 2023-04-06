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
        const char *file)
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
                "--quick --no-data --all-databases --add-not-exists";
  dump_options += space + "-r" + file + space + "--log-error=" + file;
	dump_options += space + "-u" + user + space + "-p" + password + space + "-P" + to_string(port) + space+ "-h" + host;
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
    dump_options += space + "--ignore-database=" + token;
  }

  MYSQL *conn = tc_conn_connect(host, port, user, password);
  if (conn == NULL)
  {
    my_error(ER_TCADMIN_DUMP_NODE_ERROR, MYF(0), file, host, port);
    return 1;
  }
  MYSQL_GUARD(conn);
  string charset = tc_get_variable_value(conn, "character_set_server");
  dump_options += space + "--default-character-set=" + charset;

  dump_cmd = dump_bin + space + dump_options;
  if (system(dump_cmd.c_str()) != 0)
  {
    sql_print_warning(ER(ER_TCADMIN_DUMP_NODE_ERROR), file, host, port);
    sql_print_warning("detail information in file %s.", file);
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
    my_error(ER_TCADMIN_DUMP_NODE_ERROR, MYF(0), file, host, port);
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
        const char *file)
{
  MYSQL_RES *res1, *res2;

  MYSQL *conn = tc_conn_connect(host, port, user, password);
  MYSQL_GUARD(conn);
  if (!conn) 
  {
    sql_print_error("can't connect to the node that to be restored");
    my_error(ER_TCADMIN_RESTORE_NODE_ERROR, MYF(0), file, host, port);
    return 1;
  }
  std::string sql1 = "set @old_ddl_execute_by_ctl = @@ddl_execute_by_ctl;set global ddl_execute_by_ctl = off";
  res1 = tc_exec_sql_with_result(conn, sql1);
  MYSQL_RES_GUARD(res1);

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

  if (system(restore_cmd.c_str()) != 0)
  {
    sql_print_warning(ER(ER_TCADMIN_RESTORE_NODE_ERROR), file, host, port);
    sql_print_warning("detail information in file %s.err", file);
    return 1;
  }

  std::string sql2 = "set global ddl_execute_by_ctl = @old_ddl_execute_by_ctl";
  res2 = tc_exec_sql_with_result(conn, sql2);
  MYSQL_RES_GUARD(res2);

  sql_print_information("success restore %s to node %s#%d", file, host, port);
  return 0;
}