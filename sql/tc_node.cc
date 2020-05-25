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

/*
 use mysqldump to backup node's schema
 return value
    0 ok
    1 error
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
  string ipport = string(host) + "#" + to_string(port);

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

  MYSQL *conn = tc_conn_connect(ipport, user, password);
  if (conn == NULL)
  {
    my_error(ER_TCADMIN_DUMP_NODE_ERROR, MYF(0), user, port);
    return 1;
  }
  MYSQL_GUARD(conn);
  string charset = tc_get_variable_value(conn, "character_set_server");
  dump_options += space + "--default-character-set=" + charset;

  dump_cmd = dump_bin + space + dump_options;
  if (system(dump_cmd.c_str()) != 0)
  {
    sql_print_warning(ER(ER_TCADMIN_DUMP_NODE_ERROR), host, port);
    sql_print_warning("detail information in file %s.", file);
    return 1;
  }

  sql_print_information("success dump schema to file %s.", file);

  return 0;
}

/*
 use mysql client to restore node's schema
 return value
   0 ok
   1 error
*/
int tc_restore_node_schema(
        const char *host,
        uint port,
        const char *user,
        const char *password,
        const char *file)
{
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
    sql_print_warning(ER(ER_TCADMIN_RESTORE_NODE_ERROR), host, port);
    sql_print_warning("detail information in file %s.err", file);
    return 1;
  }

  sql_print_information("success restore schema to node %s#%", host, port);
  return 0;
}
