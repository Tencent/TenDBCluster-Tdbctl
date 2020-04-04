/*
Add for node's control
*/
#include "tc_node.h"
#include "sql_base.h"         // open_tables, open_and_lock_tables,
#include "log.h"
#include "tc_base.h"

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

#if defined (_WIN32)
	dump_bin = "mysqldump";
#else
	dump_bin = mysql_home_ptr;
	dump_bin += "/bin/mysqldump";
#endif
	dump_options = "--single-transaction --no-autocommit=FALSE  --skip-opt --create-options  --routines  --quick --no-data --all-databases";
	dump_options += space + "-r" + file + space + "--log-error=" + file;
	dump_options += space + "-u" + user + space + "-p" + password + space + "-P" + to_string(port);

	string ipport = string(host) + "#" + to_string(port);
	MYSQL *conn = tc_conn_connect(ipport, user, password);
	if (conn == NULL)
	{
		my_error(ER_TCADMIN_DUMP_NODE_ERROR, MYF(0), user, port);
		return 1;
	}
	string charset = tc_get_variable_value(conn, "character_set_server");
	mysql_close(conn);
	dump_options += space + "--default-character-set=" + charset;

	dump_cmd = dump_bin + space + dump_options;
	if (system(dump_cmd.c_str()) != 0)
	{
		sql_print_warning(ER(ER_TCADMIN_DUMP_NODE_ERROR), host, port);
		sql_print_warning("detail information in file %s.", file);
		return 1;
	}

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
	restore_options += space + "-u" + user + space + "-p" + password + space + "-P" + to_string(port) + "<" + file;
	restore_cmd = restore_bin + restore_options + space + ">&" + file + ".err";

	if (system(restore_cmd.c_str()) != 0)
	{
		sql_print_warning(ER(ER_TCADMIN_RESTORE_NODE_ERROR), host, port);
		sql_print_warning("detail information in file %s.err", file);
		return 1;
	}

	return 0;
}
