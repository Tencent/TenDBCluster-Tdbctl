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

static void protocol_store_field(Protocol *protocol, MYSQL_FIELD field, const char *row)
{
	DBUG_ENTER("protocol_store_field");
	if (row == NULL) {
		protocol->store_null();
		DBUG_VOID_RETURN;
	}

	switch (field.type) {
	case MYSQL_TYPE_LONGLONG:
	case MYSQL_TYPE_DOUBLE:
		protocol->store_longlong(atoll(row), true);
		break;
	case MYSQL_TYPE_VAR_STRING:
		protocol->store(row, get_charset(field.charsetnr, MYF(MY_WME)));
		break;
	case MYSQL_TYPE_LONG:
		protocol->store_long(atol(row));
		break;
	default:
		protocol->store(row, get_charset(field.charsetnr, MYF(MY_WME)));
		break;
	}

	DBUG_VOID_RETURN;
}

/*
  Tdbctl do show processlist.
	transfer SHOW PROCESSLIST to all nodes executed and display
	*/
void tc_show_processlist(THD *thd, bool verbose, const char *server_name)
{
	map<string, MYSQL_RES*> result_map;
	string show_sql = (verbose ? 
    "select ID,USER,HOST,DB,COMMAND,TIME,STATE,INFO from "
    " information_schema.processlist" :
    "select ID,USER,HOST,DB,COMMAND,TIME,STATE,substring(Info,1,100) "
    "from information_schema.processlist");

	Item *field;
	List<Item> field_list;
	size_t max_query_length = (verbose ? thd->variables.max_allowed_packet :
		PROCESS_LIST_WIDTH);
	Protocol *protocol = thd->get_protocol();
	DBUG_ENTER("tc_show_processlist");

	field_list.push_back(new Item_empty_string("Server_name", NAME_CHAR_LEN));
	field_list.push_back(new Item_int(NAME_STRING("Id"),
		0, MY_INT64_NUM_DECIMAL_DIGITS));
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
		DBUG_VOID_RETURN;

	if (server_name != NULL) {
		//get one server's processlist result.
		MYSQL_RES *res = tc_exec_sql_by_server(show_sql, server_name);
		result_map.insert(pair<string, MYSQL_RES *>(server_name, std::move(res)));
	}
	else {
		//get all node's processlist result
		result_map = tc_exec_sql_paral_by_wrapper(show_sql, NULL_WRAPPER, TRUE);
	}
	for_each(result_map.begin(), result_map.end(), [&protocol](std::pair<string, MYSQL_RES*> its) {
		string server_name = its.first;
		MYSQL_RES* res = its.second;
		//use to free result.
		MYSQL_RES_GUARD(res);
		if (res != NULL)
		{
			uint i;
			MYSQL_ROW row;
			MYSQL_FIELD  field;
			while ((row = mysql_fetch_row(res)) != NULL)
			{
			  protocol->start_row();
				protocol->store(server_name.c_str(), system_charset_info);
				res->current_field = 0;
				for (i = 0; i < mysql_num_fields(res); i++)
				{
					field = res->fields[res->current_field++];
					protocol_store_field(protocol, field, row[i]);
				}
				if (protocol->end_row())
					break; /* purecov: inspected */
			}
		}
	});

	my_eof(thd);
	result_map.clear();
	DBUG_VOID_RETURN;
}

/*
  Tdbctl do show processlist.
	transfer SHOW PROCESSLIST to all nodes executed and display
	*/
void tc_show_variables(THD *thd, enum_var_type type, String *wild, const char *server_name)
{
	map<string, MYSQL_RES*> result_map;
	string show_sql, option, like_cause;
	Item *field;
	List<Item> field_list;
	Protocol *protocol = thd->get_protocol();
  size_t max_var_len = strlen("Variable_name") + 1;
	size_t max_value_len = strlen("Value") + 1;

	DBUG_ENTER("tc_show_variables");

	if (wild != NULL)
		like_cause = string("like ") + "'" + wild->ptr() + "'";

	switch (type) {
	case OPT_DEFAULT:
		option = "";
		break;
	case OPT_SESSION:
		option = "SESSION ";
		break;
	case OPT_GLOBAL:
		option = "GLOBAL ";
		break;
	}

	show_sql = "SHOW " + option + "VARIABLES " + like_cause;

	if (server_name != NULL) {
		//get one server's processlist result.
		MYSQL_RES *res = tc_exec_sql_by_server(show_sql, server_name);
		result_map.insert(pair<string, MYSQL_RES *>(server_name, std::move(res)));
	}
	else {
		//get all node's processlist result
		result_map = tc_exec_sql_paral_by_wrapper(show_sql, NULL_WRAPPER, TRUE);
	}

	//get max length from result
	for_each(result_map.begin(), result_map.end(), [&max_var_len, &max_value_len](std::pair<string, MYSQL_RES*> its) {
		MYSQL_RES* res = its.second;
		if (res != NULL)
		{
			DBUG_ASSERT(mysql_num_fields(res) == 2);
			MYSQL_FIELD var_field = res->fields[0];
			MYSQL_FIELD value_field = res->fields[1];
			if (max_var_len < var_field.max_length)
				max_var_len = var_field.max_length;
			if (max_value_len < value_field.max_length)
				max_value_len = value_field.max_length;
		}
	});

	field_list.push_back(new Item_empty_string("Server_name", NAME_CHAR_LEN));
	field_list.push_back(new Item_empty_string("Variable_name", max_var_len));
	field_list.push_back(field = new Item_empty_string("Value", max_value_len));
	field->maybe_null = 1;
	if (thd->send_result_metadata(&field_list,
		Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
		DBUG_VOID_RETURN;

	for_each(result_map.begin(), result_map.end(), [&protocol](std::pair<string, MYSQL_RES*> its) {
		string server_name = its.first;
		MYSQL_RES* res = its.second;
		//use to free result.
		MYSQL_RES_GUARD(res);
		if (res != NULL)
		{
			uint i;
			MYSQL_ROW row;
			MYSQL_FIELD  field;
			while ((row = mysql_fetch_row(res)) != NULL)
			{
				protocol->start_row();
				protocol->store(server_name.c_str(), system_charset_info);
				res->current_field = 0;
				for (i = 0; i < mysql_num_fields(res); i++)
				{
					field = res->fields[res->current_field++];
					protocol_store_field(protocol, field, row[i]);
				}
				if (protocol->end_row())
					break; /* purecov: inspected */
			}
		}
	});

	my_eof(thd);
	result_map.clear();
	DBUG_VOID_RETURN;

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
  if (thd->cluster_conn_manager->refresh(FALSE))
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
  if (thd->cluster_conn_manager->refresh(FALSE))
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
