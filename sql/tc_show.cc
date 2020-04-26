/*
Add for node's show 
*/

#include "tc_show.h"
#include "tc_base.h"
#include "mysql.h"
#include "protocol.h"                       // Protocol

using namespace std;

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
	string show_sql = (verbose ? "SHOW FULL PROCESSLIST" : "SHOW PROCESSLIST");

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
	field_list.push_back(field = new Item_return_int("Rows_sent",
		MY_INT64_NUM_DECIMAL_DIGITS,
		MYSQL_TYPE_LONGLONG));
	field_list.push_back(field = new Item_return_int("Rows_examined",
		MY_INT64_NUM_DECIMAL_DIGITS,
		MYSQL_TYPE_LONGLONG));
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

