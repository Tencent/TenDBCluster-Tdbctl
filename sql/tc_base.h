#ifndef TC_BASE_INCLUDED
#define TC_BASE_INCLUDED

#include <iostream>
#include <string>
#include <map>
#include <set>
#include <sstream>
#include <regex>
#include "mysql.h"
using namespace std;

//wrapper name map to mysql.servers's Wrapper field
#define MYSQL_WRAPPER "mysql"
#define SPIDER_WRAPPER "SPIDER"
#define TDBCTL_WRAPPER "TDBCTL"

void gettype_create_filed(Create_field *cr_field, String &res);
bool get_createfield_default_value(Create_field *cr_field, String *def_value);
void filed_add_zerofill_and_unsigned(String &res, bool unsigned_flag, bool zerofill);
int parse_get_shard_key_for_spider(
    const char*		table_comment,
    char*		key_buf,
    uint		key_len
);
int parse_get_config_table_for_spider(
    const char*		table_comment,
    char*		key_buf,
    uint		key_len
);
enum_sql_command tc_get_sql_type(THD *thd, LEX *lex);
int tc_get_shard_key(THD *thd, LEX *lex, char *buf, uint len);
const char* tc_get_cur_tbname(THD *thd, LEX *lex);
const char* tc_get_cur_dbname(THD *thd, LEX *lex);
const char* tc_get_new_tbname(THD *thd, LEX *lex);
const char* tc_get_new_dbname(THD *thd, LEX *lex);
bool tc_is_with_shard(THD *thd, LEX *lex);

const char* get_stmt_type_str(int type);


typedef struct tc_exec_info
{
    uint err_code;
    string err_msg;
    ulonglong row_affect;
} TC_EXEC_INFO;

typedef struct tc_execute_result
{
    bool result; // TURE, error happened; FALASE, SUCCEED
    map<string, tc_exec_info> spider_result_info;
    map<string, tc_exec_info> remote_result_info;
} TC_EXEC_RESULT;

typedef struct tc_parse_result
{
    enum_sql_command sql_type;
    LEX_CSTRING query_string;
    string db_name;
    string table_name;
    string new_table_name;
    string new_db_name;
    string shard_key;
    string result_info;
    bool is_with_shard;
    bool is_with_autu;
    bool is_with_unique;
    bool result;
} TC_PARSE_RESULT;

void tc_parse_result_init(TC_PARSE_RESULT *parse_result_t);
bool is_add_or_drop_unique_key(THD *thd, LEX *lex);

/*
sub convert_spider_use_db() {
sub convert_spider_set_db() {
sub convert_spider_privilege_sql {
sub convert_spider_create_event {
sub convert_spider_dml {
**/
bool tc_parse_getkey_for_spider(
  THD *thd, 
  char *key_name, 
  char *result, 
  int buf_len, 
  bool *is_unique_key
);

string tc_get_only_spider_ddl_withdb(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_only_spider_ddl(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_create_table(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

map<string, string> tc_get_remote_create_table(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_create_database(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

map<string, string> tc_get_remote_create_database(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_rename_table(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

map<string, string> tc_get_remote_rename_table(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_create_table_like(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

map<string, string> tc_get_remote_create_table_like(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_drop_table(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

map<string, string> tc_get_remote_drop_table(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_drop_database(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

map<string, string> tc_get_remote_drop_database(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_change_database(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

map<string, string> tc_get_remote_change_database(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_alter_table(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

map<string, string> tc_get_remote_alter_table(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_create_or_drop_index(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

map<string, string> tc_get_remote_create_or_drop_index(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_create_procedure(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_spider_drop_procedure(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

bool tc_query_parse(
  THD *thd, 
  LEX *lex, 
  TC_PARSE_RESULT *tc_parse_result_t
);

bool tc_query_convert(
  THD *thd, 
  LEX *lex, 
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count, 
  string *spider_create_sql, 
  map<string, string> *remote_create_sql
);

bool tc_spider_ddl_run_async(
  string query, 
  map<string, MYSQL> spider_conn_map, 
  tc_execute_result *exec_result
);

bool tc_remotedb_ddl_run_async(
  map<string, string> remote_sql, 
  map<string, MYSQL> remote_conn_map, 
  map<string, string> remote_ipport_map, 
  tc_execute_result *exec_result
);

bool tc_ddl_run(
  THD *thd, 
  string before_sql_for_spider, 
  string before_sql_for_remote, 
  string spider_sql, 
  map<string, string> remote_sql, 
  tc_execute_result *exec_result
);

bool tc_append_before_query(
  THD *thd, 
  LEX *lex, 
  string &sql_spider, 
  string &sql_remote
);

MYSQL* tc_conn_connect(
  string ipport, 
  string user, 
  string passwd
);

map<string, MYSQL*> tc_remote_conn_connect(
  int &ret, 
  map<string, string> remote_ipport_map, 
  map<string, string> remote_user_map, 
  map<string, string> remote_passwd_map);

map<string, MYSQL*> tc_spider_conn_connect(
  int &ret,
  set<string> spider_ipport_set, 
  map<string, string> spider_user_map,
  map<string, string> spider_passwd_map
);

MYSQL* tc_spider_conn_single(
	int &ret,
	set<string> spider_ipport_set,
	map<string, string> spider_user_map,
	map<string, string> spider_passwd_map
);

MYSQL *tc_tdbctl_conn_primary(
	int &ret,
	map<string, string> tdbctl_ipport_map,
	map<string, string> tdbctl_user_map,
	map<string, string> tdbctl_passwd_map
);


set<string> get_spider_ipport_set(
  MEM_ROOT *mem, 
  map<string, string> &spider_user_map, 
  map<string, string> &spider_passwd_map, 
  bool with_slave
);

map<string, string> get_remote_ipport_map(
  MEM_ROOT *mem, 
  map<string, string> &remote_user_map, 
  map<string, string> &remote_passwd_map
);

map<string, string> get_server_name_map(
	MEM_ROOT *mem,
	const char *wrapper,
	bool with_slave
);

map<string, string> get_tdbctl_ipport_map(
	MEM_ROOT *mem,
	map<string, string> &tdbctl_user_map,
	map<string, string> &tdbctl_passwd_map
);

map<string, string>get_remote_server_name_map(
	map<string, string> remote_ipport_map);

bool tc_conn_free( map<string, MYSQL*> &conn_map);
int tc_mysql_next_result(MYSQL* mysql);

bool tc_exec_sql_paral(string exec_sql, map<string, MYSQL*>& conn_map,
  map<string, tc_exec_info>& result_map,
  map<string, string> user_map,
  map<string, string> passwd_map,
  bool error_retry);

bool tc_reconnect(string ipport,
  map<string, MYSQL*>& spider_conn_map,
  map<string, string> spider_user_map,
  map<string, string> spider_passwd_map);

bool tc_exec_sql_up(MYSQL* mysql, string sql, tc_exec_info* exec_info);
MYSQL_RES* tc_exec_sql_with_result(MYSQL* mysql, string sql);
bool tc_exec_sql_without_result(MYSQL* mysql, string sql, tc_exec_info* exec_info);
MYSQL_RES* tc_exec_sql_up_with_result(MYSQL* mysql, string sql, MYSQL_RES** res);
MYSQL_RES* tc_exec_sql_with_result(MYSQL* mysql, string sql);
bool tc_exec_sql_paral_with_result(
  string exec_sql,
  map<string, MYSQL*>& conn_map,
  map<string, MYSQL_RES*>& result_map,
  map<string, string>& user_map,
  map<string, string>& passwd_map,
  bool error_retry);

my_time_t string_to_timestamp(const string s);

unsigned int tc_is_running_node(char *host, ulong *port);

#endif /* TC_BASE_INCLUDED */