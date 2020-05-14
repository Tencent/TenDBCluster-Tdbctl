/*
    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

#ifndef TC_PARTITION_ADMIN_INCLUDED
#define TC_PARTITION_ADMIN_INCLUDED

#include <iostream>
#include <string>
#include <map>
#include <set>
#include <sstream>
#include <regex>
#include "mysql.h"
using namespace std;

/*
partition column type which is supported
*/
#define INT_COLUMN_TYPE "int"
#define DATETIME_COLUMN_TYPE "datetime"
#define DATE_COLUMN_TYPE "date"

enum COLUMN_TYPE { int_column_type, datetime_column_type, date_column_type };

COLUMN_TYPE get_column_type(string partition_column_type);

/*
remote_hash_algorithm
partition algorithm  which is supported
*/
#define LIST_PARTITION_ALGORITHM "list"
#define RANGE_PARTITION_ALGORITHM "range"

enum PARTITION_ALGORITHM_TYPE { list_partition_algorithm, range_partition_algorithm };

PARTITION_ALGORITHM_TYPE get_partition_algorithm_type(string remote_hash_algorithm);

//second of one day
const int TERM = 86400;

void create_partition_admin_thread();
void tc_partition_admin_thread();
int tc_partition_admin_worker(int step);

/*
admin partition for remote from  mysql.tc_partiton_admin_config
*/
int tc_remote_admin_partition(MYSQL *tdbctl_primary_conn,
	map<string, MYSQL*> remote_conn_map,
	map<string, string> remote_server_name_map,
	int step);

/*
init partition for remote from  mysql.tc_partiton_admin_config
*/
int tc_remote_new_partition(MYSQL* mysql, MYSQL* tdbctl_primary_conn,
	string remote_db, string tb_name, string  db_partition_columnname,
	string partition_column_type, int interval_time, string remote_hash_algorithm,
	string ipport, string server_name);

/*
add and delete partition for remote from  mysql.tc_partiton_admin_config
*/
int tc_remote_add_del_partition(MYSQL* mysql, MYSQL* tdbctl_primary_conn,
	string remote_db, string tb_name, string  db_partition_columnname,
	string partition_column_type, int interval_time, string remote_hash_algorithm,
	string ipport, string server_name, int expiration_time);

int check_table_is_partitioned(MYSQL* mysql, string remote_db, string tb_name,
	tc_exec_info *exec_info, int& count);
int get_min_max_partition(MYSQL* mysql, string remote_db, string tb_name,
	string& min_partition, string& max_partition, tc_exec_info* exec_info);
int tc_partiton_log(MYSQL* tdbctl_primary_conn, tc_exec_info* exec_info, string db, string tb_name,
	string server_name, string ipport, string error_code, string message);

/*
init sql for add partition 
*/
string tc_create_add_partition_sql(string remote_db, string tb_name, string db_partition_columnname,
	string partition_column_type, int interval_time, string remote_hash_algorithm, string max_partition);

/*
init sql for delete partition
*/
int tc_create_del_partition_sql(MYSQL* mysql, string remote_db, string tb_name, string db_partition_columnname,
	string partition_column_type, int interval_time, string remote_hash_algorithm, string& del_sql);

/*
init sql for new partition
*/
string tc_create_alter_sql(string remote_db, string tb_name, string partition_column_type,
	int interval_time, string remote_hash_algorithm, string  db_partition_columnname);

string get_time_string(string str);
#endif /* TC_PARTITION_ADMIN_INCLUDED */