#include "sql_base.h"         // open_tables, open_and_lock_tables,
#include "sql_lex.h"
#include "sp_head.h"
#include "tc_base.h"
#include "sql_servers.h"
#include "mysql.h"
#include "sql_common.h"
#include "m_string.h"
#include "handler.h"
#include "log.h"
#include <string.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <list>
#include <vector>
#include <sstream>
#include <regex>
#include <thread>
#include <mutex>
#include "tc_monitor.h"


static PSI_memory_key key_memory_monitor;
map<string, MYSQL*> spider_conn_map;
map<string, string> spider_server_name_map;
map<string, string> spider_user_map;
map<string, string> spider_passwd_map;
set<string> spider_ipport_set;
map<string, string> tdbctl_ipport_map;
map<string, string> tdbctl_user_map;
map<string, string> tdbctl_passwd_map;
MYSQL *tdbctl_primary_conn = NULL;
MEM_ROOT mem_root;


void tc_free_connect()
{
	tc_conn_free(spider_conn_map);
	if (tdbctl_primary_conn)
	{
		mysql_close(tdbctl_primary_conn);
		tdbctl_primary_conn = NULL;
	}
	spider_conn_map.clear();
	spider_ipport_set.clear();
	spider_user_map.clear();
	spider_passwd_map.clear();
	spider_server_name_map.clear();
	tdbctl_ipport_map.clear();
	tdbctl_user_map.clear();
	tdbctl_passwd_map.clear();
	free_root(&mem_root, MYF(0));
}

int tc_init_connect(ulong& server_version)
{
	int ret = 0;
	int result = 0;	
	time_t to_tm_time = (time_t)time((time_t*)0);
	struct tm lt;
	struct tm* l_time = localtime_r(&to_tm_time, &lt);
	tc_free_connect();

	init_sql_alloc(key_memory_monitor, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
	spider_ipport_set = get_spider_ipport_set(
		&mem_root,
		spider_user_map,
		spider_passwd_map,
		FALSE);
	spider_server_name_map = get_server_name_map(&mem_root, SPIDER_WRAPPER, false);
	spider_conn_map = tc_spider_conn_connect(
		ret,
		spider_ipport_set,
		spider_user_map,
		spider_passwd_map);
	if (ret)
	{
		result = 1;
		goto finish;
	}
	tdbctl_ipport_map = get_tdbctl_ipport_map(
		&mem_root,
		tdbctl_user_map,
		tdbctl_passwd_map);
	tdbctl_primary_conn = tc_tdbctl_conn_primary(
		ret,
		tdbctl_ipport_map,
		tdbctl_user_map,
		tdbctl_passwd_map);
	if (ret) {
		result = 1;
		goto finish;
	}
	server_version = get_modify_server_version();
	return result;
finish:
	tc_free_connect();
	fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN MONITOR] "
		"failed to init_connect \n",
		l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday,
		l_time->tm_hour, l_time->tm_min, l_time->tm_sec);
	return result;
}


void create_check_cluster_availability_thread()
{
	std::thread t(tc_check_cluster_availability_thread);
	t.detach();
}


/*
init schema and data for cluster by random spider  
*/
int tc_check_cluster_availability_init()
{
	int result = 0;
	int ret = 0;
	stringstream ss;
	tc_exec_info exec_info;
	MEM_ROOT mem_root;
	map<string, string> spider_user_map;
	map<string, string> spider_passwd_map;
	set<string>  spider_ipport_set;
	MYSQL* spider_single_conn = NULL;
	init_sql_alloc(key_memory_monitor, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);

	//init for sql
	string sql = "set ddl_execute_by_ctl = on";
	string create_db_sql = "create database if not exists cluster_monitor";
	string drop_table_sql = "drop table if exists cluster_monitor.cluster_heartbeat";
	string create_table_sql = "create table if not exists cluster_monitor.cluster_heartbeat( "
		"uid int(11) NOT NULL, "
		"time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
		"k int(11) DEFAULT 0, "
		"PRIMARY KEY(uid) "
		") ENGINE = InnoDB ";
	string replace_sql = "replace into   cluster_monitor.cluster_heartbeat(uid) values";
	string init_sql = "";
	string replace_sql_cur = "";
	ulong num = get_servers_count_by_wrapper(MYSQL_WRAPPER, FALSE);
	ulong num_tmp = num;
	vector<string>vec(num,"");
	if (num <= 0)
	{
		goto finish;
	}
	for (ulong i = 0;num_tmp>0;++i) 
	{
		ss.str("");
		ss << i;
		string str = ss.str();
		int hash_value = (int)(((longlong)crc32(0L, (uchar*)(&str)->c_str(), (&str)->length()))%num);
		if (vec[hash_value] == "")
		{
			--num_tmp;
			ss.str("");
			ss << i;
			vec[hash_value] = ss.str();
			replace_sql_cur = "(";
			replace_sql_cur += ss.str();
			replace_sql_cur += "),";
			replace_sql += replace_sql_cur;
		}
	}
	replace_sql.erase(replace_sql.end() - 1);
	init_sql = sql + ";" + create_db_sql + ";" + drop_table_sql + ";" + create_table_sql + ";" + replace_sql;
	
	spider_ipport_set = get_spider_ipport_set(
		&mem_root,
		spider_user_map,
		spider_passwd_map,
		FALSE);

	spider_single_conn = tc_spider_conn_single(
		ret,
		spider_ipport_set,
		spider_user_map,
		spider_passwd_map);
	if (ret)
	{
		result = 1;
		goto finish;
	}
	if (tc_exec_sql_without_result(spider_single_conn, init_sql, &exec_info))
	{
		result = 2;
		goto finish;
	}
finish:
	mysql_close(spider_single_conn);
	spider_ipport_set.clear();
	spider_user_map.clear();
	spider_passwd_map.clear();
	free_root(&mem_root, MYF(0));
	return result;
}


/*
check_cluster_availability
1.update cluster cluster_monitor.cluster_heartbeat table
2.log result in tdbctl: cluster_admin.cluster_heartbeat_log
*/
int tc_check_cluster_availability()
{
	int result = 0;
	time_t to_tm_time = (time_t)time((time_t*)0);
	struct tm lt;
	struct tm* l_time = localtime_r(&to_tm_time, &lt);
	tc_exec_info exec_info;
	stringstream ss;
	static int64 current_id = 0;
	static bool current_id_init = FALSE;
	map<string, MYSQL*>::iterator its;

	//init for sql
	string check_heartbeat_sql = "update cluster_monitor.cluster_heartbeat set k=(k+1)%1024";
	string heartbeat_log_sql_pre = "replace into cluster_admin.cluster_heartbeat_log( "
		"id, server_name, host, code, message) values(";
	string str_id = "";
	string quotation = "\"";
	string server_name = "";
	string host = "";
	string error_code = "0";
	string message = "";
	string heartbeat_log_sql = "";

	if (!current_id_init)
	{
		MYSQL_RES* res;
		string sql_max_id = "select max(id) from cluster_admin.cluster_heartbeat_log";
		MYSQL_ROW row = NULL;
		res = tc_exec_sql_with_result(tdbctl_primary_conn, sql_max_id);
		if (res && (row = mysql_fetch_row(res)))
		{
			current_id = row[0] ? atoi(row[0]) : 0;
			current_id_init = true;
		}
		else
		{
			result = 2;
			fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN MONITOR] "
				"select cluster_heartbeat_log failed\n",
				l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, l_time->tm_hour,
				l_time->tm_min, l_time->tm_sec);
			goto finish;
		}
	}
	for (its = spider_conn_map.begin(); its != spider_conn_map.end(); its++)
	{	
		heartbeat_log_sql = heartbeat_log_sql_pre;
		host = its->first;
		server_name = spider_server_name_map[its->first];
		MYSQL* spider_conn = its->second;
		tc_exec_info exec_info_spider;
		tc_exec_info exec_info_tdbctl;
		current_id = (current_id + 1) % max_heartbeat_log;
		ss.str("");
		ss << current_id;
		str_id = ss.str();

		if (tc_exec_sql_without_result(spider_conn, check_heartbeat_sql, &exec_info_spider))
		{
			result = 2;
			ss.str("");
			ss << exec_info_spider.err_code;
			error_code = ss.str();
			message = exec_info_spider.err_msg;
		}

		//append sql
		heartbeat_log_sql += str_id;
		heartbeat_log_sql += ",";
		heartbeat_log_sql += quotation;
		heartbeat_log_sql += server_name;
		heartbeat_log_sql += quotation;
		heartbeat_log_sql += ",";
		heartbeat_log_sql += quotation;
		heartbeat_log_sql += host;
		heartbeat_log_sql += quotation;
		heartbeat_log_sql += ",";
		heartbeat_log_sql += error_code;
		heartbeat_log_sql += ",";
		heartbeat_log_sql += quotation;
		heartbeat_log_sql += message;
		heartbeat_log_sql += quotation;
		heartbeat_log_sql += ")";
		if(tc_exec_sql_without_result(tdbctl_primary_conn, heartbeat_log_sql, &exec_info_tdbctl))
		{
			result = 2;
			fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN MONITOR] "
				"log in cluster_heartbeat_log failed :%02d %s\n",
				l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday,
				l_time->tm_hour, l_time->tm_min, l_time->tm_sec,
				exec_info_tdbctl.err_code, (char*)(exec_info_tdbctl.err_msg.data()));
		}
	}

finish:
	return result;
}
 
bool check_server_version(ulong& server_version) 
{
	bool res = false;
	ulong server_version_new = get_modify_server_version();
	if (server_version != server_version_new)
	{
		server_version = server_version_new;
		res = true;
	}
	return res;
}

/*
check cluster availability
tc_check_cluster_availability do check work and log in cluster_admin.cluster_heartbeat_log
*/
void tc_check_cluster_availability_thread()
{
	/*
	flag of whether need to re-init connect
	0 means not re-init
	1 means re-init

	after re-init ok , set flag=0
	if the tdbctl is not primary or set global tc_check_availability=0 ,set flag=1 and  free connect
	*/
	int flag = 1;

	/*
	result of  tc_init_connect  and tc_check_cluster_availability
	0 means ok
	1 means error
	if error,need to re-init connection
	*/
	int res = 0;

	/*
	control the time of  re-init
	*/
	ulong j = 0;
	ulong server_version = -1;
	/*
	init Tdbctl_is_master in background thread
	*/
	Tdbctl_is_master = tc_is_master_tdbctl_node();
	while (1)
	{
		/*
		if tc_check_availability=1 and is primary tdbctl
		TODO:get tc_tdbctl_conn_primary by host and port
		*/
		if (tc_check_availability)
		{
			/*
			if first time do check
			or do  tc_init_connect  and tc_check_cluster_availability error
			or the connect time 
			*/
			while (flag || res || j*tc_check_availability_interval > tc_check_availability_connect
				|| check_server_version(server_version))
			{
				//init memory and connect
				if(!(res= tc_init_connect(server_version)))
				{
					flag = 0;
					j = 0;
				}
				else
				{
					//fail to init memory and connect
					sleep(20);
				}
			}
			for (ulong i = 0; i < labs(tc_check_availability_interval - 2); ++i) 
			{
				sleep(1);
			}
			//check available for cluster			
			if (!(res = tc_check_cluster_availability()))
			{
				++j;
			}
		}
		else if (!flag)
		{
			//free memory and connect
			tc_free_connect();
			flag = 1;
		}
		sleep(2);
	}
}
