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
#include "tc_partition_admin.h"

static PSI_memory_key key_memory_partition;
void create_partition_admin_thread()
{
	std::thread t(tc_partition_admin_thread);
	t.detach();
}


/**
ADMIN partition for remote

@input
    step   0 for init ,1 for init ,add and delete
*/
int tc_partition_admin_worker(int step)
{
	int ret = 0;
	int result = 0;
	map<string, string> tdbctl_ipport_map;
	map<string, string> tdbctl_user_map;
	map<string, string> tdbctl_passwd_map;

	map<string, MYSQL*> remote_conn_map;
	map<string, string> remote_user_map;
	map<string, string> remote_passwd_map;
	map<string, string> remote_ipport_map;
	map<string, string> remote_server_name_map;

	MYSQL *tdbctl_primary_conn = NULL;
	MEM_ROOT mem_root;

	init_sql_alloc(key_memory_partition, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
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

	remote_ipport_map = get_remote_ipport_map(&mem_root,
		remote_user_map, remote_passwd_map);
	remote_conn_map = tc_remote_conn_connect(ret, remote_ipport_map,
		remote_user_map, remote_passwd_map);
	remote_server_name_map = get_server_name_map(&mem_root, MYSQL_WRAPPER, false);
	tc_remote_admin_partition(tdbctl_primary_conn, remote_conn_map, remote_server_name_map, 0);
	if (step)
	{
		tc_remote_admin_partition(tdbctl_primary_conn, remote_conn_map, remote_server_name_map, 1);
	}
finish:
	tc_conn_free(remote_conn_map);
	if (tdbctl_primary_conn)
	{
		mysql_close(tdbctl_primary_conn);
		tdbctl_primary_conn = NULL;
	}
	tdbctl_ipport_map.clear();
	tdbctl_user_map.clear();
	tdbctl_passwd_map.clear();
	remote_conn_map.clear();
	remote_user_map.clear();
	remote_passwd_map.clear();
	remote_ipport_map.clear();
	remote_server_name_map.clear();
	free_root(&mem_root, MYF(0));
	return result;
}





/*
 ADMIN partition for remote from  cluster_admin.tc_partiton_admin_config

 input:
      tdbctl_primary_conn    : conn of the primary TDBCTL
	  remote_conn_map        : ip#port-->MYSQL*
	  remote_server_name_map : ip#port-->server_name
      step                   : 0 for init ,1 for add and delete
  output:
      result                 : 0 for ok 
*/
int tc_remote_admin_partition(MYSQL *tdbctl_primary_conn,
	map<string, MYSQL*> remote_conn_map,
	map<string, string> remote_server_name_map,
    int step)
{
	int result = 0;
	time_t to_tm_time = (time_t)time((time_t*)0);
	struct tm lt;
	struct tm* l_time = localtime_r(&to_tm_time, &lt);
	map<string, MYSQL*>::iterator its;
	regex pattern(tdbctl_mysql_wrapper_prefix);	

	/*
	init for update SQL in TDBCTL
	*/
	string update_sql_cur = "update cluster_admin.tc_partiton_admin_config set "
		" is_partitioned = 1 where db_name =";
	string update_config_sql = "";
	string quotation = "\"";

	/*
	init for get target db and table
	*/
	string get_partition_sql = "";
	if (step == 0)
	{
		/*
		init for get  unpartitioned SQL in remote for
		init partition 
		*/
		get_partition_sql = "select db_name, "
			" tb_name,partition_column,expiration_time,partition_column_type,interval_time, "
			" remote_hash_algorithm from cluster_admin.tc_partiton_admin_config where is_partitioned<>1 ";
	}
	else if (step == 1)
	{
		/*
		init for get  unpartitioned SQL in remote for
		add delete partition
		*/
		get_partition_sql = "select db_name, "
			" tb_name,partition_column,expiration_time,partition_column_type,interval_time, "
			" remote_hash_algorithm from cluster_admin.tc_partiton_admin_config where is_partitioned=1 ";
	}

	MYSQL_RES* res = tc_exec_sql_with_result(tdbctl_primary_conn, get_partition_sql);
	if (res)
	{
		MYSQL_ROW row = NULL;
		/*
		new partition for every unpartitioned table
		*/
		while ((row = mysql_fetch_row(res)))
		{
			string db_name = row[0];
			string tb_name = row[1];
			string db_partition_columnname = row[2];
			int expiration_time = atoi(row[3]);
			string partition_column_type = row[4];
			int interval_time = atoi(row[5]);
			string remote_hash_algorithm = row[6];
			tc_exec_info exec_info;
			to_tm_time = (time_t)time((time_t*)0);
			l_time = localtime_r(&to_tm_time, &lt);
			update_config_sql = "";

			/*
			new partition for every  remote
			*/
			for (its = remote_conn_map.begin(); its != remote_conn_map.end(); its++) 
			{
				string ipport = its->first;
				MYSQL* mysql = its->second;
				string server_name = remote_server_name_map[ipport];				
				string hash_value = regex_replace(server_name, pattern, "");
				string remote_db = db_name + "_" + hash_value;
				int ret = 0;
				if (step == 0)
				{
					ret = tc_remote_new_partition(mysql, tdbctl_primary_conn,
						remote_db, tb_name, db_partition_columnname, partition_column_type,
					    interval_time, remote_hash_algorithm, ipport, server_name);
				}
				else if (step == 1)
				{
					ret = tc_remote_add_del_partition(mysql, tdbctl_primary_conn,
						remote_db, tb_name, db_partition_columnname, partition_column_type,
						interval_time, remote_hash_algorithm, ipport, server_name, expiration_time);
				}
				else
				{
					ret = 1;
				}
				result = ret ? ret : result;			
			}

			if (result ==0 && step==0)
			{
				/*
				init for update config SQL in TDBCTL
				*/
				update_config_sql = update_sql_cur + quotation + db_name + quotation + " and tb_name= "
					+ quotation + tb_name + quotation;
				if (tc_exec_sql_without_result(tdbctl_primary_conn, update_config_sql, &exec_info))
				{
					fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN PARTITION_ADMIN] "
						"fail to  update  in  cluster_admin.tc_partiton_admin_config : %02d %s \n",
						l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, l_time->tm_hour,
						l_time->tm_min, l_time->tm_sec, exec_info.err_code, exec_info.err_msg.c_str());
					exec_info.err_code = 0;
					exec_info.row_affect = 0;
					exec_info.err_msg = "";
				}			
			}
		}
	}
	else
	{
		result = 2;
		fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN PARTITION_ADMIN] "
			"fail to select cluster_admin.tc_partiton_admin_config \n",
			l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, l_time->tm_hour,
			l_time->tm_min, l_time->tm_sec);
	}
	return result;
}

COLUMN_TYPE get_column_type(string partition_column_type)
{
	COLUMN_TYPE column_type = int_column_type;
	if (!strcasecmp((char *)(partition_column_type.data()), DATETIME_COLUMN_TYPE))
	{
		column_type = datetime_column_type;
	}
	else if (!strcasecmp((char *)(partition_column_type.data()), DATE_COLUMN_TYPE))
	{
		column_type = date_column_type;

	}
	else if (!strcasecmp((char*)(partition_column_type.data()), INT_COLUMN_TYPE))
	{
		column_type = int_column_type;
	}
	return column_type;
}

PARTITION_ALGORITHM_TYPE get_partition_algorithm_type(string remote_hash_algorithm)
{
	PARTITION_ALGORITHM_TYPE partition_type= range_partition_algorithm;
	if (!strcasecmp((char*)(remote_hash_algorithm.data()), RANGE_PARTITION_ALGORITHM))
	{
		partition_type = range_partition_algorithm;
	}
	else if (!strcasecmp((char*)(remote_hash_algorithm.data()), LIST_PARTITION_ALGORITHM))
	{
		partition_type = list_partition_algorithm;
	}
	return partition_type;
}

int get_min_partition(MYSQL* mysql, string remote_db, string tb_name, string& min_partition) 
{
	int result = 0;
	string quotation = "\"";
	//init for select min partition sql
	string get_min_partition_sql = "select min(PARTITION_DESCRIPTION) "
		" from information_schema.PARTITIONS where TABLE_SCHEMA=";
	get_min_partition_sql += quotation + remote_db + quotation;
	get_min_partition_sql += " and TABLE_NAME= " + quotation + tb_name + quotation;
	get_min_partition_sql += " and PARTITION_NAME is NOT NULL and PARTITION_EXPRESSION is "
		" NOT NULL order by PARTITION_DESCRIPTION";

	MYSQL_RES* res = tc_exec_sql_with_result(mysql, get_min_partition_sql);
	MYSQL_ROW row = NULL;
	if (res && (row = mysql_fetch_row(res)))
	{
		min_partition = row[0];
	}
	else
	{
		result = 2;
	}
	return result;
}


int get_min_max_partition(MYSQL* mysql, string remote_db, string tb_name, string& min_partition,
	string& max_partition) 
{
	int result = 0;
	string quotation = "\"";
	//init for select min max partition sql
	string get_min_max_partition_sql = "select min(PARTITION_DESCRIPTION),max(PARTITION_DESCRIPTION) "
		" from information_schema.PARTITIONS where TABLE_SCHEMA=";
	get_min_max_partition_sql += quotation + remote_db + quotation;
	get_min_max_partition_sql += " and TABLE_NAME= " + quotation + tb_name + quotation;
	get_min_max_partition_sql += " and PARTITION_NAME is NOT NULL and PARTITION_EXPRESSION is "
		" NOT NULL order by PARTITION_DESCRIPTION";

	MYSQL_RES* res = tc_exec_sql_with_result(mysql, get_min_max_partition_sql);
	MYSQL_ROW row = NULL;
	if (res && (row = mysql_fetch_row(res)))
	{
		min_partition = row[0];
		max_partition = row[1];
	}
	else
	{
		result = 2;
	}
	return result;
}



/*
get time diff(unit:day),between str and now
*/
int get_time_diff(string str)
{
	time_t to_tm_time = (time_t)time((time_t*)0);
	time_t timer_new;
	struct tm tm_new;
	string res = get_time_string(str);
	string year = res.substr(0, 4);
	string month = res.substr(4, 2);
	month = month[0] == '0' ? month.substr(1, 1) : month;
	string day = res.substr(6, 2);
	day = day[0] == '0' ? day.substr(1, 1) : day;


	//change data structure to tm
	tm_new.tm_year = atoi(year.c_str()) - 1900;
	tm_new.tm_mon = atoi(month.c_str()) - 1;
	tm_new.tm_mday = atoi(day.c_str());
	tm_new.tm_hour = 0;
	tm_new.tm_min = 0;
	tm_new.tm_sec = 0;
	timer_new = mktime(&tm_new);

	//get time_diff
	double diff_t = difftime(to_tm_time, timer_new);
	int t1 = static_cast<int>(diff_t / TERM);
	t1 = abs(t1);
	return t1;
}

/*
add and delete partition for remote from  cluster_admin.tc_partiton_admin_config
*/
int tc_remote_add_del_partition(MYSQL* mysql, MYSQL* tdbctl_primary_conn,
	string remote_db, string tb_name, string  db_partition_columnname,
	string partition_column_type, int interval_time, string remote_hash_algorithm,
	string ipport, string server_name, int expiration_time)
{
	int result = 0;
	int count = 0;
	tc_exec_info exec_info;
	stringstream ss;

	//init for ADMIN  partition
	string min_partition = "";
	string max_partition = "";
	string add_partition_sql = "";
	string del_partition_sql = "";
	//init for log SQL
	string quotation = "\"";
	string error_code = "0";
	string message = "";
	int retry = 3;
	int inter_min = 0;
	int inter_max = 0;

	if (check_table_is_partitioned(mysql, remote_db, tb_name, &exec_info, count))
	{
		ss.str("");
		ss << exec_info.err_code;
		error_code = ss.str();
		message = exec_info.err_msg;
		exec_info.err_code = 0;
		exec_info.row_affect = 0;
		exec_info.err_msg = "";
		result = 2;
		goto finish;
	}
	else if (count <= 0)
	{
		error_code = "1";
		message = "no partition";
		result = 2;
		goto finish;
	}
	if (get_min_max_partition(mysql, remote_db, tb_name, min_partition, max_partition))
	{
		ss.str("");
		ss << exec_info.err_code;
		error_code = ss.str();
		message = exec_info.err_msg;
		exec_info.err_code = 0;
		exec_info.row_affect = 0;
		exec_info.err_msg = "";
		result = 2;
		goto finish;
	}

	inter_min = get_time_diff(min_partition);
	inter_max = get_time_diff(max_partition);
	if (inter_max <= 40)
	{
		add_partition_sql = tc_create_add_partition_sql(remote_db, tb_name, 
			db_partition_columnname, partition_column_type, interval_time,
			remote_hash_algorithm, max_partition);
		if (tc_exec_sql_without_result(mysql, add_partition_sql, &exec_info))
		{
			result = 2;
			ss.str("");
			ss << exec_info.err_code;
			error_code = ss.str();
			message = exec_info.err_msg;
			exec_info.err_code = 0;
			exec_info.row_affect = 0;
			exec_info.err_msg = "";
			goto finish;
		}
	}

	if (inter_min > expiration_time)
	{
		int del_total = inter_min - expiration_time;
		if (interval_time != 1)
		{
			del_total = 1;
		}
		del_total = del_total - 3 > 0 ? 3 : del_total;
		while (del_total>0) {
			if(tc_create_del_partition_sql(mysql, remote_db, tb_name,
				db_partition_columnname, partition_column_type,
				interval_time, remote_hash_algorithm, del_partition_sql))
			{
				error_code = 1;
				message = "fail to get min from information_schema.PARTITIONS";
				result = 2;
				goto finish;
			}
			retry = 3;
			while (retry > 0)
			{
				if (tc_exec_sql_without_result(mysql, del_partition_sql, &exec_info))
				{
					result = 2;
					ss.str("");
					ss << exec_info.err_code;
					error_code = ss.str();
					message = exec_info.err_msg;
					exec_info.err_code = 0;
					exec_info.row_affect = 0;
					exec_info.err_msg = "";
					--retry;
				}
				else
				{
					retry = -1;
				}
			}
			if (retry == 0) 
			{
				goto finish;
			}
			--del_total;
		}
	}

finish:
	if (tc_partiton_log(tdbctl_primary_conn, &exec_info, remote_db, tb_name,
		server_name, ipport, error_code, message))
	{
		result = 2;
		exec_info.err_code = 0;
		exec_info.row_affect = 0;
		exec_info.err_msg = "";
	}
	return result;
}

/*
get time which only exist digital
for example: '2020-03-31' --> 20200331
*/
string get_time_string(string str)
{
	string res;
	regex pattern("\\d+");
	string::const_iterator start = str.begin();
	string::const_iterator end = str.end();
	smatch mat;

	//get year month day
	while (regex_search(start, end, mat, pattern))
	{
		string msg(mat[0].first, mat[0].second);
		res += msg;
		start = mat[0].second;
	}
	return res;
}

/*
get time of 3 days later,for add partition
*/
void get_new_time(string str, int i, int interval, struct tm* l_time)
{
	time_t to_tm_time = (time_t)time((time_t*)0);
	time_t timer_new;
	struct tm tm_new;
	string res = get_time_string(str);
	string year = res.substr(0, 4);
	string month = res.substr(4, 2);
	month = month[0] == '0' ? month.substr(1, 1) : month;
	string day = res.substr(6, 2);
	day = day[0] == '0' ? day.substr(1, 1) : day;


	//change data structure to tm
	tm_new.tm_year = atoi(year.c_str()) - 1900;
	tm_new.tm_mon = atoi(month.c_str()) - 1;
	tm_new.tm_mday = atoi(day.c_str());
	tm_new.tm_hour = 0;
	tm_new.tm_min = 0;
	tm_new.tm_sec = 0;

	timer_new = mktime(&tm_new);
	to_tm_time = timer_new + i*TERM*interval;
	*l_time = *(localtime_r(&to_tm_time, &tm_new));
	return;
}

/*
init SQL for add partition
*/
string tc_create_add_partition_sql(string remote_db, string tb_name,
	string db_partition_columnname, string partition_column_type,
	int interval_time, string remote_hash_algorithm, string max_partition) 
{
	string alter_sql = "alter table " + remote_db + "." + tb_name + " add partition(";
	string partition_sql = " partition by " + remote_hash_algorithm;
	string values_sql = "";
	string add_partition_sql = "";

	int add_partition_num = 3;
	struct tm l_time_new ;

	char time_string[30] = { 0 };
	char time_string_new[30] = { 0 };
	COLUMN_TYPE column_type = get_column_type(partition_column_type);
	PARTITION_ALGORITHM_TYPE partition_type = get_partition_algorithm_type(remote_hash_algorithm);
	if (column_type == datetime_column_type || column_type == date_column_type)
	{
		partition_sql += " columns(`" + db_partition_columnname + "`)";
	}
	else if (column_type == int_column_type)
	{
		partition_sql += "(`" + db_partition_columnname + "`)";
	}
	
	if (partition_type == range_partition_algorithm)
	{
		values_sql = " values less than(";
	}
	else if (partition_type == list_partition_algorithm)
	{
		values_sql = " values in (";
	}


	for (int i = 0; i < add_partition_num; ++i)
	{
		get_new_time(max_partition, i+1, interval_time, &l_time_new);
		snprintf(time_string, sizeof(time_string) - 1, "%4d%02d%02d",
			1900 + l_time_new.tm_year,
			1 + l_time_new.tm_mon,
			l_time_new.tm_mday);
		if (column_type == datetime_column_type || column_type == date_column_type)
		{
			snprintf(time_string_new, sizeof(time_string_new) - 1, "'%4d-%02d-%02d'",
				1900 + l_time_new.tm_year,
				1 + l_time_new.tm_mon,
				l_time_new.tm_mday);
		}
		else if (column_type == int_column_type)
		{
			snprintf(time_string_new, sizeof(time_string_new) - 1, "%4d%02d%02d",
				1900 + l_time_new.tm_year,
				1 + l_time_new.tm_mon,
				l_time_new.tm_mday);
		}
		add_partition_sql = add_partition_sql + " partition p" +
			time_string + values_sql + time_string_new + "),";
		memset(time_string, 0, sizeof(time_string));
		memset(time_string_new, 0, sizeof(time_string_new));
	}
	add_partition_sql.erase(add_partition_sql.end() - 1);
	add_partition_sql += ")";
	alter_sql += add_partition_sql;
	return alter_sql;
}

/*
init sql for delete partition
*/
int tc_create_del_partition_sql(MYSQL* mysql, string remote_db, string tb_name,
	string db_partition_columnname, string partition_column_type,
	int interval_time, string remote_hash_algorithm, string& del_sql)
{
	int result = 0;
	string quotation = "\"";
	string min_partition = "";
	if (get_min_partition(mysql, remote_db, tb_name, min_partition))
	{
		result = 2;
		return result;
	}
	min_partition=get_time_string(min_partition);
	del_sql = "alter table ";
	del_sql += remote_db + "." + tb_name;
	del_sql += " drop partition p"+ min_partition;
	return result;
}

int tc_partiton_log(MYSQL* tdbctl_primary_conn, tc_exec_info* exec_info,
	string db, string tb_name, string server_name, string ipport,
	string error_code, string message) 
{
	int result = 0;
	time_t to_tm_time = (time_t)time((time_t*)0);
	struct tm lt;
	struct tm* l_time = localtime_r(&to_tm_time, &lt);

	string log_sql = "insert into cluster_admin.tc_partiton_admin_log( "
		" db_name,tb_name,server_name,host,code,message) values(";
	string quotation = "\"";

	log_sql += quotation + db + quotation + ",";
	log_sql += quotation + tb_name + quotation + ",";
	log_sql += quotation + server_name + quotation + ",";
	log_sql += quotation + ipport + quotation + ",";
	log_sql += error_code + ",";
	log_sql += quotation + message + quotation + ")";

	if (tc_exec_sql_without_result(tdbctl_primary_conn, log_sql, exec_info))
	{
		result = 2;
		fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN PARTITION_ADMIN] "
			"fail to log in  cluster_admin.tc_partiton_admin_log :%02d %s\n",
			l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, l_time->tm_hour,
			l_time->tm_min, l_time->tm_sec, exec_info->err_code, exec_info->err_msg.c_str());
	}
	return result;
}

/*
init partition for remote from  cluster_admin.tc_partiton_admin_config
*/
int tc_remote_new_partition(MYSQL* mysql, MYSQL* tdbctl_primary_conn,
	string remote_db, string tb_name, string  db_partition_columnname,
	string partition_column_type, int interval_time, string remote_hash_algorithm,
	string ipport,string server_name) 
{
	int result = 0;
	int count = 0;
	tc_exec_info exec_info;
	stringstream ss;
	
	//init for log sql
	string error_code = "0";
	string message = "";

	//init for alter sql
	string alter_sql = "";

	if (check_table_is_partitioned(mysql, remote_db, tb_name, &exec_info, count)) 
	{
		ss.str("");
		ss << exec_info.err_code;
		error_code = ss.str();
		message = exec_info.err_msg;
		exec_info.err_code = 0;
		exec_info.row_affect = 0;
		exec_info.err_msg = "";
		result = 2;
		goto finish;
	}
	else if (count > 0)
	{
		message = "already has partition,do nothing";
		goto finish;
	}

	alter_sql = tc_create_alter_sql(remote_db, tb_name, partition_column_type,
		interval_time, remote_hash_algorithm, db_partition_columnname);
	if (tc_exec_sql_without_result(mysql, alter_sql, &exec_info))
	{
		result = 2;
		ss.str("");
		ss << exec_info.err_code;
		error_code = ss.str();
		message = exec_info.err_msg;
		exec_info.err_code = 0;
		exec_info.row_affect = 0;
		exec_info.err_msg = "";
		goto finish;
	}

finish:
	if (tc_partiton_log(tdbctl_primary_conn,&exec_info, remote_db, tb_name,
		server_name, ipport, error_code, message))
	{
		result = 2;
		exec_info.err_code = 0;
		exec_info.row_affect = 0;
		exec_info.err_msg = "";
	}
	return result;
}

/*
init sql for new partition
*/
string tc_create_alter_sql(string remote_db, string tb_name,
	string partition_column_type, int interval_time,
	string remote_hash_algorithm, string  db_partition_columnname)
{
	string alter_sql = "";
	string partition_sql = " partition by " + remote_hash_algorithm;
	string values_sql = "";
	string add_partition_sql = "(";

	time_t to_tm_time = (time_t)time((time_t*)0);
	time_t to_tm_time_new;
	struct tm lt_new;
	struct tm* l_time_new;

	int init_partition_num = 15;
	int mediam_num = init_partition_num / 2;
	char time_string[30] = { 0 };
	char time_string_new[30] = { 0 };

	COLUMN_TYPE column_type = get_column_type(partition_column_type);
	PARTITION_ALGORITHM_TYPE partition_type = get_partition_algorithm_type(remote_hash_algorithm);
	if (column_type == datetime_column_type || column_type == date_column_type)
	{
		partition_sql += " columns(`" + db_partition_columnname + "`)";
	}
	else if (column_type == int_column_type)
	{
		partition_sql += "(`" + db_partition_columnname + "`)";
	}

	if (partition_type == range_partition_algorithm)
	{

		values_sql = " values less than(";
	}
	else if (partition_type == list_partition_algorithm)
	{
		values_sql = " values in (";
	}

	alter_sql = "alter table " + remote_db + "." + tb_name + " " + partition_sql;

	for (int i = 0; i < init_partition_num; ++i)
	{
		to_tm_time_new = to_tm_time + (i - mediam_num)*TERM*interval_time;
		l_time_new = localtime_r(&to_tm_time_new, &lt_new);
		snprintf(time_string, sizeof(time_string) - 1, "%4d%02d%02d",
			1900 + l_time_new->tm_year,
			1 + l_time_new->tm_mon,
			l_time_new->tm_mday);
		if (column_type == datetime_column_type || column_type == date_column_type)
		{
			snprintf(time_string_new, sizeof(time_string_new) - 1, "'%4d-%02d-%02d'",
				1900 + l_time_new->tm_year,
				1 + l_time_new->tm_mon,
				l_time_new->tm_mday);
		}
		else if (column_type == int_column_type)
		{
			snprintf(time_string_new, sizeof(time_string_new) - 1, "%4d%02d%02d",
				1900 + l_time_new->tm_year,
				1 + l_time_new->tm_mon,
				l_time_new->tm_mday);
		}
		add_partition_sql = add_partition_sql + " partition p" +
			time_string + values_sql + time_string_new + "),";
		memset(time_string, 0, sizeof(time_string));
		memset(time_string_new, 0, sizeof(time_string_new));
	}
	add_partition_sql.erase(add_partition_sql.end() - 1);
	add_partition_sql += ")";
	alter_sql += add_partition_sql;
	return alter_sql;
}

int check_table_is_partitioned(MYSQL* mysql, string remote_db, string tb_name,
	tc_exec_info *exec_info,int& count)
{
	int result = 0;
	string quotation = "\"";
	string check_sql_cur1 = "select count(*) from information_schema.PARTITIONS "
		"where TABLE_SCHEMA=";
	string check_sql_cur2 = " and PARTITION_NAME is NOT NULL and "
		" PARTITION_EXPRESSION is NOT NULL order by PARTITION_DESCRIPTION";
	string remote_db_table_sql = quotation + remote_db +
		quotation + " and TABLE_NAME=" + quotation + tb_name + quotation;
	string check_sql = check_sql_cur1 + remote_db_table_sql + check_sql_cur2;
	MYSQL_RES* res = tc_exec_sql_with_result(mysql, check_sql);
	MYSQL_ROW row = NULL;
	if (res && (row = mysql_fetch_row(res)))
	{
		count = row[0] ? atoi(row[0]) : 0;
	}
	else
	{
		result = 2;
		tc_exec_sql_without_result(mysql, check_sql, exec_info);
	}	
	return result;
}

/*
do tc_partition_admin_worker if current time equals to  tc_partition_admin_time
*/
bool get_time_flag()
{
	bool flag = false;
	time_t to_tm_time = (time_t)time((time_t*)0);
	struct tm lt;
	struct tm* l_time = localtime_r(&to_tm_time, &lt);
	ulong time = (l_time->tm_hour) * 3600 + (l_time->tm_min) * 60 + l_time->tm_sec;
	ulong t = time - tc_partition_admin_time;
	if (t<5 && t>0) 
	{
		flag = true;
		//make sure next time exceed expected time
		sleep(5);
	}
	return flag;
}

/*
ADMIN partition for remote
1.read config table
2.if no partition,init partition
3.if multi-partition , add and delete partition
*/
void tc_partition_admin_thread()
{
	while (1)
	{
		/*
	    TODO:get tc_tdbctl_conn_primary by host and port
	    */
		if (tc_partition_admin &&
			((tdbctl_is_primary = tc_is_primary_tdbctl_node()) > 0)) 
		{
			for (ulong i = 0; i <= tc_partition_admin_interval; ++i)
			{
				/*
				init partition when wait time to tc_partition_init_interval
				*/
				if (i % tc_partition_init_interval == 0) 
				{
					//init partition for cluster
					tc_partition_admin_worker(0);
				}
				/*
				init ,add or delete partition  if current time equals to  tc_partition_admin_time
				or wait time to tc_partition_admin_interval
				*/
				if (i == tc_partition_admin_interval || get_time_flag())
				{
					//ADMIN partition for cluster
					tc_partition_admin_worker(1);
					i = 0;
				}
				sleep(1);
			}
		}
		sleep(2);
	}
}
