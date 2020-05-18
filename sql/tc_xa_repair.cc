/*
    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

#include "tc_xa_repair.h"
#include "sql_base.h"        
#include "sql_lex.h"
#include "tc_base.h"
#include "log.h"
#include <thread>
#include <string>
#include <list>
#include <mutex>

void create_tc_xa_repair_thread()
{
  std::thread t(tc_xa_repair_thread);
  t.detach();
}

void tc_xa_repair_thread()
{
  while (1)
  {
		if (tc_check_repair_trans &&
			(tc_is_primary_tdbctl_node() == 1))
    {
      tc_check_and_repair_trans();
      // tc_max_prepared_time - 2, because of sleep(2)
      // tc_max_prepared_time - 4 , 4 is the estimated thread execute time
      for (ulong i = 0; i < tc_max_prepared_time - 2 - 2; i++)
        sleep(1);
    }
    sleep(2);
  }
}

void tc_check_and_repair_trans()
{
  std::map<std::string, MYSQL*> remote_conn_map;
  std::map<std::string, std::string> remote_user_map;
  std::map<std::string, std::string> remote_passwd_map;
  std::map<std::string, std::string> remote_ipport_map;
  MEM_ROOT mem_root;
  int ret = 0;
  init_sql_alloc(key_memory_for_tdbctl, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);

  remote_ipport_map = get_remote_ipport_map(&mem_root,
    remote_user_map, remote_passwd_map);

  remote_conn_map = tc_remote_conn_connect(ret, remote_ipport_map,
    remote_user_map, remote_passwd_map);


  int max_time = tc_max_prepared_time;
  set<string> xid_set;
  set<string> commit_set;
  set<string> rollback_set;
  /* get remote prepared transaction from xa recover with time */
  /* TODO. push exceed time to data node */
  tc_get_remote_prepared_trans(remote_conn_map, remote_user_map,
    remote_passwd_map, max_time, xid_set );

  /* check  if prepared transactions has been recorded in mysql.xa_commit_log */
  tc_check_status_from_commit_logs( remote_conn_map, remote_user_map,
    remote_passwd_map, xid_set, commit_set, rollback_set );

  /* xa commit prepared transactions */
  tc_process_prepared_trans( remote_conn_map, remote_user_map,
    remote_passwd_map, "xa commit ", commit_set);

  /* xa rollback prepared transactions */
  tc_process_prepared_trans(remote_conn_map, remote_user_map,
    remote_passwd_map, "xa rollback ", rollback_set);

  free_root(&mem_root, MYF(0));
}

void tc_get_remote_prepared_trans(
  map<string, MYSQL*> &conn_map,
  map<string, string> &user_map,
  map<string, string> &passwd_map,
  int max_time, 
  set<string> &xid_set
)
{
  string exec_sql = "xa recover with time";
  bool error_retry = FALSE;
  map<string, MYSQL_RES*> result_map;
  map<string, MYSQL*>::iterator its;
  map<string, MYSQL_RES*>::iterator its_res;
  time_t to_tm_time = (time_t)time((time_t*)0);

  for (its = conn_map.begin(); its != conn_map.end(); its++)
  {/* init for  result_map */
    string ipport = its->first;
    MYSQL_RES* res = NULL;
    result_map.insert(pair<string, MYSQL_RES*>(ipport, res));
  }

  /* 
  TODO. for "xa recover with time",  TenDB can return the xid prepared time exceed max_time, 
  but not all prepared transaction 
  */
  tc_exec_sql_paral_with_result( exec_sql, conn_map,
    result_map, user_map, passwd_map, error_retry);

  for (its_res = result_map.begin(); its_res != result_map.end(); its_res++)
  {
    string ipport = its_res->first;
    MYSQL_RES* res = its_res->second;

    if (res)
    {
      MYSQL_ROW row = NULL;
      while ((row = mysql_fetch_row(res)))
      {
        string xid = row[3]; // row->data
        string prepare_time = row[4]; // row->prepare_time
        my_time_t time_s = string_to_timestamp(prepare_time);
        if (to_tm_time - time_s > max_time)
        {/* xa transaction prepared time exceed more than max_time */
          xid_set.insert(xid);
        }
      }
      mysql_free_result(res);
    }
    else
    {
			sql_print_warning("TDBCTL: ipport is %s," 
				"check prepared transaction mismatch",
				ipport.c_str());
    }
  }
}

void tc_check_status_from_commit_logs(
  map<string, MYSQL*> &conn_map,
  map<string, string> &user_map,
  map<string, string> &passwd_map,
  set<string> &xid_set,
  set<string> &commit_set,
  set<string> &rollback_set
)
{
  set<string>::iterator its;
  map<string, MYSQL*>::iterator its2;
  map<string, MYSQL_RES*>::iterator its_res;
  map<string, MYSQL_RES*> result_map;
  /* TODO pre_sql also need to limit the xid commit time in mysql.xa_commit_log */
  string pre_sql = "select * from mysql.xa_commit_log where xid=";
  for (its2 = conn_map.begin(); its2 != conn_map.end(); its2++)
  {/* init for  result_map */
    string ipport = its2->first;
    MYSQL_RES* res = NULL;
    result_map.insert(pair<string, MYSQL_RES*>(ipport, res));
  }

  for (its = xid_set.begin(); its != xid_set.end(); its++)
  {
    string xid = *its;
    string exec_sql = pre_sql + "\"" + xid + "\"";
    tc_exec_sql_paral_with_result(exec_sql,
      conn_map, result_map, user_map, passwd_map, FALSE);

    /* 
    if the xid exist in xa_commit_log of any node , 
    we have to commit the transaction;
    if the xid don't exist in xa_commit_log does not exist for all nodes, 
    we roll back the transaction
    */
    bool exist_xid = FALSE;
    bool need_retry = FALSE;
    for (its_res = result_map.begin(); its_res != result_map.end(); its_res++)
    {
      string ipport = its_res->first;
      MYSQL_RES* res = its_res->second;
      if (res)
      {
        // TODO, may be we need to compare the commit_time and the prepared time
        if (mysql_num_rows(res) > 0)
        {/* xid exist in xa_commit_log */
          exist_xid = TRUE;
        }
        mysql_free_result(res);
      }
      else
      {
				sql_print_warning("TDBCTL: ipport is %s, "
					" failed to get xid info from mysql.xa_commit_log",
					ipport.c_str());
        need_retry = TRUE;
      }
    }
    if(exist_xid)
      commit_set.insert(xid);
    else
    {
      /* if some error happend when reading xid info from mysql.xa_commit_log, 
      we do nothing and retry next time */
      if(!need_retry) 
        rollback_set.insert(xid);
    }
  }
}


/* xa commit/rollback prepared transactions */
void tc_process_prepared_trans(
  map<string, MYSQL*>& conn_map,
  map<string, string>& user_map,
  map<string, string>& passwd_map,
  string pre_sql,
  set<string>& commit_set
)
{
  set<string>::iterator its;
  map<string, tc_exec_info> result_map;
  map<string, MYSQL*>::iterator its_tmp;

  for (its_tmp = conn_map.begin(); its_tmp != conn_map.end(); its_tmp++)
  {/* init for exec result: result_map */
    string ipport = its_tmp->first;
    tc_exec_info exec_info;
    exec_info.err_code = 0;
    exec_info.row_affect = 0;
    exec_info.err_msg = "";
    result_map.insert(pair<string, tc_exec_info>(ipport, exec_info));
  }

  for (its = commit_set.begin(); its != commit_set.end(); its++)
  {
    string xid = *its;
    string exec_sql = pre_sql + "\"" + xid + "\"";
    map<string, tc_exec_info>::iterator its_res;
    tc_exec_sql_paral(exec_sql, conn_map,
      result_map, user_map, passwd_map, FALSE);

    for (its_res = result_map.begin(); its_res != result_map.end(); its_res++)
    {
        string ipport = its_res->first;
        tc_exec_info exec_info = its_res->second;
        if (exec_info.err_code > 0)
        {// some reason
          // 1. don't exist, 1397
          // 2. connection also hold the xid, 1397
          // 3.failed, must be warning
          if (exec_info.err_code == 1397)
          {// ignore 1397 // ERROR 1397 (XAE04): XAER_NOTA: Unknown XID
          }
        }
        else
        {// succeed to repair unexpected prepared transaction
					sql_print_warning("TDBCTL: ipport is %s, "
						" succeed to repair unexpected prepared transaction", 
						ipport.c_str());
        }
    }
  }
}
