/*
     Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

#ifndef TC_XA_REPAIR_INCLUDED
#define TC_XA_REPAIR_INCLUDED

/* Check unexpected prepared transaction and repair them */

#include "my_global.h"                  /* uint */
#include "sql_cmd.h"
#include "sql_string.h"
#include "sql_alloc.h"
#include "mysql.h"
#include <list>
#include<string>
#include<map>
#include<set>
using namespace std;

void create_tc_xa_repair_thread();
void tc_xa_repair_thread();
void tc_check_and_repair_trans();
void tc_get_remote_prepared_trans(
  map<string, MYSQL*>& conn_map,
  map<string, string>& user_map,
  map<string, string>& passwd_map,
  int time,
  set<string>& xid_set
);
void tc_check_status_from_commit_logs(
  map<string, MYSQL*>& conn_map,
  map<string, string>& user_map,
  map<string, string>& passwd_map,
  set<string>& xid_set,
  set<string>& commit_set,
  set<string>& rollback_set
);
void tc_process_prepared_trans(
  map<string, MYSQL*>& conn_map,
  map<string, string>& user_map,
  map<string, string>& passwd_map,
  string pre_sql,
  set<string>& commit_set
);


#endif /* TC_XA_REPAIR_INCLUDED */