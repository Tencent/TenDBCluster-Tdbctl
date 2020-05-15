/*
    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

#ifndef TC_MONITOR_INCLUDED
#define TC_MONITOR_INCLUDED

#include <iostream>
#include <string>
#include <map>
#include <set>
#include <sstream>
#include <regex>
#include "mysql.h"
using namespace std;
int tc_check_cluster_availability();
void create_check_cluster_availability_thread();
void tc_check_cluster_availability_thread();
int tc_check_cluster_availability_init();
int tc_monitor_log(string tdbctl_name, string spider_server_name, string host,
	string error_code, string message);
int tc_master_monitor_log(bool flag, string time_string, string spider_server_name);
#endif /* TC_MONITOR_INCLUDED */