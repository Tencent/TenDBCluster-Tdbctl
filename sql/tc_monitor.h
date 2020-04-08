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
#endif /* TC_BASE_INCLUDED */