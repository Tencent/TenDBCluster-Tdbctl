#ifndef TC_SHOW_INCLUDE
#define TC_SHOW_INCLUDE

#include "my_global.h"
#include "sql_class.h" 
void tc_show_processlist(THD *thd, bool verbose, const char *server_name);
void tc_show_variables(THD *thd, enum_var_type type, String *wild, const char *server_name);
#endif
