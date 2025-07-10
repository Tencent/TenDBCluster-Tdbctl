/*
    Copyright (C) 2020 Tencent.  All rights reserved.
*/

#ifndef TC_SHOW_INCLUDE
#define TC_SHOW_INCLUDE

#include "my_global.h"
#include "sql_class.h"
int tc_show_processlist(THD *thd, bool verbose, LEX_CSTRING from_server);
int tc_show_variables(THD *thd, enum_var_type type, String *wild,
                      LEX_CSTRING from_server);
#endif
