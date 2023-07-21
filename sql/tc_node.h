/*
    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

#ifndef TC_NODE_INCLUDE
#define TC_NODE_INCLUDE

#include "my_global.h"
#include "mysql.h"
#include "sql_lex.h"

int tc_dump_node_schema(const char *host, uint port, const char *user, const char *password, const char *file, const char* wrapper);
int tc_dump_node_grant(const char *host, uint port, const char *user, const char * password, const char *file, const char* wrapper);
int tc_restore_to_node(const char *host, uint port, const char *user, const char *password, const char *file, const char* wrapper);

bool tc_load_schema_to_new_node(THD *thd, LEX *lex);
#endif
