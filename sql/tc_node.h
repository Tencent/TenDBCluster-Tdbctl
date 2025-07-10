/*
    Copyright (C) 2020 Tencent.  All rights reserved.
*/

#ifndef TC_NODE_INCLUDE
#define TC_NODE_INCLUDE

#include "my_global.h"
#include "mysql.h"
#include "sql_lex.h"
#include "sql_servers.h"

int tc_dump_node_schema(const char *host, uint port, const char *user, const char *password, const char *file, const char* wrapper);
int tc_dump_node_grant(const char *host, uint port, const char *user, const char * password, const char *file, const char* wrapper);
int tc_restore_to_node(const char *host, uint port, const char *user, const char *password, const char *file, const char* wrapper);

bool tc_load_schema_to_new_node(THD *thd, LEX *lex, FOREIGN_SERVER *dump_server);
std::pair<FOREIGN_SERVER *, std::string> tc_find_dump_source_node(THD *thd, LEX *lex);
#endif
