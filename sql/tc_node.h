/*
    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

#ifndef TC_NODE_INCLUDE
#define TC_NODE_INCLUDE

#include "my_global.h"
#include "mysql.h"

int tc_dump_node_schema(const char *host, uint port, const char *user, const char *password, const char *file);
int tc_restore_node_schema(const char *host, uint port, const char *user, const char *password, const char *file);

#endif
