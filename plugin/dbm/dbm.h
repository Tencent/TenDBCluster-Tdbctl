/*
  Copyright (C) 2020 THL A29 Limited, a Tencent company. All rights reserved.
*/

#ifndef TDBCTL_DBM_H
#define TDBCTL_DBM_H

#define DBM_DAEMON_NAME "DBM"
#define DBM_TDBCTL_NODES_I_S_NAME "TDBCTL_NODES"

extern my_bool opt_allow_standalone_primary;

int dbm_enable_primary(THD *thd);
int dbm_disable_primary(THD *thd);
int dbm_get_primary(THD *thd);
int dbm_startup_enable_primary();

#endif // TDBCTL_DBM_H
