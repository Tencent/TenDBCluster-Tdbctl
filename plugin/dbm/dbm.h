/*
  Copyright (C) 2020 Tencent. All rights reserved.
*/

#ifndef TDBCTL_DBM_H
#define TDBCTL_DBM_H

#define DBM_DAEMON_NAME "DBM"
#define DBM_TDBCTL_NODES_I_S_NAME "TDBCTL_NODES"

extern my_bool opt_allow_standalone_primary;
extern uint opt_enable_primary_retry_times;
extern uint opt_enable_primary_initial_interval;

int dbm_enable_primary(THD *thd);
int dbm_disable_primary(THD *thd);
int dbm_get_primary(THD *thd);
int dbm_startup_enable_primary();

#endif // TDBCTL_DBM_H
