/*
  Copyright (C) 2020 Tencent. All rights reserved.
*/
#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif
#include <sql_class.h>
#include <table.h>
#include <sql_show.h>
#include <sp_instr.h>
#include <sql_prepare.h>

#include "dbm.h"

my_bool opt_allow_standalone_primary = FALSE;
uint opt_enable_primary_retry_times = 6;
uint opt_enable_primary_initial_interval = 3;

extern ST_FIELD_INFO tdbctl_nodes_fields_info[];
extern int i_s_tdbctl_nodes_fill(THD *thd, TABLE_LIST *tables, Item *cond);

static struct st_mysql_information_schema dbm_i_s_descriptor = {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static struct st_mysql_daemon dbm_daemon_descriptor = {
    MYSQL_DAEMON_INTERFACE_VERSION};

/* Variables */

static MYSQL_SYSVAR_BOOL(
    allow_standalone_primary, opt_allow_standalone_primary, PLUGIN_VAR_OPCMDARG,
    "Whether a Primary node with no replicas should be allowed.", NULL, NULL,
    FALSE);

static MYSQL_SYSVAR_UINT(
    enable_primary_retry_times, opt_enable_primary_retry_times, PLUGIN_VAR_OPCMDARG, 
    "The number of retries for attempting to set itself "
    "as the primary node during startup.",
    NULL, NULL, 6, 1, 25, 1);

static MYSQL_SYSVAR_UINT(
    enable_primary_initial_interval, opt_enable_primary_initial_interval, PLUGIN_VAR_OPCMDARG, 
    "Initial retry interval (seconds) for attempting to set itself "
    "as the primary node during startup.",
    NULL, NULL, 3, 1, 120, 1);

static struct st_mysql_sys_var *dbm_vars[] = {
    MYSQL_SYSVAR(allow_standalone_primary),
    MYSQL_SYSVAR(enable_primary_retry_times),
    MYSQL_SYSVAR(enable_primary_initial_interval),
};

/* Init/Deinit functions */

static int i_s_tdbctl_nodes_init(void *arg) {
  ST_SCHEMA_TABLE *table = (ST_SCHEMA_TABLE *)arg;
  table->fields_info = tdbctl_nodes_fields_info;
  table->fill_table = i_s_tdbctl_nodes_fill;
  return 0;
}

static int i_s_tdbctl_nodes_deinit(void *arg) { return 0; }

static int dbm_daemon_init(void *arg) {
  tdbctl_enable_primary = dbm_enable_primary;
  tdbctl_disable_primary = dbm_disable_primary;
  tdbctl_get_primary = dbm_get_primary;
  tdbctl_startup_enable_primary = dbm_startup_enable_primary;
  return 0;
}

static int dbm_daemon_deinit(void *arg) {
  reset_tdbctl_primary_functions();
  return 0;
}

mysql_declare_plugin(dbm)
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &dbm_i_s_descriptor,
  DBM_TDBCTL_NODES_I_S_NAME,
  "Tencent CROS SCC",
  "Info of TDBCTL nodes within the cluster",
  PLUGIN_LICENSE_GPL,
  i_s_tdbctl_nodes_init,
  i_s_tdbctl_nodes_deinit,
  0x0100,
  NULL,
  NULL,
  (void *)"1.0",
  0
},
{
  MYSQL_DAEMON_PLUGIN,
  &dbm_daemon_descriptor,
  DBM_DAEMON_NAME,
  "Tencent CROS SCC",
  "DBM plugin",
  PLUGIN_LICENSE_GPL,
  dbm_daemon_init,
  dbm_daemon_deinit,
  0x0100,
  NULL,
  dbm_vars,
  (void *)"1.0",
  0
}
mysql_declare_plugin_end;
