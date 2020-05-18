/*
    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

#include "tc_mode_handler.h"
#include "plugin.h"

long tdbctl_flush_routing(Sql_service_command_interface *command_interface)
{
  DBUG_ENTER("tdbctl_flush_routing");
  long error =0;

  DBUG_ASSERT(command_interface != NULL);

  error = command_interface->tdbctl_flush_routing();

  DBUG_RETURN(error);
}

long tdbctl_set_primary_on(Sql_service_command_interface *command_interface)
{
  DBUG_ENTER("tdbctl_set_primary_on");
  long error =0;

  DBUG_ASSERT(command_interface != NULL);

  error = command_interface->tdbctl_set_primary_on();

  DBUG_RETURN(error);
}

long tdbctl_set_primary_off(Sql_service_command_interface *command_interface)
{
  DBUG_ENTER("tdbctl_set_primary_off");
  long error =0;

  DBUG_ASSERT(command_interface != NULL);

  error = command_interface->tdbctl_set_primary_off();

  DBUG_RETURN(error);
}

int enable_tdbctl_primary_mode(enum_plugin_con_isolation session_isolation)
{
  Sql_service_command_interface *sql_command_interface=
      new Sql_service_command_interface();
  int error =
    sql_command_interface->
    establish_session_connection(session_isolation, get_plugin_pointer()) ||
    sql_command_interface->set_interface_user(GROUPREPL_USER) ||
    tdbctl_set_primary_on(sql_command_interface);
  delete sql_command_interface;
  return error;
}

int disable_tdbctl_primary_mode(enum_plugin_con_isolation session_isolation)
{
  Sql_service_command_interface *sql_command_interface=
      new Sql_service_command_interface();
  int error=
    sql_command_interface->
      establish_session_connection(session_isolation, get_plugin_pointer()) ||
    sql_command_interface->set_interface_user(GROUPREPL_USER) ||
    tdbctl_set_primary_off(sql_command_interface);
  delete sql_command_interface;
  return error;
}
