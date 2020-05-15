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

