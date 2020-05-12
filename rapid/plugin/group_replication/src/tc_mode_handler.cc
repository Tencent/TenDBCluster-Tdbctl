/* Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

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

