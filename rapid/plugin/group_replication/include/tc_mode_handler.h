#/* Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

#ifndef TC_MODE_HANDLER_INCLUDE
#define TC_MODE_HANDLER_INCLUDE

#include "sql_service_command.h"

/**
  do tdbctl flush routing in the server.

  @param sql_service_command  Command interface given to execute the command

  @return the operation status
    @retval 0      OK
    @retval !=0    Error
*/
long tdbctl_flush_routing(Sql_service_command_interface *sql_service_command);

/**
  This method creates a server session and connects to the server
  to set the tc_is_primary ON  

  @param session_isolation session creation requirements: use current thread,
                           use thread but initialize it or create it in a
                           dedicated thread

  @return the operation status
    @retval 0      OK
    @retval !=0    Error
*/
long tdbctl_set_primary_on(Sql_service_command_interface *sql_service_command);

/**
  This method creates a server session and connects to the server
  to set the tc_is_primary OFF  

  @param session_isolation session creation requirements: use current thread,
                           use thread but initialize it or create it in a
                           dedicated thread

  @return the operation status
    @retval 0      OK
    @retval !=0    Error
*/
long tdbctl_set_primary_off(Sql_service_command_interface *sql_service_command);

#endif /* TC_MODE_HANDLER_INCLUDE */
