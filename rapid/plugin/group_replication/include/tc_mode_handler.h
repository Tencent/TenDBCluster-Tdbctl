/*
    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

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
