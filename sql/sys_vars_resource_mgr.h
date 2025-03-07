#ifndef SYS_VARS_RESOURCE_MGR_INCLUDED
#define SYS_VARS_RESOURCE_MGR_INCLUDED
#include <hash.h>
#include "mysqld.h"                   // key_memory_THD_Session_sysvar_resource_manager
#include "malloc_allocator.h"         // Malloc_allocator
#include <map>

/* Copyright (c) 2014, 2016, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


/**
  Session_sysvar_resource_manager
  -------------------------------
  When a session (THD) gets initialized, it receives a shallow copy of all
  global system variables.
  thd->variables= global_system_variables; (see plugin_thdvar_init())

  In case of Sys_var_charptr variables, we need to maintain a separate copy for
  each session though so that global and session variables can be altered
  independently.

  This class is responsible for alloc|dealloc-ating memory for Sys_var_charptr
  variables for every session. It works in three steps :

  (1) init :
        Creates a copy (memdup()) of global Sys_var_charptr system variable for
        the respective session variable (passed as a  parameter) & inserts it
        into sysvar_string_alloc_hash (containing the alloced address) to infer
        that memory has been allocated for the session. init() is called during
        the initialization of session system variables. (plugin_thdvar_init())
  (2) update :
        When the session variable is updated, the old memory is freed and new
        memory is allocated to hold the new value. The corresponding member in
        sysvar_string_alloc_hash is also updated to hold the new alloced memory
        address. (Sys_var_charptr::session_update())
  (3) deinit :
        Its a one-shot operation to free all the session Sys_var_charptr system
        variables. It basically traverses down the sysvar_string_alloc_hash
        hash and calls free() for all the addresses that it holds.

  Note, there should always be at most one node per Sys_var_charptr session
  system variable.

*/

class Session_sysvar_resource_manager {

private:
  struct sys_var_ptr
  {
    void *data;
  };
  /**
    It maintains a member per Sys_var_charptr session variable to hold the
    address of non-freed memory, alloced to store the session variable's value.
  */
  HASH m_sysvar_string_alloc_hash;

  /**
    Returns the member that contains the given key (address).
  */
  uchar *find(void *key, size_t length);

public:

  Session_sysvar_resource_manager()
  {
    (void) memset(&m_sysvar_string_alloc_hash, 0, sizeof(m_sysvar_string_alloc_hash));
  }

  /**
    Allocates memory for Sys_var_charptr session variable during session
    initialization.
  */
  bool init(char **var, const CHARSET_INFO *charset);

  /**
    Frees the old alloced memory, memdup()'s the given val to a new memory
    address & updated the session variable pointer.
  */
  bool update(char **var, char *val, size_t val_len);

  static uchar *sysvars_mgr_get_key(const char *entry, size_t *length,
                                    my_bool not_used MY_ATTRIBUTE((unused)));

  void claim_memory_ownership();

  /**
    Frees the memory allocated for Sys_var_charptr session variables.
  */
  void deinit();
};


/**
  std::map, but with my_malloc, so that you can track the memory
  used using PSI memory keys.
*/
template <class Key, class Value, class Compare = std::less<Key> >
class malloc_map
    : public std::map<Key, Value, Compare, Malloc_allocator<std::pair<const Key, Value> > > {
 public:
  malloc_map(PSI_memory_key psi_key)
      : std::map<Key, Value, Compare, Malloc_allocator<std::pair<const Key, Value> > >(
             Compare(), Malloc_allocator<std::pair<const Key, Value> >(psi_key)) {}
};

/**
  Session_tc_sysvar_resource_manager
  -------------------------------
  When a session (THD) gets initialized, it receives a shallow copy of all
  global system variables.
  thd->variables= global_system_variables; (see plugin_thdvar_init())

  In case of Sys_var_tc_charptr variables, we need to maintain a separate copy for
  each session though so that global and session variables can be altered
  independently.

  This class is responsible for alloc|dealloc-ating memory for Sys_var_tc_charptr
  variables for every session.
*/
class Session_tc_sysvar_resource_manager {
 private:
  // The value always contains the string that the key points to.
  malloc_map<char **, char *> m_sysvar_string_alloc_hash;
 public:
  Session_tc_sysvar_resource_manager(): m_sysvar_string_alloc_hash(key_memory_THD_Session_sysvar_resource_manager) {

  }

  /**
    Allocates memory for Sys_var_charptr session variable during session
    initialization.
  */
  bool init(char **var);

  /**
    Frees the old allocated memory, memdup()'s the given val to a new memory
    address & updates the session variable pointer.
  */
  bool update(char **var, char *val, size_t val_len);

  /**
   * Parameter "bool claim" is removed.
  */
  void claim_memory_ownership();

  /**
    Frees the memory allocated for Sys_var_charptr session variables.
  */
  void deinit();
};
#endif /* SYS_VARS_RESOURCE_MGR_INCLUDED */


