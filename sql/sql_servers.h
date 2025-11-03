#ifndef SQL_SERVERS_INCLUDED
#define SQL_SERVERS_INCLUDED

/* Copyright (c) 2006, 2013, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include "my_global.h"                  /* uint */
#include "sql_cmd.h"
#include "sql_string.h"
#include "sql_alloc.h"
#include "mysql.h"
#include <list>
#include <string>
#include <vector>
#include <set>
#include <map>

class THD;
struct LEX;
struct TABLE;
typedef struct st_mem_root MEM_ROOT;
class Cluster_conn_manager; 

class FOREIGN_SERVER : public Sql_alloc
{
public:
  char *server_name;
  long port;
  long version;
  size_t server_name_length;
  char *db, *scheme, *username, *password, *socket, *owner, *host, *sport;

  FOREIGN_SERVER()
    : server_name(NULL), port(-1), server_name_length(0), db(NULL),
    scheme(NULL), username(NULL), password(NULL), socket(NULL),
    owner(NULL), host(NULL), sport(NULL)
  { }
};


/* cache handlers */
bool servers_init(bool dont_read_server_table);
bool servers_reload(THD *thd);
void servers_free(bool end=0);

/* lookup functions */
FOREIGN_SERVER* get_server_by_name(
  MEM_ROOT *mem, 
  const char *server_name,
  FOREIGN_SERVER *server_buffer
);

std::string get_new_server_name_by_wrapper(
		const char* wrapper_name
);

std::string get_new_server_name_by_number(
  const char* wrapper_name,
  const long number
);

int get_node_type_by_wrapper(
  const char* wrapper_name
);

const char *get_wrapper_prefix_by_wrapper(
	const char *wrapper_name
);

void trim_server_name_slave_suffix(std::string &server_name);
void trim_wrapper_name_slave_suffix(std::string &wrapper_name);

ulong get_servers_count();
ulong get_modify_server_version();
ulong tc_get_server_cache_update_time();
ulong get_server_version_by_name(const char* server_name);
int back_up_one_server(FOREIGN_SERVER* server);
bool update_server_version(bool* version_updated);
void get_deleted_servers();
bool backup_server_cache();
int delete_redundant_routings();
int get_remote_changed_servers(
  MEM_ROOT* mem_root, 
  std::list<FOREIGN_SERVER*>* diff_serverlist
);

void get_server_by_wrapper(
  std::list<FOREIGN_SERVER*>& server_list, 
  MEM_ROOT* mem, 
  const char* wrapper_name, 
  bool with_slave
);

ulong get_servers_count_by_wrapper(
	const char* wrapper_name, 
	bool with_slave);

/**
 * @brief flush routing to the mysql.servers of other nodes with conn_mgr
 * 
 * @param lex 
 * @param conn_mgr 
 * @return true means failure
 * @return false means success
 */
bool tc_flush_routing(LEX *lex, Cluster_conn_manager* conn_mgr);

/**
 * @brief flush routing to the specific nodes according to wrapper name
 * 
 * @param lex 
 * @param nodes_to_be_flushed 
 * @param conn_mgr 
 * @param wrapper 
 * @return true means failure
 * @return false means success
 */
bool tc_flush_routing_to_nodes(LEX* lex, std::set<std::string> nodes_to_be_flushed, Cluster_conn_manager* conn_mgr, const char* wrapper);

/**
 * Flush routing information to foreign servers.
 * 
 * This function creates a temporary Cluster_conn_manager object, adds the user-specified 
 * foreign servers to this temporary Cluster_conn_manager object, and then refreshes the 
 * routing for the foreign servers.
 *
 * Note: Please additionally ensure that the user-specified nodes are external nodes of the cluster.
 * 
 * @param lex The LEX structure containing server options and other command information
 * @return bool Returns true if any operation fails, false if all operations succeed
 */
bool tc_flush_routing_to_foreign_servers(LEX* lex);

enum FLUSH_ROUTING_RESULT {
  SUCCESS = 0,                   // success
  UNEXPECTED_WRAPPER,            // node type that cannot flush routing
  SET_OPTION_FAILURE,            // fail to set option
  SYNC_SERVERS_FAILURE,          // fail to modify mysql.servers table
  FLUSH_TABLE_FAILURE,           // fail to execute 'flush tables; flush table with read lock;'
  FLUSH_PRIV_FAILURE,            // fail to execute 'flush privileges;'
};
extern const char *FLUSH_ROUTING_INFO[];
int tc_check_and_repair_routing();
void create_check_and_repaire_routing_thread();

/**
 * @brief Prepare for 'TDBCTL CREATE NODE' command
 * 
 * This function will:
 * - Check server options
 * - Generate proper server name
 * - Check ip#port conflicts
 * - Validate IP consistency
 * 
 * @param[in]  thd      Thread handler
 * @param[in]  lex      LEX structure containing server options
 * @param[out] err_msg  Error message buffer
 * @retval false        Preparation succeeded
 * @retval true         Preparation failed (error message will be stored in err_msg)
 */
bool prepare_server_creation(THD *thd, LEX *lex, std::string &err_msg);


/**
   This class represent server options as set by the parser.
 */

class Server_options
{
public:
  static const long PORT_NOT_SET= -1;
  static const long NUM_NOT_SET= -1;
  LEX_STRING m_server_name;
private:
  long m_port;
  long m_num;
  LEX_STRING m_host;
  LEX_STRING m_db;
  LEX_STRING m_username;
  LEX_STRING m_password;
  LEX_STRING m_scheme;
  LEX_STRING m_socket;
  LEX_STRING m_owner;

public:
  void set_port(long port)               { m_port= port; }
  void set_num(long num)                 { m_num= num; }
  void set_host(LEX_STRING host)         { m_host= host; }
  void set_db(LEX_STRING db)             { m_db= db; }
  void set_username(LEX_STRING username) { m_username= username; }
  void set_password(LEX_STRING password) { m_password= password; }
  void set_scheme(LEX_STRING scheme)     { m_scheme= scheme; }
  void set_socket(LEX_STRING socket)     { m_socket= socket; }
  void set_owner(LEX_STRING owner)       { m_owner= owner; }

  long get_port() const            { return m_port; }
  long get_num() const             { return m_num; }
  const char *get_host() const     { return m_host.str; }
  const char *get_db() const       { return m_db.str; }
  const char *get_username() const { return m_username.str; }
  const char *get_password() const { return m_password.str; }
  const char *get_scheme() const   { return m_scheme.str; }
  const char *get_socket() const   { return m_socket.str; }
  const char *get_owner() const    { return m_owner.str; }

  /**
     Reset all strings to NULL and port to PORT_NOT_SET.
     This prepares the structure for being used by a new statement.
  */
  void reset();

  /**
     Create a cache entry and insert it into the cache.

     @returns false if entry was created and inserted, true otherwise.
  */
  bool insert_into_cache() const;

  /**
     Update a cache entry.

     @param existing  Cache entry to update

     @returns false if the entry was updated, true otherwise.
  */
  bool update_cache(FOREIGN_SERVER *existing) const;

  /**
     Create a record representing these server options,
     ready to be inserted into the mysql.servers table.

     @param table  Table to be inserted into.
  */
  void store_new_server(TABLE *table) const;

  /**
     Create a record for updating a row in the mysql.servers table.

     @param table     Table to be updated.
     @param existing  Cache entry represeting the existing values.
  */
  void store_altered_server(TABLE *table, FOREIGN_SERVER *existing) const;
};


/**
   This class has common code for CREATE/ALTER/DROP SERVER statements.
*/

class Sql_cmd_common_server : public Sql_cmd
{
protected:
  TABLE *table;

  Sql_cmd_common_server()
    : table(NULL)
  { }

  virtual ~Sql_cmd_common_server()
  { }

  /**
     Check permissions and open the mysql.servers table.

     @param thd  Thread context

     @returns false if success, true otherwise
  */
  bool check_and_open_table(THD *thd);
};


/**
   This class implements the CREATE SERVER statement.
*/

class Sql_cmd_create_server : public Sql_cmd_common_server
{
  /**
     Server_options::m_server_name contains the name of the
     server to create. The remaining Server_options fields
     contain options as set by the parser.
     Unset options are NULL (or PORT_NOT_SET for port).
  */
  const Server_options *m_server_options;

public:
  Sql_cmd_create_server(Server_options *server_options)
    : Sql_cmd_common_server(), m_server_options(server_options)
  { }

  enum_sql_command sql_command_code() const
  { return SQLCOM_CREATE_SERVER; }

  /**
     Create a new server by inserting a row into the
     mysql.server table and creating a cache entry.

     @param thd  Thread context

     @returns false if success, true otherwise
  */
  bool execute(THD *thd);
};


/**
   This class implements the TDBCTL CREATE NODE statement.
*/
class Sql_cmd_create_multi_server : public Sql_cmd_common_server
{

  const std::vector<Server_options> &m_server_options_list;

public:
  Sql_cmd_create_multi_server(std::vector<Server_options> &server_options_list)
    : Sql_cmd_common_server(), m_server_options_list(server_options_list)
  { }

  enum_sql_command sql_command_code() const
  { return TC_SQLCOM_CREATE_NODE; }

  /**
     Create multiple new servers by inserting rows into the
     mysql.servers table and creating cache entries for each.
     The servers to create are specified in m_server_options_list.

     @param thd  Thread context

     @returns false if all servers were created successfully, 
              true if any creation failed
  */
  bool execute(THD *thd);
};


/**
   This class implements the ALTER SERVER statement.
*/

class Sql_cmd_alter_server : public Sql_cmd_common_server
{
  /**
     Server_options::m_server_name contains the name of the
     server to change. The remaining Server_options fields
     contain changed options as set by the parser.
     Unchanged options are NULL (or PORT_NOT_SET for port).
  */
  const Server_options *m_server_options;

public:
  Sql_cmd_alter_server(Server_options *server_options)
    : Sql_cmd_common_server(), m_server_options(server_options)
  { }

  enum_sql_command sql_command_code() const
  { return SQLCOM_ALTER_SERVER; }

  /**
     Alter an existing server by updating the matching row in the
     mysql.servers table and updating the cache entry.

     @param thd  Thread context

     @returns false if success, true otherwise
  */
  bool execute(THD *thd);
};


/**
   This class implements the DROP SERVER statement.
*/

class Sql_cmd_drop_server : public Sql_cmd_common_server
{
  /// Name of server to drop
  LEX_STRING m_server_name;

  /// Is this DROP IF EXISTS?
  bool m_if_exists;

public:
  Sql_cmd_drop_server(LEX_STRING server_name,
                      bool if_exists)
    : Sql_cmd_common_server(),
    m_server_name(server_name), m_if_exists(if_exists)
  { }

  enum_sql_command sql_command_code() const
  { return SQLCOM_DROP_SERVER; }

  /**
     Drop an existing server by deleting the matching row from the
     mysql.servers table and removing the cache entry.

     @param thd  Thread context

     @returns false if success, true otherwise
  */
  bool execute(THD *thd);
};


/**
   This class currently only serves the rollback operation for 
   the TDBCTL CREATE NODE command.
*/
class Sql_cmd_drop_multi_server : public Sql_cmd_common_server
{

  const std::vector<Server_options> &m_server_options_list;

public:
  Sql_cmd_drop_multi_server(std::vector<Server_options> &server_options_list)
    : Sql_cmd_common_server(), m_server_options_list(server_options_list)
  { }

  enum_sql_command sql_command_code() const
  { return TC_SQLCOM_CREATE_NODE; }

  /**
     Drop multiple existing servers by deleting the matching rows from the
     mysql.servers table and removing their cache entries.

     @param thd  Thread context

     @returns false if all servers were dropped successfully, 
              true if any drop operation failed
  */
  bool execute(THD *thd);
};


bool server_compare(FOREIGN_SERVER*& first, FOREIGN_SERVER*& second);

struct SPIDER_AUTOINC_INFO {
  bool mode_switch;
  unsigned int mode_value;
  unsigned int inc_step;

  SPIDER_AUTOINC_INFO() : mode_switch(false), mode_value(0), inc_step(0) {}
  SPIDER_AUTOINC_INFO(bool mode_switch, unsigned int mode_value, unsigned int inc_step) :
    mode_switch(mode_switch), mode_value(mode_value), inc_step(inc_step) {}
};

enum SPIDER_AUTOINC_CONFLICT {
  SPIDER_AUTOINC_CONFLICT_NONE = 0,     // no conflict
  SPIDER_AUTOINC_CONFLICT_MODE_SWITCH,  // different mode switch
  SPIDER_AUTOINC_CONFLICT_MODE_VALUE,   // same mode value
  SPIDER_AUTOINC_CONFLICT_INC_STEP,     // different inc step
};

typedef std::pair<SPIDER_AUTOINC_CONFLICT, std::string> SPIDER_AUTOINC_CONFLICT_ITEM;

bool get_spider_autoinc_info(const std::map<std::string, MYSQL *> &spider_conns,
                             std::map<std::string, SPIDER_AUTOINC_INFO> &autoinc_info, 
                             std::string &errmsg);

bool validate_auto_increment_settings(const std::map<std::string, SPIDER_AUTOINC_INFO> &autoinc_info, 
                                      std::vector<SPIDER_AUTOINC_CONFLICT_ITEM> &failure_details);

bool find_auto_increment_conflict(const std::map<std::string, SPIDER_AUTOINC_INFO> &cluster_autoinc_map, 
                                  const std::string &foreign_server_name, 
                                  const SPIDER_AUTOINC_INFO &foreign_server_autoinc,
                                  SPIDER_AUTOINC_CONFLICT &conflict_type,
                                  std::string &conflict_str);

bool check_autoinc_settings_for_new_spider_node(THD *thd, LEX *lex);

bool check_autoinc_for_multiple_new_spider_nodes(THD *thd, LEX *lex);

bool restore_one_server(FOREIGN_SERVER* server_backup);
bool restore_server_cache();

#endif /* SQL_SERVERS_INCLUDED */
