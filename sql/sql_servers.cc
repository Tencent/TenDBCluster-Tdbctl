/* Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

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


/*
  The servers are saved in the system table "servers"
  
  Currently, when the user performs an ALTER SERVER or a DROP SERVER
  operation, it will cause all open tables which refer to the named
  server connection to be flushed. This may cause some undesirable
  behaviour with regard to currently running transactions. It is 
  expected that the DBA knows what s/he is doing when s/he performs
  the ALTER SERVER or DROP SERVER operation.
  
  TODO:
  It is desirable for us to implement a callback mechanism instead where
  callbacks can be registered for specific server protocols. The callback
  will be fired when such a server name has been created/altered/dropped
  or when statistics are to be gathered such as how many actual connections.
  Storage engines etc will be able to make use of the callback so that
  currently running transactions etc will not be disrupted.
*/

#include "sql_servers.h"
#include "sql_base.h"                           // close_mysql_tables
#include "records.h"          // init_read_record, end_read_record
#include "hash_filo.h"
#include <m_ctype.h>
#include <stdarg.h>
#include "log.h"
#include "auth_common.h"
#include "sql_parse.h"
#include "lock.h"                               // MYSQL_LOCK_IGNORE_TIMEOUT
#include "transaction.h"      // trans_rollback_stmt, trans_commit_stmt
#include "sql_class.h"
#include "tc_base.h"
#include <thread>
#include <string>
#include <list>
#include <mutex>
#include <boost/algorithm/string/join.hpp>

/*
  We only use 1 mutex to guard the data structures - THR_LOCK_servers.
  Read locked when only reading data and write-locked for all other access.
*/

// static list<string> to_delete_servername_list;
static MEM_ROOT tc_mem;
static HASH servers_cache_bak;
static MEM_ROOT mem_bak;

ulong global_modify_server_version = 0;
static bool modify_tdbctl_flag = false;

static HASH servers_cache;
static MEM_ROOT mem;
static mysql_rwlock_t THR_LOCK_servers;

/**
   This enum describes the structure of the mysql.servers table.
*/
enum enum_servers_table_field
{
  SERVERS_FIELD_NAME= 0,
  SERVERS_FIELD_HOST,
  SERVERS_FIELD_DB,
  SERVERS_FIELD_USERNAME,
  SERVERS_FIELD_PASSWORD,
  SERVERS_FIELD_PORT,
  SERVERS_FIELD_SOCKET,
  SERVERS_FIELD_SCHEME,
  SERVERS_FIELD_OWNER
};

const char *FLUSH_ROUTING_INFO[] = {
  "success", 
  "node type that cannot flush routing", 
  "failed to set option", 
  "failed to modify mysql.servers table", 
  "failed to execute 'flush tables; flush table with read lock;'", 
  "failed to execute 'flush privileges;'", 
};

#define RESULT_SUCCEED  0
#define RESULT_FAILED   1
#define RESULT_ABNORMAL 0

static int check_all_privileges(MYSQL *mysql, const AUTH_INFO &auth);

static std::string generate_routing_sql_for_spider(bool is_slave_routing = false);
static std::string generate_routing_sql_for_tdbctl();
static bool get_server_from_table_to_cache(TABLE *table);

static uchar *servers_cache_get_key(FOREIGN_SERVER *server, size_t *length,
                                    my_bool not_used MY_ATTRIBUTE((unused)))
{
  *length= (uint) server->server_name_length;
  return (uchar*) server->server_name;
}

static PSI_memory_key key_memory_servers;

#ifdef HAVE_PSI_INTERFACE
static PSI_rwlock_key key_rwlock_THR_LOCK_servers;

static PSI_rwlock_info all_servers_cache_rwlocks[]=
{
  { &key_rwlock_THR_LOCK_servers, "THR_LOCK_servers", PSI_FLAG_GLOBAL}
};

static PSI_memory_info all_servers_cache_memory[]=
{
  { &key_memory_servers, "servers_cache", PSI_FLAG_GLOBAL}
};

static void init_servers_cache_psi_keys(void)
{
  const char* category= "sql";
  int count;

  count= array_elements(all_servers_cache_rwlocks);
  mysql_rwlock_register(category, all_servers_cache_rwlocks, count);

  count= array_elements(all_servers_cache_memory);
  mysql_memory_register(category, all_servers_cache_memory, count);
}
#endif /* HAVE_PSI_INTERFACE */

/*
  Initialize structures responsible for servers used in federated
  server scheme information for them from the server
  table in the 'mysql' database.

  SYNOPSIS
    servers_init()
      dont_read_server_table  TRUE if we want to skip loading data from
                            server table and disable privilege checking.

  NOTES
    This function is mostly responsible for preparatory steps, main work
    on initialization and grants loading is done in servers_reload().

  RETURN VALUES
    0	ok
    1	Could not initialize servers
*/

bool servers_init(bool dont_read_servers_table)
{
  THD  *thd;
  bool return_val= FALSE;
  DBUG_ENTER("servers_init");

#ifdef HAVE_PSI_INTERFACE
  init_servers_cache_psi_keys();
#endif

  /* init the mutex */
  if (mysql_rwlock_init(key_rwlock_THR_LOCK_servers, &THR_LOCK_servers))
    DBUG_RETURN(TRUE);

  /* initialise our servers cache */
  if (my_hash_init(&servers_cache, system_charset_info, 32, 0, 0,
                   (my_hash_get_key) servers_cache_get_key, 0, 0,
                   key_memory_servers))
  {
    return_val= TRUE; /* we failed, out of memory? */
    goto end;
  }

  /* initialise our servers cache */
  if (my_hash_init(&servers_cache_bak, system_charset_info, 32, 0, 0,
                  (my_hash_get_key) servers_cache_get_key, 0, 0,
                  key_memory_servers))
  {
    return_val = TRUE; /* we failed, out of memory? */
    goto end;
  }

  /* Initialize the mem root for data */
  init_sql_alloc(key_memory_servers, &mem, ACL_ALLOC_BLOCK_SIZE, 0);

  /* Initialize the mem root for data */
  init_sql_alloc(key_memory_servers, &tc_mem, ACL_ALLOC_BLOCK_SIZE, 0);

  if (dont_read_servers_table)
    goto end;

  /*
    To be able to run this from boot, we allocate a temporary THD
  */
  if (!(thd=new THD))
    DBUG_RETURN(TRUE);
  thd->thread_stack= (char*) &thd;
  thd->store_globals();
  /*
    It is safe to call servers_reload() since servers_* arrays and hashes which
    will be freed there are global static objects and thus are initialized
    by zeros at startup.
  */
  return_val= servers_reload(thd);
  delete thd;

end:
  DBUG_RETURN(return_val);
}

/*
  Initialize server structures

  SYNOPSIS
    servers_load()
      thd     Current thread
      tables  List containing open "mysql.servers"

  RETURN VALUES
    FALSE  Success
    TRUE   Error

  TODO
    Revert back to old list if we failed to load new one.
*/

static bool servers_load(THD *thd, TABLE *table)
{
  READ_RECORD read_record_info;
  bool return_val= TRUE;
  bool version_updated = FALSE;
  DBUG_ENTER("servers_load");

  init_sql_alloc(key_memory_servers, &mem_bak, ACL_ALLOC_BLOCK_SIZE, 0);
  backup_server_cache();
  my_hash_reset(&servers_cache);
  free_root(&mem, MYF(0));
  init_sql_alloc(key_memory_servers, &mem, ACL_ALLOC_BLOCK_SIZE, 0);

  if (init_read_record(&read_record_info, thd, table,
                       NULL, 1, 1, FALSE))
    DBUG_RETURN(TRUE);

  while (!(read_record_info.read_record(&read_record_info)))
  {
    /* return_val is already TRUE, so no need to set */
    if ((get_server_from_table_to_cache(table)))
      goto end;
  }

  update_server_version(&version_updated);
  // get_deleted_servers();
  if (version_updated)
  {
    global_modify_server_version++; /* mean flush privileges modify mysql.servers */
    sql_print_information("modify mysql.servers and do flush privileges, "
                          "server_version is %lu", 
                          global_modify_server_version);
  }
  return_val= FALSE;

end:
  end_read_record(&read_record_info);
  my_hash_reset(&servers_cache_bak);
  free_root(&mem_bak, MYF(0));
  DBUG_RETURN(return_val);
}


/*
  Forget current servers cache and read new servers 
  from the conneciton table.

  SYNOPSIS
    servers_reload()
      thd  Current thread

  NOTE
    All tables of calling thread which were open and locked by LOCK TABLES
    statement will be unlocked and closed.
    This function is also used for initialization of structures responsible
    for user/db-level privilege checking.

  RETURN VALUE
    FALSE  Success
    TRUE   Failure
*/

bool servers_reload(THD *thd)
{
  TABLE_LIST tables[1];
  bool return_val= true;
  DBUG_ENTER("servers_reload");

  DBUG_PRINT("info", ("locking servers_cache"));
  mysql_rwlock_wrlock(&THR_LOCK_servers);

  tables[0].init_one_table("mysql", 5, "servers", 7, "servers", TL_READ);
  if (open_trans_system_tables_for_read(thd, tables))
  {
    /*
      Execution might have been interrupted; only print the error message
      if an error condition has been raised.
    */
    if (thd->get_stmt_da()->is_error())
      sql_print_error("Can't open and lock privilege tables: %s",
                      thd->get_stmt_da()->message_text());
    goto end;
  }

  if ((return_val= servers_load(thd, tables[0].table)))
  {					// Error. Revert to old list
    /* blast, for now, we have no servers, discuss later way to preserve */

    DBUG_PRINT("error",("Reverting to old privileges"));
    servers_free();
  }

  close_trans_system_tables(thd);
end:
  DBUG_PRINT("info", ("unlocking servers_cache"));
  mysql_rwlock_unlock(&THR_LOCK_servers);

  /*
  if dml or alter tdbclt in mysql.servers, then modify_tdbctl_flag=true;
  need to maintain Tdbctl_is_primary
  if create node or drop node, need to update maintain Tdbctl_is_primary directly

  if global_modify_server_version<=1 ,means mysqld init,
  unable to maintain tdbctl_is_primary
  */
  if (modify_tdbctl_flag && global_modify_server_version > 1) 
  {
    tdbctl_is_primary = tc_is_primary_tdbctl_node();
  }
  modify_tdbctl_flag = false;
  // return_val = delete_redundant_routings();
  DBUG_RETURN(return_val);
}


/*
  Initialize structures responsible for servers used in federated
  server scheme information for them from the server
  table in the 'mysql' database.

  SYNOPSIS
    get_server_from_table_to_cache()
      TABLE *table         open table pointer


  NOTES
    This function takes a TABLE pointer (pointing to an opened
    table). With this open table, a FOREIGN_SERVER struct pointer
    is allocated into root memory, then each member of the FOREIGN_SERVER
    struct is populated. A char pointer takes the return value of get_field
    for each column we're interested in obtaining, and if that pointer
    isn't 0x0, the FOREIGN_SERVER member is set to that value, otherwise,
    is set to the value of an empty string, since get_field would set it to
    0x0 if the column's value is empty, even if the default value for that
    column is NOT NULL.

  RETURN VALUES
    0	ok
    1	could not insert server struct into global servers cache
*/

static bool get_server_from_table_to_cache(TABLE *table)
{
  /* alloc a server struct */
  char *ptr;
  char * const blank= (char*)"";
  FOREIGN_SERVER *server= new (&mem) FOREIGN_SERVER();

  DBUG_ENTER("get_server_from_table_to_cache");
  table->use_all_columns();

  /* get each field into the server struct ptr */
  ptr= get_field(&mem, table->field[SERVERS_FIELD_NAME]);
  server->server_name= ptr ? ptr : blank;
  server->server_name_length= strlen(server->server_name);
  ptr= get_field(&mem, table->field[SERVERS_FIELD_HOST]);
  server->host= ptr ? ptr : blank;
  ptr= get_field(&mem, table->field[SERVERS_FIELD_DB]);
  server->db= ptr ? ptr : blank;
  ptr= get_field(&mem, table->field[SERVERS_FIELD_USERNAME]);
  server->username= ptr ? ptr : blank;
  ptr= get_field(&mem, table->field[SERVERS_FIELD_PASSWORD]);
  server->password= ptr ? ptr : blank;
  ptr= get_field(&mem, table->field[SERVERS_FIELD_PORT]);
  server->sport= ptr ? ptr : blank;

  server->port= server->sport ? atoi(server->sport) : 0;
  server->version = 0;

  ptr= get_field(&mem, table->field[SERVERS_FIELD_SOCKET]);
  server->socket= ptr && strlen(ptr) ? ptr : blank;
  ptr= get_field(&mem, table->field[SERVERS_FIELD_SCHEME]);
  server->scheme= ptr ? ptr : blank;
  ptr= get_field(&mem, table->field[SERVERS_FIELD_OWNER]);
  server->owner= ptr ? ptr : blank;
  DBUG_PRINT("info", ("server->server_name %s", server->server_name));
  DBUG_PRINT("info", ("server->host %s", server->host));
  DBUG_PRINT("info", ("server->db %s", server->db));
  DBUG_PRINT("info", ("server->username %s", server->username));
  DBUG_PRINT("info", ("server->password %s", server->password));
  DBUG_PRINT("info", ("server->socket %s", server->socket));
  if (my_hash_insert(&servers_cache, (uchar*) server))
  {
    DBUG_PRINT("info", ("had a problem inserting server %s at %lx",
                        server->server_name, (long unsigned int) server));
    // error handling needed here
    DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}


/**
  Close all tables which match specified connection string or
  if specified string is NULL, then any table with a connection string.
*/

static bool close_cached_connection_tables(THD *thd,
                                           const char *connection_string,
                                           size_t connection_length)
{
  uint idx;
  TABLE_LIST tmp, *tables= NULL;
  bool result= FALSE;
  DBUG_ENTER("close_cached_connection_tables");
  DBUG_ASSERT(thd);

  memset(&tmp, 0, sizeof(TABLE_LIST));

  mysql_mutex_lock(&LOCK_open);

  for (idx= 0; idx < table_def_cache.records; idx++)
  {
    TABLE_SHARE *share= (TABLE_SHARE *) my_hash_element(&table_def_cache, idx);

    /*
      Skip table shares being opened to avoid comparison reading into
      uninitialized memory further below.

      Thus, in theory, there is a risk that shares are left in the
      cache that should really be closed (matching the submitted
      connection string), and this risk is already present since
      LOCK_open is unlocked before calling this function. However,
      this function is called as the final step of DROP/ALTER SERVER,
      so its goal is to flush all tables which were open before
      DROP/ALTER SERVER started. Thus, if a share gets opened after
      this function is called, the information about the server has
      already been updated, so the new table will use the new
      definition of the server.

      It might have been an issue, however if one thread started
      opening a federated table, read the old server definition into a
      share, and then a switch to another thread doing ALTER SERVER
      happened right before setting m_open_in_progress to false for
      the share. Because in this case ALTER SERVER would not flush
      the share opened by the first thread as it should have been. But
      luckily, server definitions affected by * SERVER statements are
      not read into TABLE_SHARE structures, but are read when we
      create the TABLE object in ha_federated::open().

      This means that ignoring shares that are in the process of being
      opened is safe, because such shares don't have TABLE objects
      associated with them yet.
    */
    if (share->m_open_in_progress)
      continue;

    /* Ignore if table is not open or does not have a connect_string */
    if (!share->connect_string.length || !share->ref_count)
      continue;

    /* Compare the connection string */
    if (connection_string &&
        (connection_length > share->connect_string.length ||
         (connection_length < share->connect_string.length &&
          (share->connect_string.str[connection_length] != '/' &&
           share->connect_string.str[connection_length] != '\\')) ||
         native_strncasecmp(connection_string, share->connect_string.str,
                     connection_length)))
      continue;

    /* close_cached_tables() only uses these elements */
    tmp.db= share->db.str;
    tmp.table_name= share->table_name.str;
    tmp.next_local= tables;

    tables= (TABLE_LIST *) memdup_root(thd->mem_root, (char*)&tmp,
                                       sizeof(TABLE_LIST));
  }
  mysql_mutex_unlock(&LOCK_open);

  if (tables)
    result= close_cached_tables(thd, tables, FALSE, LONG_TIMEOUT);

  DBUG_RETURN(result);
}


void Server_options::reset()
{
  m_server_name.str= NULL;
  m_server_name.length= 0;
  m_port= PORT_NOT_SET;
  m_num= NUM_NOT_SET;
  m_host.str= NULL;
  m_host.length= 0;
  m_db.str= NULL;
  m_db.length= 0;
  m_username.str= NULL;
  m_db.length= 0;
  m_password.str= NULL;
  m_password.length= 0;
  m_scheme.str= NULL;
  m_scheme.length= 0;
  m_socket.str= NULL;
  m_socket.length= 0;
  m_owner.str= NULL;
  m_owner.length= 0;
}


bool Server_options::insert_into_cache() const
{
  char *unset_ptr= (char*)"";
  DBUG_ENTER("Server_options::insert_into_cache");

  FOREIGN_SERVER *server= new (&mem) FOREIGN_SERVER();
  if (!server)
    DBUG_RETURN(true);

  /* these two MUST be set */
  if (!(server->server_name= strdup_root(&mem, m_server_name.str)))
    DBUG_RETURN(true);
  server->server_name_length= m_server_name.length;

  if (!(server->host= m_host.str ? strdup_root(&mem, m_host.str) : unset_ptr))
    DBUG_RETURN(true);

  if (!(server->db= m_db.str ? strdup_root(&mem, m_db.str) : unset_ptr))
    DBUG_RETURN(true);

  if (!(server->username= m_username.str ?
        strdup_root(&mem, m_username.str) : unset_ptr))
    DBUG_RETURN(true);

  if (!(server->password= m_password.str ?
        strdup_root(&mem, m_password.str) : unset_ptr))
    DBUG_RETURN(true);

  /* set to 0 if not specified */
  server->port= m_port != PORT_NOT_SET ? m_port : 0;
  server->version = 0;
  /*
  maintain for create server
  */
  global_modify_server_version++;
  if (!native_strncasecmp(m_server_name.str, tdbctl_control_wrapper_prefix, strlen(tdbctl_control_wrapper_prefix)))
  {
	  modify_tdbctl_flag = true;
  }

  if (!(server->socket= m_socket.str ?
        strdup_root(&mem, m_socket.str) : unset_ptr))
    DBUG_RETURN(true);

  if (!(server->scheme= m_scheme.str ?
        strdup_root(&mem, m_scheme.str) : unset_ptr))
    DBUG_RETURN(true);

  if (!(server->owner= m_owner.str ?
        strdup_root(&mem, m_owner.str) : unset_ptr))
    DBUG_RETURN(true);

  DBUG_RETURN(my_hash_insert(&servers_cache, (uchar*) server));
}


bool Server_options::update_cache(FOREIGN_SERVER *existing) const
{
  DBUG_ENTER("Server_options::update_cache");

  /*
    Note: Since the name can't change, we don't need to set it.
    This also means we can just update the existing cache entry.
  */

  /*
    The logic here is this: is this value set AND is it different
    than the existing value?
  */
  if (m_host.str && strcmp(m_host.str, existing->host) &&
      !(existing->host= strdup_root(&mem, m_host.str)))
    DBUG_RETURN(true);

  if (m_db.str && strcmp(m_db.str, existing->db) &&
      !(existing->db= strdup_root(&mem, m_db.str)))
    DBUG_RETURN(true);

  if (m_username.str && strcmp(m_username.str, existing->username) &&
      !(existing->username= strdup_root(&mem, m_username.str)))
    DBUG_RETURN(true);

  if (m_password.str && strcmp(m_password.str, existing->password) &&
      !(existing->password= strdup_root(&mem, m_password.str)))
    DBUG_RETURN(true);

  /*
    port is initialised to PORT_NOT_SET, so if unset, it will be -1
  */
  if (m_port != PORT_NOT_SET && m_port != existing->port)
    existing->port= m_port;

  if (m_socket.str && strcmp(m_socket.str, existing->socket) &&
      !(existing->socket= strdup_root(&mem, m_socket.str)))
    DBUG_RETURN(true);

  if (m_scheme.str && strcmp(m_scheme.str, existing->scheme) &&
      !(existing->scheme= strdup_root(&mem, m_scheme.str)))
    DBUG_RETURN(true);

  if (m_owner.str && strcmp(m_owner.str, existing->owner) &&
      !(existing->owner= strdup_root(&mem, m_owner.str)))
    DBUG_RETURN(true);

  DBUG_RETURN(false);
}


/**
   Helper function for creating a record for inserting
   a new server into the mysql.servers table.

   Set a field to the given parser string. If the parser
   string is empty, set the field to "" instead.
*/

static inline void store_new_field(TABLE *table,
                                   enum_servers_table_field field,
                                   const LEX_STRING *val)
{
  if (val->str)
    table->field[field]->store(val->str, val->length,
                                  system_charset_info);
  else
    table->field[field]->store("", 0U, system_charset_info);
}


void Server_options::store_new_server(TABLE *table) const
{
  store_new_field(table, SERVERS_FIELD_HOST, &m_host);
  store_new_field(table, SERVERS_FIELD_DB, &m_db);
  store_new_field(table, SERVERS_FIELD_USERNAME, &m_username);
  store_new_field(table, SERVERS_FIELD_PASSWORD, &m_password);

  if (m_port != PORT_NOT_SET)
    table->field[SERVERS_FIELD_PORT]->store(m_port);
  else
    table->field[SERVERS_FIELD_PORT]->store(0);

  store_new_field(table, SERVERS_FIELD_SOCKET, &m_socket);
  store_new_field(table, SERVERS_FIELD_SCHEME, &m_scheme);
  store_new_field(table, SERVERS_FIELD_OWNER, &m_owner);
}


/**
   Helper function for creating a record for updating
   an existing server in the mysql.servers table.

   Set a field to the given parser string unless
   the parser string is empty or equal to the existing value.
*/

static inline void store_updated_field(TABLE *table,
                                       enum_servers_table_field field,
                                       const char *existing_val,
                                       const LEX_STRING *new_val)
{
  if (new_val->str && strcmp(new_val->str, existing_val))
    table->field[field]->store(new_val->str, new_val->length,
                               system_charset_info);
}


void Server_options::store_altered_server(TABLE *table,
                                          FOREIGN_SERVER *existing) const
{
  store_updated_field(table, SERVERS_FIELD_HOST, existing->host, &m_host);
  store_updated_field(table, SERVERS_FIELD_DB, existing->db, &m_db);
  store_updated_field(table, SERVERS_FIELD_USERNAME,
                      existing->username, &m_username);
  store_updated_field(table, SERVERS_FIELD_PASSWORD,
                      existing->password, &m_password);

  if (m_port != PORT_NOT_SET && m_port != existing->port)
    table->field[SERVERS_FIELD_PORT]->store(m_port);

  store_updated_field(table, SERVERS_FIELD_SOCKET, existing->socket, &m_socket);
  store_updated_field(table, SERVERS_FIELD_SCHEME, existing->scheme, &m_scheme);
  store_updated_field(table, SERVERS_FIELD_OWNER, existing->owner, &m_owner);
}


bool Sql_cmd_common_server::check_and_open_table(THD *thd)
{
  if (check_global_access(thd, SUPER_ACL))
    return true;

  TABLE_LIST tables;
  tables.init_one_table("mysql", 5, "servers", 7, "servers", TL_WRITE);

  table= open_ltable(thd, &tables, TL_WRITE, MYSQL_LOCK_IGNORE_TIMEOUT);
  return (table == NULL);
}


bool Sql_cmd_create_server::execute(THD *thd)
{
  DBUG_ENTER("Sql_cmd_create_server::execute");

  if (Sql_cmd_common_server::check_and_open_table(thd))
    DBUG_RETURN(true);

  // Check for existing cache entries with same name
  mysql_rwlock_wrlock(&THR_LOCK_servers);
  if (my_hash_search(&servers_cache,
                     (uchar*) m_server_options->m_server_name.str,
                     m_server_options->m_server_name.length))
  {
    mysql_rwlock_unlock(&THR_LOCK_servers);
    my_error(ER_FOREIGN_SERVER_EXISTS, MYF(0),
             m_server_options->m_server_name.str);
    trans_rollback_stmt(thd);
    close_mysql_tables(thd);
    DBUG_RETURN(true);
  }

  int error;
  table->use_all_columns();
  empty_record(table);

  /* set the field that's the PK to the value we're looking for */
  table->field[SERVERS_FIELD_NAME]->store(
    m_server_options->m_server_name.str,
    m_server_options->m_server_name.length,
    system_charset_info);

  /* read index until record is that specified in server_name */
  error= table->file->ha_index_read_idx_map(
    table->record[0], 0,
    table->field[SERVERS_FIELD_NAME]->ptr,
    HA_WHOLE_KEY,
    HA_READ_KEY_EXACT);

  if (!error)
  {
    my_error(ER_FOREIGN_SERVER_EXISTS, MYF(0),
             m_server_options->m_server_name.str);
    error= 1;
  }
  else if (error != HA_ERR_KEY_NOT_FOUND && error != HA_ERR_END_OF_FILE)
  {
    /* if not found, err */
    table->file->print_error(error, MYF(0));
  }
  else
  {
    /* store each field to be inserted */
    m_server_options->store_new_server(table);

    /* write/insert the new server */
    if ((error= table->file->ha_write_row(table->record[0])))
      table->file->print_error(error, MYF(0));
    else
    {
      /* insert the server into the cache */
      if ((error= m_server_options->insert_into_cache()))
        my_error(ER_OUT_OF_RESOURCES, MYF(0));
    }
  }

  mysql_rwlock_unlock(&THR_LOCK_servers);

  if (modify_tdbctl_flag)
  {
    /*
    tc_is_primary_tdbctl_node need to get THR_LOCK_servers,
    so must maintain tdbctl_is_primary after unlock
    */
    // todo: I'm not sure if it is necessary to maintain the tdbctl_is_primary here
    tdbctl_is_primary = tc_is_primary_tdbctl_node();
    modify_tdbctl_flag = false;
  }

  if (error)
    trans_rollback_stmt(thd);
  else
    trans_commit_stmt(thd);
  close_mysql_tables(thd);

  if (error == 0 && !thd->killed)
    my_ok(thd, 1);
  DBUG_RETURN(error != 0 || thd->killed);
}


bool Sql_cmd_alter_server::execute(THD *thd)
{
  DBUG_ENTER("Sql_cmd_alter_server::execute");

  if (Sql_cmd_common_server::check_and_open_table(thd))
    DBUG_RETURN(true);

  // Find existing cache entry to update
  mysql_rwlock_wrlock(&THR_LOCK_servers);
  FOREIGN_SERVER *existing=
    (FOREIGN_SERVER *) my_hash_search(&servers_cache,
                                  (uchar*) m_server_options->m_server_name.str,
                                  m_server_options->m_server_name.length);
  if (!existing)
  {
    my_error(ER_FOREIGN_SERVER_DOESNT_EXIST, MYF(0),
             m_server_options->m_server_name.str);
    mysql_rwlock_unlock(&THR_LOCK_servers);
    trans_rollback_stmt(thd);
    close_mysql_tables(thd);
    DBUG_RETURN(true);
  }

  int error;
  table->use_all_columns();

  /* set the field that's the PK to the value we're looking for */
  table->field[SERVERS_FIELD_NAME]->store(
    m_server_options->m_server_name.str,
    m_server_options->m_server_name.length,
    system_charset_info);

  error= table->file->ha_index_read_idx_map(
    table->record[0], 0,
    table->field[SERVERS_FIELD_NAME]->ptr,
    ~(longlong)0,
    HA_READ_KEY_EXACT);
  if (error)
  {
    if (error != HA_ERR_KEY_NOT_FOUND && error != HA_ERR_END_OF_FILE)
      table->file->print_error(error, MYF(0));
    else
      my_error(ER_FOREIGN_SERVER_DOESNT_EXIST, MYF(0),
               m_server_options->m_server_name.str);
  }
  else
  {
    /* ok, so we can update since the record exists in the table */
    store_record(table, record[1]);
    m_server_options->store_altered_server(table, existing);
    if ((error=table->file->ha_update_row(table->record[1],
                                          table->record[0])) &&
        error != HA_ERR_RECORD_IS_THE_SAME)
      table->file->print_error(error, MYF(0));
    else
    {
      // Update cache entry
      if ((error= m_server_options->update_cache(existing)))
        my_error(ER_OUT_OF_RESOURCES, MYF(0));
    }
  }

  /* Perform a reload so we don't have a 'hole' in our mem_root */
  servers_load(thd, table);

  // NOTE: servers_load() must be called under acquired THR_LOCK_servers.
  mysql_rwlock_unlock(&THR_LOCK_servers);

  if (error)
    trans_rollback_stmt(thd);
  else
    trans_commit_stmt(thd);
  close_mysql_tables(thd);

  if (close_cached_connection_tables(thd, m_server_options->m_server_name.str,
                                     m_server_options->m_server_name.length))
  {
    push_warning_printf(thd, Sql_condition::SL_WARNING,
                        ER_UNKNOWN_ERROR, "Server connection in use");
  }

  if (error == 0 && !thd->killed && !thd->no_send)
    my_ok(thd, 1);
  DBUG_RETURN(error != 0 || thd->killed);
}


bool Sql_cmd_drop_server::execute(THD *thd)
{
  DBUG_ENTER("Sql_cmd_drop_server::execute");

  if (Sql_cmd_common_server::check_and_open_table(thd))
    DBUG_RETURN(true);

  int error;
  mysql_rwlock_wrlock(&THR_LOCK_servers);
  table->use_all_columns();

  /* set the field that's the PK to the value we're looking for */
  table->field[SERVERS_FIELD_NAME]->store(m_server_name.str,
                                          m_server_name.length,
                                          system_charset_info);

  error= table->file->ha_index_read_idx_map(
    table->record[0], 0,
    table->field[SERVERS_FIELD_NAME]->ptr,
    HA_WHOLE_KEY, HA_READ_KEY_EXACT);
  if (error)
  {
    if (error != HA_ERR_KEY_NOT_FOUND && error != HA_ERR_END_OF_FILE)
      table->file->print_error(error, MYF(0));
    else if (!m_if_exists)
      my_error(ER_FOREIGN_SERVER_DOESNT_EXIST, MYF(0), m_server_name.str);
    else
      error= 0; // Reset error - we will report my_ok() in this case.
  }
  else
  {
    // Delete from table
    if ((error= table->file->ha_delete_row(table->record[0])))
      table->file->print_error(error, MYF(0));
    else
    {
      // Remove from cache
      FOREIGN_SERVER *server=
        (FOREIGN_SERVER *)my_hash_search(&servers_cache,
                                         (uchar*) m_server_name.str,
                                         m_server_name.length);
      if (server)
      {
        my_hash_delete(&servers_cache, (uchar*)server);
        /*maintain for drop server*/
        global_modify_server_version++;
        if (!native_strncasecmp(m_server_name.str, tdbctl_control_wrapper_prefix, strlen(tdbctl_control_wrapper_prefix)))
        {
          modify_tdbctl_flag = true;
        }
        /* add to_delete_servername_list for TDBCTL DROP NODE command, which will
        traverse the list later and delete SPIDER node's server_name also.
        NB: We should have call delete_redundant_routings here, but it acquire
        THR_LOCK_servers lock also(this function had acquired ), so we have to call
        delete_redundant_routings after THR_LOCK_servers unlock.
        In concurrence DROP SERVER situation, the to_delete_servername_list
        may incorrect if we use outside of THR_LOCK_servers lock. In fact, no need to
        worry, because follow:
        Before we add this logic, DROP SERVER not care about spider's routing,
        so the incorrect not affect DROP SERVER command. For TDBCTL DROP NODE command,
        we had acquire a MDL_EXCLUSIVE lock by lock_statement_by_name function to block other
        concurrence DROP NODE, so acceptable at present
        */
        // to_delete_servername_list.clear();
        // to_delete_servername_list.push_back(server->server_name);
      }
      else if (!m_if_exists)
      {
        my_error(ER_FOREIGN_SERVER_DOESNT_EXIST, MYF(0),  m_server_name.str);
        error= 1;
      }
    }
  }

  mysql_rwlock_unlock(&THR_LOCK_servers);

  if (modify_tdbctl_flag)
  {
    /*
      tc_is_primary_tdbctl_node need to get THR_LOCK_servers,
      so must maintain tdbctl_is_primary after unlock
    */
    // todo: I'm not sure if it is necessary to maintain the tdbctl_is_primary here
    tdbctl_is_primary = tc_is_primary_tdbctl_node();
    modify_tdbctl_flag = false;
  }
  if (error)
    trans_rollback_stmt(thd);
  else
    trans_commit_stmt(thd);
  close_mysql_tables(thd);

  /* after delete server, should transfer to spider also */
//  error = delete_redundant_routings();
  if (close_cached_connection_tables(thd, m_server_name.str,
                                     m_server_name.length))
  {
    push_warning_printf(thd, Sql_condition::SL_WARNING,
                        ER_UNKNOWN_ERROR, "Server connection in use");
  }

  if (error == 0 && !thd->killed && !thd->no_send)
    my_ok(thd, 1);
  DBUG_RETURN(error != 0 || thd->killed);
}


void servers_free(bool end)
{
  DBUG_ENTER("servers_free");
  if (!my_hash_inited(&servers_cache))
    DBUG_VOID_RETURN;
  if (!end)
  {
    free_root(&mem, MYF(MY_MARK_BLOCKS_FREE));
	my_hash_reset(&servers_cache);
    DBUG_VOID_RETURN;
  }
  mysql_rwlock_destroy(&THR_LOCK_servers);
  free_root(&mem,MYF(0));
  my_hash_free(&servers_cache);
  DBUG_VOID_RETURN;
}


/*
  SYNOPSIS

  clone_server(MEM_ROOT *mem_root, FOREIGN_SERVER *orig, FOREIGN_SERVER *buff)

  Create a clone of FOREIGN_SERVER. If the supplied mem_root is of
  thd->mem_root then the copy is automatically disposed at end of statement.

  NOTES

  ARGS
   MEM_ROOT pointer (strings are copied into this mem root) 
   FOREIGN_SERVER pointer (made a copy of)
   FOREIGN_SERVER buffer (if not-NULL, this pointer is returned)

  RETURN VALUE
   FOREIGN_SEVER pointer (copy of one supplied FOREIGN_SERVER)
*/

static FOREIGN_SERVER *clone_server(MEM_ROOT *mem, const FOREIGN_SERVER *server,
                                    FOREIGN_SERVER *buffer)
{
  DBUG_ENTER("sql_server.cc:clone_server");

  if (!buffer)
    buffer= new (mem) FOREIGN_SERVER();

  buffer->server_name= strmake_root(mem, server->server_name,
                                    server->server_name_length);
  buffer->port= server->port;
  buffer->version = server->version;
  buffer->server_name_length= server->server_name_length;
  
  /* TODO: We need to examine which of these can really be NULL */
  buffer->db= server->db ? strdup_root(mem, server->db) : NULL;
  buffer->scheme= server->scheme ? strdup_root(mem, server->scheme) : NULL;
  buffer->username= server->username? strdup_root(mem, server->username): NULL;
  buffer->password= server->password? strdup_root(mem, server->password): NULL;
  buffer->sport = server->sport ? strdup_root(mem, server->sport) : NULL;
  buffer->socket= server->socket ? strdup_root(mem, server->socket) : NULL;
  buffer->owner= server->owner ? strdup_root(mem, server->owner) : NULL;
  buffer->host= server->host ? strdup_root(mem, server->host) : NULL;

  DBUG_RETURN(buffer);
}


FOREIGN_SERVER *get_server_by_name(MEM_ROOT *mem, const char *server_name,
                                   FOREIGN_SERVER *buff)
{
  size_t server_name_length;
  FOREIGN_SERVER *server;
  DBUG_ENTER("get_server_by_name");
  DBUG_PRINT("info", ("server_name %s", server_name));

  if (! server_name || !strlen(server_name))
  {
    DBUG_PRINT("info", ("server_name not defined!"));
    DBUG_RETURN((FOREIGN_SERVER *)NULL);
  }

  server_name_length = strlen(server_name);

  DBUG_PRINT("info", ("locking servers_cache"));
  mysql_rwlock_rdlock(&THR_LOCK_servers);
  if (!(server= (FOREIGN_SERVER *) my_hash_search(&servers_cache,
                                                  (uchar*) server_name,
                                                  server_name_length)))
  {
    DBUG_PRINT("info", ("server_name %s length %u not found!",
                        server_name, (unsigned) server_name_length));
    server= (FOREIGN_SERVER *) NULL;
  }
  /* otherwise, make copy of server */
  else
    server= clone_server(mem, server, buff);

  DBUG_PRINT("info", ("unlocking servers_cache"));
  mysql_rwlock_unlock(&THR_LOCK_servers);
  DBUG_RETURN(server);
}

ulong get_servers_count()
{
  mysql_rwlock_rdlock(&THR_LOCK_servers);
  ulong records = servers_cache.records;
  mysql_rwlock_unlock(&THR_LOCK_servers);
  return records;
}

/*
  @param
  with_slave: if true, server_list include SPIDER_SLAVE
*/
ulong get_servers_count_by_wrapper(const char* wrapper_name, bool with_slave)
{
  ulong ret = 0;
  FOREIGN_SERVER* server = NULL;
  string wrapper_slave = wrapper_name;
  if (with_slave)
    wrapper_slave += "_SLAVE";

  mysql_rwlock_rdlock(&THR_LOCK_servers);
  ulong records = servers_cache.records;
  if(records == 0)
    goto finish;
  for (ulong i = 0; i < records; i++)
  {
    if (!(server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i)))
      server = (FOREIGN_SERVER*)NULL;
    else
    {
      if (!strcasecmp(server->scheme, wrapper_name) ||
          !strcasecmp(server->scheme, wrapper_slave.c_str()))
        ++ret;
    }
  }

finish:
  mysql_rwlock_unlock(&THR_LOCK_servers);
  return ret;
}

bool server_compare(FOREIGN_SERVER*& first, FOREIGN_SERVER*& second)
{
  int ret = strcmp(first->server_name, second->server_name);
  if (ret < 0)
    return TRUE;
  else
    return FALSE;
}

/*
if wraper_name is NULL_WRAPPER, return all servers
*/
void get_server_by_wrapper(
  list<FOREIGN_SERVER*>& server_list, 
  MEM_ROOT* mem, 
  const char* wrapper_name, 
  bool with_slave
)
{
  ulong records = 0;
  FOREIGN_SERVER* server;
  string wrapper_slave = wrapper_name;
  mysql_rwlock_rdlock(&THR_LOCK_servers);
  records = servers_cache.records;
  if (with_slave)
    wrapper_slave +=  "_SLAVE";

  for (ulong i = 0; i < records; i++)
  {
    if (!(server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i)))
      server = (FOREIGN_SERVER*)NULL;
    else
    {
      if (!strcasecmp(wrapper_name, NULL_WRAPPER) ||
          !strcasecmp(server->scheme, wrapper_name) ||
          !strcasecmp(server->scheme, wrapper_slave.c_str()))
      {
        server = clone_server(mem, server, NULL);
        server_list.push_back(server);
      }
    }
  }
  mysql_rwlock_unlock(&THR_LOCK_servers);
  server_list.sort(server_compare);
}

/*
  generate a new unique server_name by wrapper
  server_name = wrapper_name + increase id

  @wrapper_name TDBCTL|SPIDER_SLAVE|SPIDER|mysql|mysql_slave

  @retval return an unique server_name which
   not exist in mysql.servers
   TODO: generate continuous number, gap maybe exist after
   frequent ADD/DROP node
*/
string get_new_server_name_by_wrapper(
    const char* wrapper_name
)
{
  ostringstream server_name;
  ulong records = 0;
  ulong max_suffix_num = 0;
  //whether wrapper is spider or spider_slave
  // bool is_spider = false;

  server_name.str("");
  server_name << get_wrapper_prefix_by_wrapper(wrapper_name);
  // if (strcasecmp(server_name.str().c_str(), tdbctl_spider_wrapper_prefix) == 0 || 
  // strcasecmp(server_name.str().c_str(), tdbctl_spider_slave_wrapper_prefix) == 0)
  //   is_spider = true;

  mysql_rwlock_rdlock(&THR_LOCK_servers);
  records = servers_cache.records;

  for (ulong i = 0; i < records; i++)
  {
    if (auto server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i))
    {
      //for spider, total SPIDER and SPIDER_SLAVE's server_name must be unique
      if (!strcasecmp(server->scheme, wrapper_name) /* ||
          ((is_spider && !strcasecmp(server->scheme, SPIDER_WRAPPER)|| !strcasecmp(server->scheme, SPIDER_SLAVE_WRAPPER))) */)
      {
        ulong suffix_num = 0;
        string prefix = server->server_name;
        regex pattern(server_name.str().c_str(), regex::icase);
        prefix = regex_replace(prefix, pattern, "");
        suffix_num = std::atol(prefix.c_str());
        if (max_suffix_num <= suffix_num)
          max_suffix_num = suffix_num + 1;
      }
    }
  }
  mysql_rwlock_unlock(&THR_LOCK_servers);

  server_name << max_suffix_num;

  return server_name.str();
}

std::string get_new_server_name_by_number(
    const char* wrapper_name,
    const long number
)
{
  std::ostringstream server_name;
  server_name.str("");
  server_name << get_wrapper_prefix_by_wrapper(wrapper_name);
  server_name << number;
  return server_name.str();
}

int get_node_type_by_wrapper(
  const char* wrapper_name
)
{
  int node_type;
  if (strcasecmp(wrapper_name, SPIDER_WRAPPER) == 0)
    return node_type = NODE_TYPE_SPIDER;
  else if (strcasecmp(wrapper_name, SPIDER_SLAVE_WRAPPER) == 0)
    return node_type = NODE_TYPE_SPIDER_SLAVE;
  else if (strcasecmp(wrapper_name, MYSQL_WRAPPER) == 0)
    return node_type = NODE_TYPE_REMOTE;
  else if (strcasecmp(wrapper_name, MYSQL_SLAVE_WRAPPER) == 0)
    return node_type = NODE_TYPE_REMOTE_SLAVE;
  else if (strcasecmp(wrapper_name, TDBCTL_WRAPPER) == 0)
    return node_type = NODE_TYPE_CTL;
  return -1;
}

const char *get_wrapper_prefix_by_wrapper(
    const char *wrapper_name
)
{
  if (strcasecmp(wrapper_name, SPIDER_WRAPPER) == 0)
    return tdbctl_spider_wrapper_prefix;
  else if (strcasecmp(wrapper_name, SPIDER_SLAVE_WRAPPER) == 0)
    return tdbctl_spider_slave_wrapper_prefix;
  else if (strcasecmp(wrapper_name, TDBCTL_WRAPPER) == 0)
    return tdbctl_control_wrapper_prefix;
  else if (strcasecmp(wrapper_name, MYSQL_WRAPPER) == 0)
    return tdbctl_mysql_wrapper_prefix;
  else if (strcasecmp(wrapper_name, MYSQL_SLAVE_WRAPPER) == 0)
    return tdbctl_mysql_slave_wrapper_prefix;
  else
    return wrapper_name;
}

//This is only used for mysql_slave node now
void trim_server_name_slave_suffix(std::string &server_name)
{
  std::string subset = "_SLAVE";
  size_t pos = server_name.find(subset);
  if (pos != std::string::npos)
    server_name.replace(pos, subset.size(), "");
}

//This is only used for mysql_slave node now
void trim_wrapper_name_slave_suffix(std::string &wrapper_name)
{
  if (strcasecmp(wrapper_name.c_str(), SPIDER_SLAVE_WRAPPER) == 0)
  {
    std::string subset = "_SLAVE";
    size_t pos = wrapper_name.find(subset);
    if (pos != std::string::npos)
      wrapper_name.replace(pos, subset.size(), "");
  }
  else if (strcasecmp(wrapper_name.c_str(), MYSQL_SLAVE_WRAPPER) == 0)
  {
    std::string subset = "_slave";
    size_t pos = wrapper_name.find(subset);
    if (pos != std::string::npos)
      wrapper_name.replace(pos, subset.size(), "");
  }
}

/*
  get server info from mysql.servers and generate SQL statement for 
  flushing routing sql to the mysql.servers of other tdbctl nodes.
  Do not encapsulate replace tdbctl routing sql with begin...commit, because the slave_sql_thread 
  will commit implicitly and produe gtid.

  tdbctl routing info: all spider,spider_slave,remote,remote_slave and tdbctl nodes
*/
static std::string generate_routing_sql_for_tdbctl()
{
  ulong records = 0;
  FOREIGN_SERVER* server;
  mysql_rwlock_rdlock(&THR_LOCK_servers);
  records = servers_cache.records;
  std::string replace_sql_all = "replace into mysql.servers"
    "(Server_name, Host, Db, Username, Password, Port, Socket, Wrapper, Owner)  values";
  std::stringstream ss;
  std::string comma = ",";

  if (records == 0)
  {
    sql_print_warning("no records found in mysql.servers, null sql returned");
    mysql_rwlock_unlock(&THR_LOCK_servers);
    return "";
  }

  /*
    flush the mysql.server info to other tdbctl nodes
  */
  ss.str("");
  //ss << "delete from mysql.servers where Wrapper='";
  //ss << TDBCTL_WRAPPER;
  //ss << "';";
  ss << "delete from mysql.servers;";
  replace_sql_all.insert(0, ss.str());

  for (ulong i = 0; i < records; i++)
  {
    server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i);
    if (server)
    {
      std::string replace_sql_cur = "(";
      std::string server_name = server->server_name;
      std::string wrapper_name = server->scheme;
      
      std::string name = TC_STR_DOUBLE_QUOTED(server_name) + comma;
      std::string host = TC_STR_DOUBLE_QUOTED(std::string(server->host)) + comma;
      std::string db = TC_STR_DOUBLE_QUOTED(std::string(server->db)) + comma;
      std::string username = TC_STR_DOUBLE_QUOTED(std::string(server->username)) + comma;
      std::string password = TC_STR_DOUBLE_QUOTED(std::string(server->password)) + comma;
      int port = server->port;
      std::string socket = TC_STR_DOUBLE_QUOTED(std::string(server->socket)) + comma;
      std::string wrapper = TC_STR_DOUBLE_QUOTED(std::string(wrapper_name)) + comma;
      std::string owner = TC_STR_DOUBLE_QUOTED(std::string(server->owner));
      ss.str("");
      ss << port;
      std::string port_s = ss.str() + comma;
      replace_sql_cur = replace_sql_cur + name + host + db + username
        + password + port_s + socket + wrapper + owner;
      replace_sql_cur += "),";
      replace_sql_all += replace_sql_cur;
    }
  }

  replace_sql_all.erase(replace_sql_all.end() - 1);
  mysql_rwlock_unlock(&THR_LOCK_servers);
  return replace_sql_all;
}

/*
  get server info from mysql.servers and generate SQL statement
  for flushing routing sql to spider/spider_slave.
  If is_slave_routing is false, we will generate routing sql for spider.
  Otherwise, we will generate routing sql for spider_slave;

  @Note
  spider routing info: all spider nodes, all remote nodes, tdbctl_primary
  spider_slave routing info : all spider_slave nodes, all remote_slave nodes, tdbctl_primary 

  How to choose the tdbctl_primary:
  MGR scenario:
  Multi-Primary: use local node
  Single-Primary: use Primary node
  None-MGR scenario: use local node
*/
static string generate_routing_sql_for_spider(bool is_slave_routing)
{
  ulong records = 0;
  FOREIGN_SERVER* server;
  std::map<std::string, std::pair<std::string, std::string>> tdbctl_sql_map;
  mysql_rwlock_rdlock(&THR_LOCK_servers);
  records = servers_cache.records;
  string replace_sql_all = "replace into mysql.servers"
    "(Server_name, Host, Db, Username, Password, Port, Socket, Wrapper, Owner)  values";
  stringstream ss;
  string comma = ",";
  std::string begin_sql = "begin;";
  std::string commit_sql = "commit;";
  std::string flush_table_sql = "flush tables with no block;";

  if (records == 0)
  {
    sql_print_warning("no records found in mysql.servers, null sql returned");
    mysql_rwlock_unlock(&THR_LOCK_servers);
    return "";
  }

  /*
    Construct SQL to delete redundant nodes
  */
  string delete_sql_all = "delete from mysql.servers where Server_name not in (";

  for (ulong i = 0; i < records; i++)
  {
    server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i);
    if (server)
    {
      // flush routing to slave_spider, skip master_spider and master_mysql
      if (is_slave_routing && (!strcasecmp(server->scheme, SPIDER_WRAPPER) || !strcasecmp(server->scheme, MYSQL_WRAPPER)))
        continue;
      // flush routing to master_spider, skip slave_spider and slave_mysql
      if (!is_slave_routing && (!strcasecmp(server->scheme, SPIDER_SLAVE_WRAPPER) || !strcasecmp(server->scheme, MYSQL_SLAVE_WRAPPER)))
        continue;
      //sql_print_information("slave %d, server_name %s", is_slave_routing, server->server_name);
      std::string replace_sql_cur = "(";
      std::string server_name;
      std::string wrapper_name;
      if (!strcasecmp(server->scheme, MYSQL_SLAVE_WRAPPER))
      {
        // convert server_name: SPT_SLAVEn -> SPTn
        // convert wrapper: mysql_slave -> mysql
        server_name = server->server_name;
        wrapper_name = server->scheme;
        trim_server_name_slave_suffix(server_name);
        trim_wrapper_name_slave_suffix(wrapper_name);
      }
      else
      {
        server_name = server->server_name;
        wrapper_name = server->scheme;
      }
      
      std::string name = TC_STR_DOUBLE_QUOTED(server_name) + comma;
      std::string host = TC_STR_DOUBLE_QUOTED(std::string(server->host)) + comma;
      std::string db = TC_STR_DOUBLE_QUOTED(std::string(server->db)) + comma;
      std::string username = TC_STR_DOUBLE_QUOTED(std::string(server->username)) + comma;
      std::string password = TC_STR_DOUBLE_QUOTED(std::string(server->password)) + comma;
      int port = server->port;
      std::string socket = TC_STR_DOUBLE_QUOTED(std::string(server->socket)) + comma;
      std::string wrapper = TC_STR_DOUBLE_QUOTED(std::string(wrapper_name)) + comma;
      std::string owner = TC_STR_DOUBLE_QUOTED(std::string(server->owner));
      ss.str("");
      ss << port;
      string port_s = ss.str() + comma;
      replace_sql_cur = replace_sql_cur + name + host + db + username
        + password + port_s + socket + wrapper + owner;
      replace_sql_cur += "),";
      /* for tdbctl node, need special deal subsequent */
      if (strcasecmp(server->scheme, TDBCTL_WRAPPER) == 0)
      {
        /* NOTE: at present, ip#port must be unique for tdbctl */
        string ip_port = string(server->host) + "#" + ss.str();
        tdbctl_sql_map.insert({ip_port, {name, replace_sql_cur}});
        continue;
      }
      replace_sql_all += replace_sql_cur;
      delete_sql_all += name;
    }
  }

  if (!tdbctl_sql_map.empty())
  {
    string ip_port;
    string primary_host = "";
    uint primary_port;
    if (tc_get_primary_node(primary_host, &primary_port) != 0)
    {
      ss.str("");
      ss << primary_port;
      ip_port = primary_host + "#" + ss.str();

      if (tdbctl_sql_map.count(ip_port) == 1) {
        //add tdbctl insert sql
        replace_sql_all += tdbctl_sql_map[ip_port].second;
        delete_sql_all += tdbctl_sql_map[ip_port].first;
      }
      else
      {
        sql_print_warning("primary node not in mysql.servers, null sql returned");
        mysql_rwlock_unlock(&THR_LOCK_servers);
        return "";
      }
    }
    else
    {// unknown error, such as network partition.
      sql_print_warning("get primary node info failed, null sql returned");
      mysql_rwlock_unlock(&THR_LOCK_servers);
      return "";
    }
  } else {
    sql_print_warning("primary node not in mysql.servers, null sql returned");
    mysql_rwlock_unlock(&THR_LOCK_servers);
    return "";
  }

  replace_sql_all.pop_back();
  replace_sql_all += ";";
  delete_sql_all.pop_back();
  delete_sql_all += ");";
  std::string flush_routing_sql = begin_sql + delete_sql_all + replace_sql_all
                                  + commit_sql + flush_table_sql;
  mysql_rwlock_unlock(&THR_LOCK_servers);
  return flush_routing_sql;
}


map<string, string> get_ipport_map_from_serverlist(
  map<string, string>& user_map,
  map<string, string>& passwd_map,
  list<FOREIGN_SERVER*>& diff_server_list
)
{
  map<string, string> ipport_map;
  ostringstream  sstr;
  user_map.clear();
  passwd_map.clear();

  list<FOREIGN_SERVER*>::iterator its;
  for (its = diff_server_list.begin(); its != diff_server_list.end(); its++)
  {
    FOREIGN_SERVER* server = (*its);
    string server_name = server->server_name;
    string host = server->host;
    string user = server->username;
    string passwd = server->password;
    sstr.str("");
    sstr << server->port;
    string ports = sstr.str();
    string s = host + "#" + ports;
    ipport_map.insert(pair<string, string>(server_name, s));
    user_map.insert(pair<string, string>(s, user));
    passwd_map.insert(pair<string, string>(s, passwd));
  }
  return ipport_map;
}

int tc_set_changed_remote_read_only()
{
  int ret = 0;
  int result = 0;
  tc_exec_info exec_info;
  //string sql = "set global super_read_only=1";
  string sql = "set global read_only=1";
  list<FOREIGN_SERVER*> diff_server_list;
  ostringstream  sstr;
  map<string, MYSQL*> conn_map;
  map<string, string> remote_user_map;
  map<string, string> remote_passwd_map;
  map<string, string> remote_ipport_map;
  map<string, tc_exec_info> result_map;
  map<string, string>::iterator its;
  MEM_ROOT mem_root;

  if (!tc_set_changed_node_read_only)
    return result;
  init_sql_alloc(key_memory_servers, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
  ret = get_remote_changed_servers(&mem_root, &diff_server_list);

  if (ret == 1)
  {
    remote_ipport_map = get_ipport_map_from_serverlist(remote_user_map, remote_passwd_map, diff_server_list);
    conn_map = tc_remote_conn_connect(ret, remote_ipport_map, remote_user_map, remote_passwd_map);

    for (its = remote_ipport_map.begin(); its != remote_ipport_map.end(); its++)
    {/* init for exec result: result_map */
      string ipport = its->second;
      tc_exec_info exec_info;
      exec_info.err_code = 0;
      exec_info.row_affect = 0;
      exec_info.err_msg = "";
      result_map.insert(pair<string, tc_exec_info>(ipport, exec_info));
    }
    if (tc_exec_sql_paral(sql, conn_map, result_map, remote_user_map, remote_passwd_map, FALSE))
    {// error 
			sql_print_error("TDBCTL: failed to set remote data node read only %s, "
				"and some error happened when modify spider routing ",
				remote_ipport_map.begin()->second.c_str());
      result = 1;
    }
    else
      result = 0;
  }
  else if(ret == 2)
  {
    result = 2;
  }
  else
  {// nothing to change
    result = 0;
  }

  tc_conn_free(conn_map);
  conn_map.clear();
  diff_server_list.clear();
  remote_passwd_map.clear();
  remote_user_map.clear();
  remote_ipport_map.clear();
  free_root(&mem_root, MYF(0));
  return result;
}

static int check_all_privileges(MYSQL *mysql, const AUTH_INFO &auth) {
  char errmsg[128];
  MYSQL_ROW row;
  MYSQL_RES *res;
  const char *show_grants = "SHOW GRANTS";
  const char *found1 = NULL, *found2 = NULL;
  ulong version = mysql_get_server_version(mysql);

  if (version >= 80000 && version < 90000) {
    /*
      MySQL 8.0 does not return literal "ALL PRIVILEGES" for SHOW GRANTS. For
      now, we simply skip grant checks in this case.
    */
    return FALSE;
  }

  if (mysql_real_query(mysql, show_grants, strlen(show_grants)) ||
      !(res = mysql_store_result(mysql))) {
    snprintf(errmsg, sizeof(errmsg), "failed to show grants from %s: %s",
             auth.ipport_str.c_str(), mysql_error(mysql));
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), errmsg);
    return TRUE;
  }

  if ((row = mysql_fetch_row(res))) {
    found1 = strstr(row[0], "ALL PRIVILEGES");
    found2 = strstr(row[0], "WITH GRANT OPTION");
  }
  mysql_free_result(res);

  if (!found1 || !found2) {
    snprintf(errmsg, sizeof(errmsg),
             "ALL PRIVILEGES WITH GRANT OPTION on %s is needed for this operation",
             auth.ipport_str.c_str());
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0), errmsg);
    return TRUE;
  }

  return FALSE;
}

int tc_do_grants_internal(THD *thd, LEX *lex) {
  Cluster_conn_manager *conn_mgr;
  AUTH_INFO auth_info;
  FOREIGN_SERVER *server;
  const char *scheme;
  string create_user_sql, grant_sql;
  MYSQL *mysql; /* connection to the target node */
  const Server_options &svr_options = lex->server_options;
  DBUG_ENTER("tc_do_grants_internal");

  conn_mgr = thd->cluster_conn_manager;
  DBUG_ASSERT(conn_mgr);
  if (conn_mgr->refresh(FALSE, TRUE))
    DBUG_RETURN(TRUE);

  if (lex->sql_command == TC_SQLCOM_ALTER_NODE) {
    server = get_server_by_name(thd->mem_root,
                                lex->server_options.m_server_name.str, NULL);
    DBUG_ASSERT(server);
    scheme = server->scheme;
    /* DEPRECATED*/
    /*if (unlikely(strcasecmp(scheme, MYSQL_WRAPPER) &&
                 strcasecmp(scheme, MYSQL_SLAVE_WRAPPER))) {
      DBUG_ASSERT(0);
      DBUG_RETURN(TRUE);
    }*/
  } else {
    scheme = svr_options.get_scheme();
  }
  fill_auth_info(&auth_info, svr_options.get_host(), svr_options.get_port(),
                 svr_options.get_username(), svr_options.get_password(),
                 scheme);
  if (!(mysql = tc_conn_connect(auth_info))) {
    my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), auth_info.ipport_str.c_str());
    DBUG_RETURN(TRUE);
  }
  MYSQL_GUARD(mysql);

  /* Check we have ALL PRIVILEGES WITH GRANT OPTION on the new server */
  if (check_all_privileges(mysql, auth_info))
    DBUG_RETURN(TRUE);

  if (!strcasecmp(scheme, MYSQL_WRAPPER) ||
      !strcasecmp(scheme, MYSQL_SLAVE_WRAPPER)) {
    /* Target Node Type: REMOTE */
    /*
      1. New Remote ==grant==> All Spiders
      This allows all Spiders to access data on the new remote node.
    */
    if (tc_grant_single_to_multi(thd, mysql, auth_info, NODE_TYPE_SPIDER))
      DBUG_RETURN(TRUE);

    /*
      2. New Remote ==grant==> All Tdbctls
      This allows all Tdbctl nodes to operate on the new remote node.
    */
    if (tc_grant_single_to_multi(thd, mysql, auth_info, NODE_TYPE_CTL))
      DBUG_RETURN(TRUE);

    /*
      3. New Remote(slave) ==grant==> All Spiders(slave)
    */
    if (!strcasecmp(scheme, MYSQL_SLAVE_WRAPPER) &&
        tc_grant_single_to_multi(thd, mysql, auth_info, NODE_TYPE_SPIDER_SLAVE))
      DBUG_RETURN(TRUE);
  } else if (!strcasecmp(scheme, SPIDER_WRAPPER) ||
             !strcasecmp(scheme, SPIDER_SLAVE_WRAPPER)) {
    /* Target Node Type: SPIDER */
    /*
      1. New Spider ==grant==> All Tdbctls
      This allows all Tdbctl nodes to operate on the new Spider node.
    */
    if (tc_grant_single_to_multi(thd, mysql, auth_info, NODE_TYPE_CTL))
      DBUG_RETURN(TRUE);

    /*
      2. All Remotes(master & slave) ==grant==> New Spider
      This allows the new Spider to access data on remote nodes.
    */
    if (tc_grant_multi_to_single(thd, auth_info, NODE_TYPE_REMOTE) ||
        tc_grant_multi_to_single(thd, auth_info, NODE_TYPE_REMOTE_SLAVE))
      DBUG_RETURN(TRUE);

    /*
      3. All Tdbctls ==grant==> New Spider
      This allows the new Spider to run DDLs on a cluster level (with
      @@ddl_execute_by_ctl=ON).
    */
    if (tc_grant_multi_to_single(thd, auth_info, NODE_TYPE_CTL))
      DBUG_RETURN(TRUE);
  } else if (!strcasecmp(scheme, TDBCTL_WRAPPER)) {
    /* Target Node Type: TDBCTL */
    /*
      1. New Tdbctl ==grant==> All Tdbctls
      This allows existing Tdbctl nodes to operate on the new Tdbctl node.
    */
    if (tc_grant_single_to_multi(thd, mysql, auth_info, NODE_TYPE_CTL))
      DBUG_RETURN(TRUE);

    /*
      2. All Tdbctls ==grant==> New Tdbctl
      This allows the new Tdbctl to operate on all existing Tdbctl nodes.
    */
    if (tc_grant_multi_to_single(thd, auth_info, NODE_TYPE_CTL))
      DBUG_RETURN(TRUE);

    /*
      3. All Spiders(master & slave) ==grant==> New Tdbctl
      This allows the new Tdbctl to operate on all Spiders (usually when its
      Primary Mode is enabled)
    */
    if (tc_grant_multi_to_single(thd, auth_info, NODE_TYPE_SPIDER) ||
        tc_grant_multi_to_single(thd, auth_info, NODE_TYPE_SPIDER_SLAVE))
      DBUG_RETURN(TRUE);

    /*
      4. New Tdbctl ==grant==> All Spiders(master & slave)
      This allows all Spiders to run DDLs on the new Tdbctl on a cluster level
      (with @@ddl_execute_by_ctl=ON).
    */
    if (tc_grant_single_to_multi(thd, mysql, auth_info, NODE_TYPE_SPIDER) ||
        tc_grant_single_to_multi(thd, mysql, auth_info, NODE_TYPE_SPIDER_SLAVE))
      DBUG_RETURN(TRUE);

    /*
      5. All Remotes(master & slave) ==grant==> New Tdbctl
      This allows the new Tdbctl to operate on the remote nodes.
    */
    if (tc_grant_multi_to_single(thd, auth_info, NODE_TYPE_REMOTE) ||
        tc_grant_multi_to_single(thd, auth_info, NODE_TYPE_REMOTE_SLAVE))
      DBUG_RETURN(TRUE);
  } else {
    /* unreachable */
    DBUG_ASSERT(0);
  }

  DBUG_RETURN(FALSE);
}

// Generate routing sql by wrapper name
// And then send these sql to spider/spider_slave/tdbctl nodes
enum FLUSH_ROUTING_RESULT tc_flush_routing_by_wrapper(map<string, tc_exec_info> &result_map, 
                                                      map<string, MYSQL*>conn_map, 
                                                      const char* wrapper, 
                                                      bool is_force, 
                                                      bool is_flush_only_cache)
{
  std::string flush_priv_sql = "flush privileges";
  std::string flush_table_sql = "flush tables with no block";
  std::string flush_rdlock_sql = "flush table with read lock";
  std::string set_mdl_timeout_sql = "set lock_wait_timeout = 60";
  std::string set_interactive_timeout_sql = "set wait_timeout = 180";
  std::string set_option_sql = set_mdl_timeout_sql + ";" + set_interactive_timeout_sql;
  if (!strcasecmp(wrapper, TDBCTL_WRAPPER))
  {
    // set sql_log_bin = off is to avoid producing gtid
    std::string set_sql_log_bin_sql = "set tc_admin=0;set sql_log_bin = off;";
    set_option_sql = set_sql_log_bin_sql + set_option_sql;
    is_force = true;
  }
  std::string unlock_sql = "unlock tables";
  std::string replace_sql;
  if (!strcasecmp(wrapper, SPIDER_WRAPPER))
    replace_sql = generate_routing_sql_for_spider(false);
  else if (!strcasecmp(wrapper, SPIDER_SLAVE_WRAPPER))
    replace_sql = generate_routing_sql_for_spider(true);
  else if (!strcasecmp(wrapper, TDBCTL_WRAPPER))
    replace_sql = generate_routing_sql_for_tdbctl();
  else
  {
    return FLUSH_ROUTING_RESULT::UNEXPECTED_WRAPPER;
  }

  if (!is_flush_only_cache && (replace_sql.length() == 0))  //empty replace sql
  {
    if (current_thd)
      push_warning(current_thd, Sql_condition::SL_WARNING, ER_TCADMIN_FLUSH_ROUTING_ERROR,
                  "routing sql is null, flush do nothing");
    return FLUSH_ROUTING_RESULT::SUCCESS;
  }

  /* SET OPTION */
  if (tc_exec_sql_paral(set_option_sql, conn_map, result_map))
  {
    return FLUSH_ROUTING_RESULT::SET_OPTION_FAILURE;
  }

  /* Modify the mysql.servers table, but the routing information does not take effect. */
  if (!is_flush_only_cache) {  // If the CACHE option is specified, skip this step. 
    if (tc_exec_sql_paral(replace_sql, conn_map, result_map))
    {
      /* if failed to replace mysql.servers; set changed data node read only */
      // tc_set_changed_remote_read_only();
      return FLUSH_ROUTING_RESULT::SYNC_SERVERS_FAILURE;
    }
  }

  /* FLUSH TABLE WITH READ LOCK */
  if (!is_force)  // If the FORCE option is specified, skip this step.
  {
    if (tc_exec_sql_paral(flush_table_sql, conn_map, result_map) ||
      tc_exec_sql_paral(flush_rdlock_sql, conn_map, result_map))
    {/* unlock tables;*/
      map<string, tc_exec_info> new_result_map = result_map_like(result_map);
      tc_exec_sql_paral(unlock_sql, conn_map, new_result_map);
      merge_error_info(result_map, new_result_map);
      return FLUSH_ROUTING_RESULT::FLUSH_TABLE_FAILURE;
    }
  }

  /* FLUSH PRIVILEGES */
  if (tc_exec_sql_paral(flush_priv_sql, conn_map, result_map))
  {
      return FLUSH_ROUTING_RESULT::FLUSH_PRIV_FAILURE;
  }

  /* UNLOCK TABLES */
  if (!is_force)
  {
    tc_exec_sql_paral(unlock_sql, conn_map, result_map);
  }

  return FLUSH_ROUTING_RESULT::SUCCESS;
}

// Generate routing sql by wrapper name
// Just modify the mysql.servers table
enum FLUSH_ROUTING_RESULT tc_sync_servers_table_by_wrapper(map<string, tc_exec_info> &result_map, 
                                                            map<string, MYSQL*>conn_map, 
                                                            const char* wrapper)
{
  std::string set_mdl_timeout_sql = "set lock_wait_timeout = 60";
  std::string set_interactive_timeout_sql = "set wait_timeout = 180";
  std::string set_option_sql = set_mdl_timeout_sql + ";" + set_interactive_timeout_sql;
  if (!strcasecmp(wrapper, TDBCTL_WRAPPER))
  {
    // set sql_log_bin = off is to avoid producing gtid
    std::string set_sql_log_bin_sql = "set tc_admin=0;set sql_log_bin = off;";
    set_option_sql = set_sql_log_bin_sql + set_option_sql;
  }
  std::string replace_sql;
  if (!strcasecmp(wrapper, SPIDER_WRAPPER))
    replace_sql = generate_routing_sql_for_spider(false);
  else if (!strcasecmp(wrapper, SPIDER_SLAVE_WRAPPER))
    replace_sql = generate_routing_sql_for_spider(true);
  else if (!strcasecmp(wrapper, TDBCTL_WRAPPER))
    replace_sql = generate_routing_sql_for_tdbctl();
  else
  {
    return FLUSH_ROUTING_RESULT::UNEXPECTED_WRAPPER;
  }

  if (replace_sql.length() == 0)  //empty replace sql
  {
    if (current_thd)
      push_warning(current_thd, Sql_condition::SL_WARNING, ER_TCADMIN_FLUSH_ROUTING_ERROR,
                  "routing sql is null, flush do nothing");
    return FLUSH_ROUTING_RESULT::SUCCESS;
  }

  if (tc_exec_sql_paral(set_option_sql, conn_map, result_map))
  {
    return FLUSH_ROUTING_RESULT::SET_OPTION_FAILURE;
  }

  if (tc_exec_sql_paral(replace_sql, conn_map, result_map))
  {
    return FLUSH_ROUTING_RESULT::SYNC_SERVERS_FAILURE;
  }

  return FLUSH_ROUTING_RESULT::SUCCESS;
}

string tc_get_ipport_from_server_by_wrapper(Server_options* server_options, const char* wrapper_name)
{
  FOREIGN_SERVER* server;
  ostringstream  sstr;
  string ipport = "";

  mysql_rwlock_rdlock(&THR_LOCK_servers);
  if ((server = (FOREIGN_SERVER*)my_hash_search(&servers_cache,
    (uchar*)server_options->m_server_name.str,
    server_options->m_server_name.length)) 
	&& !strcasecmp(server->scheme, wrapper_name))
  {
    string host = server->host;
    string user = server->username;
    string passwd = server->password;
    sstr.str("");
    sstr << server->port;
    string ports = sstr.str();
    ipport = host + "#" + ports;
  }
  mysql_rwlock_unlock(&THR_LOCK_servers);
  return ipport;
}


bool tc_check_ipport_valid()
{
  return FALSE;

}

bool tc_flush_routing(LEX* lex, Cluster_conn_manager* conn_mgr)
{
  std::set<std::string> spider_nodes;
  std::set<std::string> spider_slave_nodes;
  std::set<std::string> tdbctl_nodes;
  bool result = FALSE;
  MEM_ROOT mem_root;
  init_sql_alloc(key_memory_servers , &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
  int ret = 0;
  std::string tdbctl_server_name;
  tdbctl_server_name = tc_get_server_name(ret, &mem_root, TDBCTL_WRAPPER, true);
  if (ret)
  {
    //tdbctl node must exists in mysql.servers
    //TODO:need judge this tdbctl node is primary(current tdbctl)
    my_error(ER_TCADMIN_FLUSH_ROUTING_ERROR, MYF(0), "no tdbctl node found");
    result = TRUE;
    return result;
  }

  switch (lex->tc_flush_type)
  {
  case FLUSH_ALL_ROUTING:
  case SYNC_ROUTING_FOR_ALTER_NODE:
  {
    get_server_name_set(&mem_root, spider_nodes, SPIDER_WRAPPER);
    get_server_name_set(&mem_root, spider_slave_nodes, SPIDER_SLAVE_WRAPPER);
    get_server_name_set(&mem_root, tdbctl_nodes, TDBCTL_WRAPPER);
    // remove the tdbctl_server_name of local node
    tdbctl_nodes.erase(tdbctl_server_name);

    if (tc_flush_routing_to_nodes(lex, spider_nodes, conn_mgr, SPIDER_WRAPPER) ||
        tc_flush_routing_to_nodes(lex, spider_slave_nodes, conn_mgr, SPIDER_SLAVE_WRAPPER) ||
        tc_flush_routing_to_nodes(lex, tdbctl_nodes, conn_mgr, TDBCTL_WRAPPER))
    {
      result = TRUE;
      break;
    }

    break;
  }
  case FLUSH_ROUTING_BY_SERVER:
  {
    std::string server_name = std::string(lex->server_options.m_server_name.str,
                                     lex->server_options.m_server_name.length);
    std::set<std::string> nodes;
    FOREIGN_SERVER *server =
            get_server_by_name(&mem_root, lex->server_options.m_server_name.str, NULL);
    if (!server) {
      my_error(ER_FOREIGN_SERVER_DOESNT_EXIST, MYF(0), server_name.c_str());
      result = TRUE;
      break;
    }
    nodes.insert(string(server->server_name, server->server_name_length));
    if(tc_flush_routing_to_nodes(lex, nodes, conn_mgr, server->scheme))
    {
      result = TRUE;
      break;
    }
    break;
  }
  case FLUSH_ROUTING_FOR_CREATE_NODE:
  {
    // get non-primary tdbctl nodes 
    get_server_name_set(&mem_root, tdbctl_nodes, TDBCTL_WRAPPER);
    tdbctl_nodes.erase(tdbctl_server_name);

    // get the newly added node
    std::string server_name = std::string(lex->server_options.m_server_name.str,
                                          lex->server_options.m_server_name.length);
    FOREIGN_SERVER *server =
            get_server_by_name(&mem_root, server_name.c_str(), NULL);
    if (!server) {
      my_error(ER_FOREIGN_SERVER_DOESNT_EXIST, MYF(0), server_name.c_str());
      result = TRUE;
      break;
    }
    std::set<std::string> new_added_node;
    new_added_node.insert(string(server->server_name, server->server_name_length));

    /*
      The TDBCTL node will definitely refresh the routing. Therefore, if the newly added node 
      is also a TDBCTL node, there is no need to refresh the routing separately.
    */
    bool skip_new_node = (strcasecmp(server->scheme, TDBCTL_WRAPPER) == 0);

    if ((!skip_new_node && tc_flush_routing_to_nodes(lex, new_added_node, conn_mgr, server->scheme)) ||
        tc_flush_routing_to_nodes(lex, tdbctl_nodes, conn_mgr, TDBCTL_WRAPPER))
    {
      result = TRUE;
      break;
    }
    break;
  }
  default:
    break;
  }
  free_root(&mem_root, MYF(0));

  return result;
}

bool tc_flush_routing_to_nodes(LEX* lex, std::set<std::string> nodes_to_be_flushed, Cluster_conn_manager* conn_mgr, const char* wrapper)
{
  bool result = FALSE;
  bool is_force = lex->tc_force;
  bool is_flush_only_cache = lex->tc_flush_only_cache;
  int retry_times = 3;
  std::set<std::string>::iterator its;
  map<std::string, tc_exec_info> result_map;

  for (its = nodes_to_be_flushed.begin(); its !=  nodes_to_be_flushed.end(); its++)
  {/* init for exec result: result_map */
    string server_name = (*its);
    tc_exec_info exec_info;
    exec_info.err_code = 0;
    exec_info.row_affect = 0;
    exec_info.err_msg = "";
    result_map.insert(pair<string, tc_exec_info>(server_name, exec_info));
  }

  std::map<std::string, MYSQL*> conn_map;
  std::map<std::string, MYSQL*> needed_conn_map;
  std::map<std::string, MYSQL*>::iterator conn_its;
  int node_type = get_node_type_by_wrapper(wrapper);
  enum FLUSH_ROUTING_RESULT exec_ret = FLUSH_ROUTING_RESULT::SUCCESS;
  while (retry_times-- > 0)
  {
    if (conn_mgr->connect((enum_node_type)node_type, false))
    {
      result = TRUE;
      goto finish;
    }

    // build the needed conn_map
    conn_map = conn_mgr->get_conn_map((enum_node_type)node_type);
    for (its = nodes_to_be_flushed.begin(); its != nodes_to_be_flushed.end(); its++)
    {
      if((conn_its = conn_map.find(*its)) != conn_map.end())
      {
        needed_conn_map.insert(std::make_pair(conn_its->first, conn_its->second));
      }
    }

    if (!nodes_to_be_flushed.empty()) {
      if (lex->tc_flush_type == SYNC_ROUTING_FOR_ALTER_NODE) {
        exec_ret = tc_sync_servers_table_by_wrapper(result_map, needed_conn_map, wrapper);
      } else {
        exec_ret = tc_flush_routing_by_wrapper(result_map, needed_conn_map, wrapper, is_force, is_flush_only_cache);
      }
    }

    if (exec_ret != FLUSH_ROUTING_RESULT::SUCCESS)
    {
      result = TRUE;
      sleep(1);  // Wait 1 second and try again

      /* FLUSH_PRIV_FAILURE means "replace mysql.servers" is ok, but "flush privileges" failed.
      Mean while, the global lock is not unlocked. So we just skip modifying mysql.servers and try again. */
      if (exec_ret == FLUSH_ROUTING_RESULT::FLUSH_PRIV_FAILURE)
        is_flush_only_cache = true;
    }
    else
    {
      result = FALSE;
      goto finish;
    }
  }
  
  if (result)
  {
    std::string nodes_success, nodes_failed; 
    if (exec_ret == FLUSH_ROUTING_RESULT::FLUSH_PRIV_FAILURE) {
      /* flush privileges failed again after retry, then we set read_only for the failed nodes. */
      std::string set_read_only_sql = "set global read_only=1";
      std::string unlock_sql = "unlock tables";
      std::map<std::string, MYSQL*> failed_conn_map;
      map<std::string, tc_exec_info> failed_result_map;
      for (const auto &result_item: result_map) {  // get failed connection.
        if (result_item.second.err_code > 0) {
          failed_conn_map[result_item.first] = needed_conn_map[result_item.first];
          /* Try to repair a connection that was disconnected due to a timeout. 
            Prepare for setting read_only. */ 
          conn_mgr->connect(result_item.first, (enum_node_type)node_type, true);
        }
      }
      tc_exec_sql_paral(set_read_only_sql, failed_conn_map, failed_result_map);  // set read_only
      for (const auto &result_item: failed_result_map) {  // describe the execution result
        if (result_item.second.err_code > 0)
          nodes_failed += result_item.first + ": " + result_item.second.err_msg + "\n";
        else
          nodes_success += result_item.first + "\n";
      }

      /* unlock tables if needed*/
      if (!is_force)
      {
        map<string, tc_exec_info> new_result_map = result_map_like(result_map);
        tc_exec_sql_paral(unlock_sql, needed_conn_map, new_result_map);
        merge_error_info(result_map, new_result_map);
      }
    }

    std::string failed_step = FLUSH_ROUTING_INFO[exec_ret];
    std::string error_info = failed_step + "\n" + concat_result_map(result_map);
    if (exec_ret == FLUSH_ROUTING_RESULT::FLUSH_PRIV_FAILURE) {
      error_info += "These nodes set read_only successfully:\n" + nodes_success;
      if (nodes_failed.size() > 0)
        error_info += "Failed to set read_only on these nodes:\n" + nodes_failed; 
    }
    error_info.pop_back();
    switch (lex->sql_command) 
    {
    case TC_SQLCOM_ALTER_NODE:
      my_error(ER_TCADMIN_ALTER_NODE_ERROR, MYF(0), ("with sync option, " + error_info).c_str());
      break;
    case TC_SQLCOM_CREATE_NODE:
      my_error(ER_TCADMIN_CREATE_NODE_ERROR, MYF(0), ("with schema option, " + error_info).c_str());
      break;
    default:
      my_error(ER_TCADMIN_FLUSH_ROUTING_ERROR, MYF(0), error_info.c_str());
    }    
  }

finish:
  nodes_to_be_flushed.clear();
  result_map.clear();
  return result;
}

bool tc_flush_routing_to_foreign_server(LEX* lex)
{
  DBUG_ENTER("tc_flush_routing_to_foreign_server");
  // Create a temporary Cluster_conn_manager instance
  std::unique_ptr<Cluster_conn_manager> conn_mgr(new Cluster_conn_manager());
  conn_mgr->skip_refresh_intentionally();

  // Prepare authentication information for the foreign server
  AUTH_INFO foreign_server_info;
  DBUG_ASSERT(lex->server_options.get_host() != NULL);
  DBUG_ASSERT(lex->server_options.get_port() != NULL);
  DBUG_ASSERT(lex->server_options.get_username() != NULL);
  DBUG_ASSERT(lex->server_options.get_password() != NULL);
  DBUG_ASSERT(lex->server_options.get_scheme() != NULL);

  std::string foreign_server_wrapper = lex->server_options.get_scheme();
  fill_auth_info(&foreign_server_info, 
                 lex->server_options.get_host(), 
                 lex->server_options.get_port(), 
                 lex->server_options.get_username(), 
                 lex->server_options.get_password(), 
                 foreign_server_wrapper);
  
  // Generate foreign server name by combining wrapper prefix and IP:port string
  std::string foreign_server_name = get_wrapper_prefix_by_wrapper(foreign_server_wrapper.c_str());
  foreign_server_name += "_foreign_" + foreign_server_info.ipport_str;
  
  // Add the foreign server to the temporary connection manager
  if(conn_mgr->add_foreign_server(foreign_server_info, foreign_server_name)) {
    DBUG_RETURN(true);
  }

  std::set<std::string> foreign_nodes{ foreign_server_name };
  // Flush routing information to the specified nodes
  if(tc_flush_routing_to_nodes(lex, foreign_nodes, conn_mgr.get(), foreign_server_wrapper.c_str())) {
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

bool compare_server_list(list<FOREIGN_SERVER*>& first, list<FOREIGN_SERVER*>& second)
{
  list<FOREIGN_SERVER*>::iterator its1;
  list<FOREIGN_SERVER*>::iterator its2;
  if (first.size() != second.size())
  {
    return TRUE;
  }

  for (its1 = first.begin(), its2 = second.begin();
    its1 != first.end(), its2 != second.end();
    its1++, its2++)
  {
    FOREIGN_SERVER* sv1 = (*its1);
    FOREIGN_SERVER* sv2 = (*its2);

    if (strcmp(sv1->server_name, sv2->server_name) ||
      strcmp(sv1->host, sv2->host) ||
      strcmp(sv1->username, sv2->username) ||
      strcmp(sv1->password, sv2->password) ||
      strcmp(sv1->db, sv2->db) ||
      strcmp(sv1->scheme, sv2->scheme) ||
      strcmp(sv1->socket, sv2->socket) ||
      strcmp(sv1->owner, sv2->owner) ||
      sv1->port != sv2->port)
    {
      return TRUE;
    }
  }
  return FALSE;
}


int compare_and_return_changed_server(
  list<FOREIGN_SERVER*>& old_list,
  list<FOREIGN_SERVER*>& new_list,
  list<FOREIGN_SERVER*>* server_list
)
{
  list<FOREIGN_SERVER*>::iterator its1;
  list<FOREIGN_SERVER*>::iterator its2;
  int ret = 0;
  if (old_list.size() != new_list.size())
  {
    ret = 2;
    return ret;
  }

  for (its1 = old_list.begin(), its2 = new_list.begin();
    its1 != old_list.end(), its2 != new_list.end();
    its1++, its2++)
  {
    FOREIGN_SERVER* sv1 = (*its1);
    FOREIGN_SERVER* sv2 = (*its2);

    if (strcmp(sv1->server_name, sv2->server_name) ||
      strcmp(sv1->host, sv2->host) ||
      strcmp(sv1->username, sv2->username) ||
      strcmp(sv1->password, sv2->password) ||
      strcmp(sv1->db, sv2->db) ||
      strcmp(sv1->scheme, sv2->scheme) ||
      strcmp(sv1->socket, sv2->socket) ||
      strcmp(sv1->owner, sv2->owner) ||
      sv1->port != sv2->port)
    {
      server_list->push_back(sv1);
      ret = 1;
    }
  }
  return ret;
}



void tc_check_and_repair_routing_thread()
{
  while (1)
  {
	  if (tc_check_repair_routing && tdbctl_is_primary)
    {
      if (tc_check_and_repair_routing())
      {
        sleep(10);
      }
      else
      {
        for (ulong i = 0; i < tc_check_repair_routing_interval; i++)
          sleep(1);
      }
    }
    sleep(2);
  }
}

/*
  get mysql.servers of TDBCTL without SLAVE-TDBCTL
  and init conn_map between TDBCTL and spider 
  which used to repair routing

  @param(out)
    tdbctl_server_map: server_map of TDBCTL
    spider_conn_map:   conn_map of spider

  @retval
    true:              ok
    false:             error
*/
bool tc_get_repair_map(MEM_ROOT* mem_root,
  map<string, FOREIGN_SERVER*>& tdbctl_server_map,
  map<string, MYSQL*>& spider_conn_map) 
{
  bool res = false;
  ulong records;
  FOREIGN_SERVER* server;
  string primary_host = "";
  uint primary_port;
  ostringstream  sstr;
  if (tc_get_primary_node(primary_host, &primary_port) == 0)
  {// unknown error, such as network partition.
    sql_print_warning("get primary node info failed");
    return true;
  }
  mysql_rwlock_rdlock(&THR_LOCK_servers);
  records = servers_cache.records;
  for (ulong i = 0; i < records; i++)
  {
    if ((server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i)))
    {
      //if this is TDBCTL
      if (!strcasecmp(server->scheme, TDBCTL_WRAPPER))
      {//if the TDBCTL is not primary, should not insert into tdbctl_server_map
        if (strcmp(server->host, primary_host.c_str()) || server->port != primary_port)
        {
          continue;
        }
      }
      FOREIGN_SERVER* cur_server = clone_server(mem_root, server, NULL);
      tdbctl_server_map[cur_server->server_name] = cur_server;

      //if this is SPIDER, should init connection,and insert into spider_conn_map
      if (!strcasecmp(server->scheme, SPIDER_WRAPPER))
      {
        string host = server->host;
        sstr.str("");
        sstr << server->port;
        string ports = sstr.str();
        string ipport = host + "#" + ports;
        MYSQL* mysql;
        if ((mysql = tc_conn_connect(ipport, server->username, server->password)))
          spider_conn_map.insert(pair<string, MYSQL*>(ipport, mysql));
        else
        {
          /* error */
          res = true;
          my_error(ER_TCADMIN_CONNECT_ERROR, MYF(0), ipport.c_str());
          goto finish;
        }
      }
    }
  }
finish:
  mysql_rwlock_unlock(&THR_LOCK_servers);
  return res;
}


/*
  compare and create repair sql for routing
  @param
    tdbctl_server_map: server_map of TDBCTL 
    spider_server_map: server_map of spider
    repair_sql(out):   the sql to repair mysql.servers of spider

  @retval
    true:              need to repair
    false:             no need to repair
*/
bool tc_create_repair_sql(map<string, FOREIGN_SERVER*> tdbctl_server_map,
  map<string, FOREIGN_SERVER*>spider_server_map,
  string& repair_sql) 
{
  bool res = false;
  string replace_sql_all = "replace into mysql.servers"
    "(Server_name, Host, Db, Username, Password, Port, Socket, Wrapper, Owner)  values";
  string flush_priv_sql = "flush privileges";
  string del_sql_all = "";
  stringstream ss;
  string quotation = "\"";
  string comma = ",";
  string::size_type len = replace_sql_all.size();
  map<string, FOREIGN_SERVER*>::iterator tdbctl_its;
  map<string, FOREIGN_SERVER*>::iterator spider_its;
  for (tdbctl_its = tdbctl_server_map.begin();
    tdbctl_its != tdbctl_server_map.end();
    tdbctl_its++)
  {
    string replace_sql_cur = "";
    string del_sql_cur = "";
    FOREIGN_SERVER* server = tdbctl_its->second; 
    spider_its = spider_server_map.find(tdbctl_its->first);
    /*
    if same , do nothing.
    if not same or not exist, create replace_sql
    */
    if(spider_its!= spider_server_map.end())
    { 
      FOREIGN_SERVER* server_bak = spider_server_map[tdbctl_its->first];
      spider_server_map.erase(spider_its);
      if (!(strcmp(server->host, server_bak->host) ||
        strcmp(server->username, server_bak->username) ||
        strcmp(server->password, server_bak->password) ||
        strcmp(server->db, server_bak->db) ||
        strcmp(server->scheme, server_bak->scheme) ||
        strcmp(server->socket, server_bak->socket) ||
        strcmp(server->owner, server_bak->owner) ||
        server->port != server_bak->port))
      {/*if same, do nothing*/ 
        continue;
      }
    }
    /*
    if not exist or different, create replace_sql
    */
    replace_sql_cur = "(";
    string name = quotation + server->server_name + quotation + comma;
    string host = quotation + server->host + quotation + comma;
    string db = quotation + server->db + quotation + comma;
    string username = quotation + server->username + quotation + comma;
    string password = quotation + server->password + quotation + comma;
    int port = server->port;
    string socket = quotation + server->socket + quotation + comma;
    string wrapper = quotation + server->scheme + quotation + comma;
    string owner = quotation + server->owner + quotation;
    ss.str("");
    ss << port;
    string port_s = ss.str() + comma;
    replace_sql_cur = replace_sql_cur + name + host + db + username
      + password + port_s + socket + wrapper + owner;
    replace_sql_cur += "),";
    replace_sql_all += replace_sql_cur;
    res = true;
  }
  /*if there are data in spider_server_map, create del_sql*/
  if (spider_server_map.size() > 0) 
  {
    res = true;
    for (spider_its = spider_server_map.begin();
      spider_its != spider_server_map.end();
      spider_its++)
    {
      FOREIGN_SERVER* server = spider_its->second;
      string del_sql_cur = "delete from mysql.servers where Server_name=";
      del_sql_cur += quotation + server->server_name + quotation + ";";
      del_sql_all += del_sql_cur;
    }
  }
  if (res)
  {
    if (del_sql_all.size())
      repair_sql += del_sql_all;
    
    if (replace_sql_all.size() != len)
    {
      replace_sql_all.erase(replace_sql_all.end() - 1);
      repair_sql += replace_sql_all + ";";
    }
    repair_sql += flush_priv_sql;
  }
  return res;
}


int tc_check_and_repair_routing()
{
  int result = 0;
  map<string, MYSQL*> spider_conn_map;
  map<string, MYSQL*>::iterator its;
  string sql = "select Server_name,Host,Db,Username,Password,Port,Socket,Wrapper,Owner "
    "from mysql.servers order by Server_name";
  string flush_priv_sql = "flush privileges";
  string replace_sql;
  string repair_sql_all;
  tc_exec_info exec_info;
  map<string, FOREIGN_SERVER*> spider_server_map;
  map<string, FOREIGN_SERVER*> tdbctl_server_map;
  THD *thd;
  if (!(thd = new THD))
  {
    sql_print_warning("init repair thread failed, skip repair");
    result = 1;
    goto finish;
  }
  if (lock_statement_by_name(thd, server_uuid_ptr, MDL_EXCLUSIVE))
  {
    sql_print_error("lock for repair routing timeout");
    result = 1;
    goto finish;
  }
  replace_sql = generate_routing_sql_for_spider();
  repair_sql_all = replace_sql + ";" + flush_priv_sql;
  thd->variables.lock_wait_timeout = tc_check_repair_routing_interval;
  
  if (tc_get_repair_map(thd->mem_root, tdbctl_server_map, spider_conn_map)) 
  {
    result = 1;
    goto finish;
  }

  for (its = spider_conn_map.begin(); its != spider_conn_map.end(); its++)
  {
    string repair_sql = "";
    string ipport = its->first;
    MYSQL* mysql = its->second;
    MYSQL_RES* res;
    res = tc_exec_sql_with_result(mysql, sql);
    if (res)
    {
      MYSQL_ROW row = NULL;
      while ((row = mysql_fetch_row(res)))
      {
        FOREIGN_SERVER tmp_server;
        FOREIGN_SERVER* cur_server;
        tmp_server.server_name = row[0];
        tmp_server.server_name_length = (uint)strlen(row[0]);
        tmp_server.host = row[1];
        tmp_server.db = row[2];
        tmp_server.username = row[3];
        tmp_server.password = row[4];
        tmp_server.sport = row[5];
        tmp_server.port = tmp_server.sport ? atoi(tmp_server.sport) : 0;
        tmp_server.socket = row[6];
        tmp_server.scheme = row[7];
        tmp_server.owner = row[8];
        cur_server = clone_server(thd->mem_root, &tmp_server, NULL);
        spider_server_map[cur_server->server_name] = cur_server;
      }
      if (tc_create_repair_sql(tdbctl_server_map, spider_server_map, repair_sql))
      {
        sql_print_warning("ipport is %s, routing mismatch", ipport.c_str());
        if (tc_exec_sql_up(mysql, repair_sql, &exec_info))
        {
          sql_print_error("ipport is %s, routing repair failed", ipport.c_str());
          result = 2;
        }
        else
        {
          sql_print_information("ipport is %s, routing repair succeed", ipport.c_str());
        }
      }
      spider_server_map.clear();
      mysql_free_result(res);
    }
    else
    {
      sql_print_warning("ipport is %s, routing mismatch", ipport.c_str());
      if (tc_exec_sql_up(mysql, repair_sql_all, &exec_info))
      {
        sql_print_error("ipport is %s, routing repair failed", ipport.c_str());
        result = 2;
      }
      else
      {
        sql_print_information("ipport is %s, routing repair succeed", ipport.c_str());
      }
    }
  }

finish:

  tc_conn_free(spider_conn_map);
  spider_conn_map.clear();
  tdbctl_server_map.clear();
  spider_server_map.clear();
  delete thd;
  return result;
}


void create_check_and_repaire_routing_thread()
{
  std::thread t(tc_check_and_repair_routing_thread);
  t.detach();
}


ulong get_modify_server_version()
{
  return global_modify_server_version;
}


ulong get_server_version_by_name(const char* server_name)
{
  size_t server_name_length;
  ulong server_version = 0;
  FOREIGN_SERVER* server;
  DBUG_ENTER("get_server_version");

  server_name_length = strlen(server_name);

  if (!server_name || !strlen(server_name))
  {
    DBUG_RETURN(0);
  }

  mysql_rwlock_rdlock(&THR_LOCK_servers);
  if ((server = (FOREIGN_SERVER*)my_hash_search(&servers_cache, 
                                                (uchar*)server_name, 
                                                server_name_length)))
  {
    server_version = server->version;
  }
  mysql_rwlock_unlock(&THR_LOCK_servers);
  DBUG_RETURN(server_version);
}

int back_up_one_server(FOREIGN_SERVER* server)
{
  int error = 0;
  DBUG_ENTER("insert_into_servers_cache_version");
  /* construct  FOREIGN_SERVER_V */
  FOREIGN_SERVER* tmp = (FOREIGN_SERVER*)alloc_root(&mem_bak, sizeof(FOREIGN_SERVER));
  tmp->server_name = safe_strdup_root(&mem_bak, server->server_name);
  tmp->host = safe_strdup_root(&mem_bak, server->host);
  tmp->username = safe_strdup_root(&mem_bak, server->username);
  tmp->password = safe_strdup_root(&mem_bak, server->password);
  tmp->db = safe_strdup_root(&mem_bak, server->db);
  tmp->scheme = safe_strdup_root(&mem_bak, server->scheme);
  tmp->socket = safe_strdup_root(&mem_bak, server->socket);
  tmp->owner = safe_strdup_root(&mem_bak, server->owner);
  // "create server ..." may trigger a bug here
  // tmp->sport = safe_strdup_root(&mem_bak, server->sport);
  tmp->port = server->port;
  tmp->server_name_length = server->server_name_length;
  tmp->version = server->version;

  if (my_hash_insert(&servers_cache_bak, (uchar*)tmp))
  {
    // error handling needed here
    error = 1;
  }
  DBUG_RETURN(error);
}


bool backup_server_cache()
{
  FOREIGN_SERVER* server;
  ulong share_records = servers_cache.records;
  DBUG_ENTER("backup_server_cache");
  for (ulong i = 0; i < share_records; i++)
  {/* foreach share */
    server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i);
    if (back_up_one_server(server))
      DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}

/*
  @NOTE
    compare the num of server in mysql.servers when server_reload
  @param
    with_slave: if true, server_list include SERVER_SLAVE
    with_lock:  if true, get num with lock
*/
bool compare_server_num_by_warpper(const char* wrapper_name, bool with_slave,
  bool with_lock)
{
  FOREIGN_SERVER* server = NULL;
  ulong ret = 0;
  ulong ret_bak = 0;
  string wrapper_slave = wrapper_name;
  if (with_slave)
    wrapper_slave += "_SLAVE";
  if(with_lock)
    mysql_rwlock_rdlock(&THR_LOCK_servers);
  ulong records = servers_cache.records;
  ulong records_bak = servers_cache_bak.records;

  if (records != 0)
  {
    for (ulong i = 0; i < records; i++)
    {
      if (!(server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i)))
        server = (FOREIGN_SERVER*)NULL;
      else
      {
        if (!strcasecmp(server->scheme, wrapper_name) ||
          !strcasecmp(server->scheme, wrapper_slave.c_str()))
          ++ret;
      }
    }
  }
  if (records_bak != 0)
  {
    for (ulong i = 0; i < records_bak; i++)
    {
      if (!(server = (FOREIGN_SERVER*)my_hash_element(&servers_cache_bak, i)))
        server = (FOREIGN_SERVER*)NULL;
      else
      {
        if (!strcasecmp(server->scheme, wrapper_name) ||
          !strcasecmp(server->scheme, wrapper_slave.c_str()))
          ++ret_bak;
      }
    }
  }

  if (with_lock)
    mysql_rwlock_unlock(&THR_LOCK_servers);
  return ret != ret_bak;
}


bool update_server_version(bool* version_updated)
{
  FOREIGN_SERVER* server_bak;
  FOREIGN_SERVER* server;
  ulong share_records = servers_cache.records;
  ulong share_records_bak = servers_cache_bak.records;
  if (compare_server_num_by_warpper(TDBCTL_WRAPPER, false, false))
  {
    *version_updated = TRUE;
    modify_tdbctl_flag = true;
  }
  else
  {
    if (share_records != share_records_bak)
    {
      *version_updated = TRUE;
    }
  }

  DBUG_ENTER("replace_server_version");
  for (ulong i = 0; i < share_records; i++)
  {/* foreach share */
    server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i);
    if ((server_bak = (FOREIGN_SERVER*)my_hash_search(&servers_cache_bak, 
                                                      (uchar*)server->server_name, 
                                                      server->server_name_length)))
    {/* exist, update mysql.servers.version */
      if (strcmp(server->host, server_bak->host) ||
        strcmp(server->username, server_bak->username) ||
        strcmp(server->password, server_bak->password) ||
        strcmp(server->db, server_bak->db) ||
        strcmp(server->scheme, server_bak->scheme) ||
        strcmp(server->socket, server_bak->socket) ||
        strcmp(server->owner, server_bak->owner) ||
        server->port != server_bak->port)
      {/* not equal: 1.update server_v; 2.version++ */
        server_bak->version++;
        *version_updated = TRUE;
		/*
		if modify tdbctl ,need to maintain modify_tdbctl_flag 
		*/
		if (!modify_tdbctl_flag &&
			(!native_strncasecmp(server->server_name, tdbctl_control_wrapper_prefix, strlen(tdbctl_control_wrapper_prefix)) ||
			!native_strncasecmp(server_bak->server_name, tdbctl_control_wrapper_prefix, strlen(tdbctl_control_wrapper_prefix))))
		{
			modify_tdbctl_flag = true;
		}
      }
      server->version = server_bak->version;
    }
    else/*if insert new server into mysql.servers*/
    {
      *version_updated = TRUE;
      if (!modify_tdbctl_flag &&
        !native_strncasecmp(server->server_name, tdbctl_control_wrapper_prefix, strlen(tdbctl_control_wrapper_prefix)))
      {
        modify_tdbctl_flag = true;
      }
    }

  }
  DBUG_RETURN(FALSE);
}


// void get_deleted_servers()
// {
//   FOREIGN_SERVER* server_bak;
//   FOREIGN_SERVER* server;
//   ulong records = servers_cache.records;
//   ulong bak_records = servers_cache_bak.records;

//   if (bak_records > records)
//   {/* delete some servers */
//     for (ulong i = 0; i < bak_records; i++)
//     {
//       server_bak = (FOREIGN_SERVER*)my_hash_element(&servers_cache_bak, i);
//       if (!(server = (FOREIGN_SERVER*)my_hash_search(&servers_cache,
//         (uchar*)server_bak->server_name, server_bak->server_name_length)))
//       {/* don't exits */
//         string name = server_bak->server_name;
//         to_delete_servername_list.push_back(name);
//       }
//     }
//   }
// }


int get_remote_changed_servers(
  MEM_ROOT *mem_root, 
  list<FOREIGN_SERVER*> *diff_serverlist
)
{
  int result = 0;
  map<string, MYSQL*> spider_conn_map;
  map<string, MYSQL*>::iterator its;
  string sql = "select Server_name,Host,Db,Username,Password,Port,Socket,Wrapper,Owner "
               "from mysql.servers where Wrapper = \"mysql\" order by Server_name";
  list<FOREIGN_SERVER*> new_list;
  Cluster_conn_manager *conn_mgr = new Cluster_conn_manager();
  //exclude spider_slave
  if (conn_mgr->refresh(false, true) || conn_mgr->connect(SPIDER_WRAPPER, false))
  {
    delete conn_mgr;
    return 1;
  }
  else
    spider_conn_map = conn_mgr->get_spider_conn_map();

  get_server_by_wrapper(new_list, mem_root, MYSQL_WRAPPER, FALSE);
  new_list.sort(server_compare);

  for (its = spider_conn_map.begin(); its != spider_conn_map.end(); its++)
  {
    string ipport = its->first;
    MYSQL* mysql = its->second;
    MYSQL_RES* res;
    res = tc_exec_sql_with_result(mysql, sql);
    if (res)
    {
      MYSQL_ROW row = NULL;
      list<FOREIGN_SERVER*> old_list;
      int ret = 0;
      while ((row = mysql_fetch_row(res)))
      {
        FOREIGN_SERVER tmp_server;
        FOREIGN_SERVER* cur_server;
        tmp_server.server_name = row[0];
        tmp_server.server_name_length = (uint)strlen(row[0]);
        tmp_server.host = row[1];
        tmp_server.db = row[2];
        tmp_server.username = row[3];
        tmp_server.password = row[4];
        tmp_server.sport = row[5];
        tmp_server.port = tmp_server.sport ? atoi(tmp_server.sport) : 0;
        tmp_server.socket = row[6];
        tmp_server.scheme = row[7];
        tmp_server.owner = row[8];
        cur_server = clone_server(mem_root, &tmp_server, NULL);
        old_list.push_back(cur_server);
      }
      old_list.sort(server_compare);

      ret = compare_and_return_changed_server(old_list, new_list, diff_serverlist);
      if (ret == 2)
      {
        result = 2;
      }
      else if(ret == 1)
      {
        result = 1;
      }
      else
      {
        result = 0;
      }
      old_list.clear();
      mysql_free_result(res);
    }
    else
    {
			sql_print_warning("TDBCTL: ipport is %s, routing mismatch", ipport.c_str());
      result = 2;
    }
    break;
  }


  delete conn_mgr;
  new_list.clear();
  return result;
}

// string get_delete_routing_sql()
// {
//   list<string>::iterator its;
//   string sql = "";
//   mysql_rwlock_rdlock(&THR_LOCK_servers);
//   if (to_delete_servername_list.size() > 0)
//   {
//     string del_sql = "delete from mysql.servers where Server_name in(";
//     string quotation = "\"";
//     for (its = to_delete_servername_list.begin();
//       its != to_delete_servername_list.end(); its++)
//     {
//       string name = *its;
//       del_sql = del_sql + quotation + name + quotation;
//     }
//     sql = del_sql + ")";
//   }
//   mysql_rwlock_unlock(&THR_LOCK_servers);
//   return sql;
// }

// int delete_redundant_routings()
// {
//   string del_sql = get_delete_routing_sql();
//   if (del_sql.length() > 0)
//   {
//     map<string, MYSQL*> spider_conn_map;
//     Cluster_conn_manager *conn_mgr = new Cluster_conn_manager();
//     //exclude spider_slave
//     if (conn_mgr->refresh(false, true) || conn_mgr->connect(NODE_TYPE_SPIDER, false))
//     {
//       delete conn_mgr;
//       return 1;
//     }
//     else
//       spider_conn_map = conn_mgr->get_spider_conn_map();

//     string flush_priv_sql = "flush privileges";
//     string sql;
//     map<string, MYSQL*>::iterator it;
//     sql = del_sql + ";" + flush_priv_sql;

//     for (it = spider_conn_map.begin(); it != spider_conn_map.end(); it++)
//     {
//       string ipport = it->first;
//       MYSQL* mysql = it->second;
//       tc_exec_info exec_info;
//       tc_exec_sql_without_result(mysql, sql, &exec_info);
//     }

//     delete conn_mgr;
//     to_delete_servername_list.clear();
//   }

//   return 0;
// }

bool prepare_server_creation(THD *thd, LEX *lex, std::string &err_msg) 
{
  err_msg.clear();

  /* Check if all USER, PASSWORD, HOST, PORT options are specified */
  if (!lex->server_options.get_host() ||
      (lex->server_options.get_port() == lex->server_options.PORT_NOT_SET) ||
      !lex->server_options.get_username() ||
      !lex->server_options.get_password())
  {
    err_msg = "USER, PASSWORD, HOST, and PORT options should all be specified";
    return true;
  }
  
  /* Generate new node's server name string */
  std::string server_name;
  if (lex->server_options.get_num() == lex->server_options.NUM_NOT_SET)
  {
    /* get an unique server_name by wrapper */
    server_name = get_new_server_name_by_wrapper(lex->server_options.get_scheme());
  } else {
    // produce server_name with server_options->m_num
    server_name = get_new_server_name_by_number(lex->server_options.get_scheme(),
                                                lex->server_options.get_num());
  }
  DBUG_ASSERT(server_name.length() != 0);
  lex->server_options.m_server_name.length = server_name.length();
  lex->server_options.m_server_name.str =
      strmake_root(thd->mem_root, server_name.c_str(), server_name.length());
  
  /* Get all servers */
  list<FOREIGN_SERVER *> server_list;
  get_server_by_wrapper(server_list, thd->mem_root, NULL_WRAPPER, TRUE);
  if(server_list.empty()) {
    return false;
  }

  /*
    Check if server name or ip#port already exists
    For mysql and mysql_slave nodes, skip ip#port check
    For other node types, the new node's ip#port must be unique in the cluster
  */
  bool is_already_exist = false;
  string external_ip_port = string(lex->server_options.get_host()) + "#" +
                            to_string(lex->server_options.get_port());
  string err_buff;
  for(FOREIGN_SERVER *server : server_list) {
    if (strcasecmp(lex->server_options.m_server_name.str, server->server_name) == 0) {
      err_buff = "the server_name " + std::string(lex->server_options.m_server_name.str) + 
                " already exists in the mysql.servers";
      is_already_exist = true;
      break;
    }
    if (strcasecmp(lex->server_options.get_scheme(), MYSQL_WRAPPER) == 0 || 
        strcasecmp(lex->server_options.get_scheme(), MYSQL_SLAVE_WRAPPER) == 0) {
      continue;
    }
    string internal_ip_port = string(server->host) + "#" + to_string(server->port);
    if (external_ip_port.compare(internal_ip_port) == 0) {
      err_buff = "the ip#port " + external_ip_port + " already exists in the mysql.servers";
      is_already_exist = true;
      break;
    }
  }
  if (is_already_exist) {
    err_msg = err_buff;
    return true;
  }

  /* Check if IPs are consistent (all non-localhost or all localhost) */
  unsigned int localhost_count = 0;
  auto is_localhost = [](const char *ip) -> bool {
    return (!strcasecmp(ip, "127.0.0.1") || !strcasecmp(ip, "localhost"));
  };
  if (is_localhost(lex->server_options.get_host())) {
    ++localhost_count;
  }
  for(FOREIGN_SERVER *server: server_list) {
    if (is_localhost(server->host)) {
      ++localhost_count;
    }
  }
  if ((localhost_count > 0) && (localhost_count != server_list.size() + 1)) {
    err_msg = "mysql.servers can't contain both loop-back network address and "
              "external network address, please change the value of 'host' column";
    return true;
  }

  return false;
}

/**
 * @brief Get auto-increment related information from spider connections
 * 
 * @param spider_conns Map of spider connections (key: Server name, value: MYSQL pointer)
 * @param autoinc_info Output parameter to store retrieved auto-increment information
 * @param errmsg Output parameter for error message
 * @return true if failed to get variables, false otherwise
 */
bool get_spider_autoinc_info(const std::map<std::string, MYSQL *> &spider_conns,
                             std::map<std::string, SPIDER_AUTOINC_INFO> &autoinc_info, 
                             std::string &errmsg)
{
  static const std::string autoinc_variables[] = {"SPIDER_AUTO_INCREMENT_MODE_SWITCH",
                                                  "SPIDER_AUTO_INCREMENT_MODE_VALUE",
                                                  "SPIDER_AUTO_INCREMENT_STEP"};

  autoinc_info.clear();
  errmsg.clear();
  const size_t var_count = sizeof(autoinc_variables) / sizeof(string);
  std::string var_values[var_count];
  for (auto it = spider_conns.begin(); it != spider_conns.end(); ++it)
  {
    if (tc_get_multi_variable_values(it->second, autoinc_variables, 
        var_values, var_count)) {
      errmsg = "Failed to get auto-increment related variable values from spider node " + it->first;
      // sql_print_warning("get_spider_autoinc_info: %s", errmsg.c_str());
      return true;
    }
    autoinc_info[it->first] = SPIDER_AUTOINC_INFO(var_values[0] == "ON",
      (unsigned int)std::strtoul(var_values[1].c_str(), nullptr, 10),
      (unsigned int)std::strtoul(var_values[2].c_str(), nullptr, 10));
  }
  return false;
}


/**
 * Validates auto-increment settings across spider nodes for consistency.
 * 
 * This function checks if all spider nodes have consistent auto-increment configurations.
 * It identifies conflicts in mode switch status, mode values, and increment steps.
 * 
 * @param autoinc_info Map containing auto-increment info keyed by node name
 * @param failure_details Output vector to store conflict details
 * @return bool True if all settings are consistent, false if conflicts exist
 */
bool validate_auto_increment_settings(const std::map<std::string, SPIDER_AUTOINC_INFO> &autoinc_info, 
                                      std::vector<SPIDER_AUTOINC_CONFLICT_ITEM> &failure_details)
{
  bool is_ok = true;
  failure_details.clear();
  std::vector<std::string> switch_on;
  std::vector<std::string> switch_off;
  std::map<unsigned int, vector<std::string>> mode_value_map;
  std::map<unsigned int, vector<std::string>> inc_step_map;

  // Classify nodes based on their auto-increment settings
  for(auto it = autoinc_info.begin(); it != autoinc_info.end(); ++it)
  {
    if (it->second.mode_switch) {
      switch_on.push_back(it->first);  // Add to enabled nodes list
      mode_value_map[it->second.mode_value].push_back(it->first);  // Group by mode value
      inc_step_map[it->second.inc_step].push_back(it->first);  // Group by increment step
    } else {
      switch_off.push_back(it->first);  // Add to disabled nodes list
    }
  }

  // Check if some nodes have auto-increment enabled while others don't
  if(switch_on.size() && switch_off.size()) {
    is_ok = false;
    std::string conflict_str = "";
    conflict_str += "Spider nodes with auto-increment enabled: "
                    + boost::algorithm::join(switch_on, ", ");
    conflict_str += "; ";
    conflict_str += "Spider nodes with auto-increment disabled: "
                    + boost::algorithm::join(switch_off, ", ");
    failure_details.emplace_back(SPIDER_AUTOINC_CONFLICT_MODE_SWITCH, conflict_str);
  }

  // Only perform these checks if there are nodes with auto-increment enabled
  if(!switch_on.empty()) {
    // Check for nodes with same mode value
    for(auto it = mode_value_map.begin(); it != mode_value_map.end(); ++it) {
      if(it->second.size() > 1) {  // More than one node shares this mode value
        is_ok = false;
        std::string conflict_str = "";
        conflict_str += "Spider nodes with same auto-increment mode value "
                        + std::to_string(it->first) + ": "
                        + boost::algorithm::join(it->second, ", ");
        failure_details.emplace_back(SPIDER_AUTOINC_CONFLICT_MODE_VALUE, conflict_str);
      }
    }

    // Check for multiple increment step values
    if(inc_step_map.size() > 1) {  // More than one unique increment step value exists
      is_ok = false;
      std::string conflict_str = "";
      for(auto it = inc_step_map.begin(); it != inc_step_map.end(); ++it) {
        if(!conflict_str.empty()) {
          conflict_str += "; ";
        }
        conflict_str += "Spider nodes with auto-increment step value "
                        + std::to_string(it->first) + ": "
                        + boost::algorithm::join(it->second, ", ");
      }
      failure_details.emplace_back(SPIDER_AUTOINC_CONFLICT_INC_STEP, conflict_str);
    }
  }

  return is_ok;
}

/**
 * Check auto-increment compatibility between cluster nodes and foreign server
 * 
 * @param cluster_autoinc_map Map of auto-increment info for all cluster nodes
 * @param foreign_server_name Name of the foreign server to check
 * @param foreign_server_autoinc Auto-increment info of the foreign server
 * @param[out] conflict_type Type of conflict detected (output parameter)
 * @param[out] conflict_str Description of conflict (output parameter)
 * @return bool True if any conflict found, false otherwise
 */
bool check_auto_increment_compatibility(const std::map<std::string, SPIDER_AUTOINC_INFO> &cluster_autoinc_map, 
                                        const std::string &foreign_server_name, 
                                        const SPIDER_AUTOINC_INFO &foreign_server_autoinc,
                                        SPIDER_AUTOINC_CONFLICT &conflict_type,
                                        std::string &conflict_str) 
{
  bool found_conflict = false;
  conflict_type = SPIDER_AUTOINC_CONFLICT_NONE;
  conflict_str.clear();

  // Iterate through all cluster nodes to check compatibility
  for(auto it = cluster_autoinc_map.begin(); it != cluster_autoinc_map.end(); ++it)
  {
    // Skip comparison with self
    if(it->first == foreign_server_name) {
      continue;
    }

    // Check mode switch (enabled/disabled) conflict
    if(it->second.mode_switch != foreign_server_autoinc.mode_switch) {
      found_conflict = true;
      conflict_type = SPIDER_AUTOINC_CONFLICT_MODE_SWITCH;
      conflict_str += "Spider node " + it->first + " has auto-increment mode "
                      + (it->second.mode_switch ? "enabled" : "disabled")
                      + " while foreign server " + foreign_server_name 
                      + " has auto-increment mode "
                      + (foreign_server_autoinc.mode_switch ? "enabled" : "disabled");
      break;
    }

    // Check mode value conflict (same value not allowed)
    if(it->second.mode_value == foreign_server_autoinc.mode_value) {
      found_conflict = true;
      conflict_type = SPIDER_AUTOINC_CONFLICT_MODE_VALUE;
      conflict_str += "Spider node " + it->first + " and the foreign server "
                      + foreign_server_name + " have the same auto-increment mode value "
                      + std::to_string(it->second.mode_value);
      break;
    }

    // Check increment step conflict
    if(it->second.inc_step != foreign_server_autoinc.inc_step) {
      found_conflict = true;
      conflict_type = SPIDER_AUTOINC_CONFLICT_INC_STEP;
      conflict_str += "Spider node " + it->first + "\'s auto-increment step is "
                      + std::to_string(it->second.inc_step)
                      + " while foreign server " + foreign_server_name + "\'s step is "
                      + std::to_string(foreign_server_autoinc.inc_step);
      break;
    }
  }

  return found_conflict;
}

bool check_spider_autoinc_settings(THD *thd, LEX *lex)
{
  Cluster_conn_manager *conn_mgr = new Cluster_conn_manager();
  const std::map<std::string, MYSQL *> spider_conn_map = conn_mgr->get_spider_conn_map();

  std::map<std::string, SPIDER_AUTOINC_INFO> spider_autoinc_map;
  std::string errmsg;
  if(get_spider_autoinc_info(spider_conn_map, spider_autoinc_map, errmsg)) {
    // todo: log error message
    return true;
  }
  return false;
}
