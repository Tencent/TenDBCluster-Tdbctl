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
#include "sql_servers.h"
/*
  We only use 1 mutex to guard the data structures - THR_LOCK_servers.
  Read locked when only reading data and write-locked for all other access.
*/

static list<string> to_delete_servername_list;
static MEM_ROOT tc_mem;
static HASH servers_cache_bak;
static MEM_ROOT mem_bak;
/*
global_modify_server_version_old start from 1,
because global_modify_server_version++ when init mysqld, 
but global_modify_server_version_old++ at the next time
*/
ulong global_modify_server_version = 0;
ulong global_modify_server_version_old = 1;

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

#define RESULT_SUCCEED  0
#define RESULT_FAILED   1
#define RESULT_ABNORMAL 0

static string dump_servers_to_sql();
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
  get_deleted_servers();
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
  when init mysqld, global_modify_server_version=1, 
  it is not ok to get tc_is_master_tdbctl_node, because mysqld is not serving
   */
  if (global_modify_server_version != 1 &&
	  global_modify_server_version_old != global_modify_server_version)
  {
	  tdbctl_is_primary = tc_is_primary_tdbctl_node();
	  global_modify_server_version_old = global_modify_server_version;
  }
  delete_redundant_routings();
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
  /* if not do this, while we get port from server_caches, may core dump */
  server->sport = strdup_root(&mem, std::to_string(server->port).c_str());
  server->version = 0;
  global_modify_server_version++;

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

  existing->version++;
  global_modify_server_version++;
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

  if (error == 0 && !thd->killed)
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
				global_modify_server_version++;
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
				to_delete_servername_list.clear();
				to_delete_servername_list.push_back(server->server_name);
			}
      else if (!m_if_exists)
      {
        my_error(ER_FOREIGN_SERVER_DOESNT_EXIST, MYF(0),  m_server_name.str);
        error= 1;
      }
    }
  }

  mysql_rwlock_unlock(&THR_LOCK_servers);

  if (error)
    trans_rollback_stmt(thd);
  else
    trans_commit_stmt(thd);
  close_mysql_tables(thd);

	/* after delete server, should transfer to spider also */
	delete_redundant_routings();
  if (close_cached_connection_tables(thd, m_server_name.str,
                                     m_server_name.length))
  {
    push_warning_printf(thd, Sql_condition::SL_WARNING,
                        ER_UNKNOWN_ERROR, "Server connection in use");
  }

  if (error == 0 && !thd->killed)
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

  server_name_length= strlen(server_name);

  if (! server_name || !strlen(server_name))
  {
    DBUG_PRINT("info", ("server_name not defined!"));
    DBUG_RETURN((FOREIGN_SERVER *)NULL);
  }

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
@param with_slave if true, server_list include SPIDER_SLAVE
*/
ulong get_servers_count_by_wrapper(const char* wrapper_name, bool with_slave)
{
	ulong ret = 0;
	FOREIGN_SERVER* server = NULL;
	string wrapper_slave = wrapper_name;
	if (with_slave)
	{
		wrapper_slave += "_SLAVE";
	}
	mysql_rwlock_rdlock(&THR_LOCK_servers);
	ulong records = servers_cache.records;
	if(records == 0)
		goto finish;
	for (ulong i = 0; i < records; i++)
	{
		if (!(server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i)))
		{
			server = (FOREIGN_SERVER*)NULL;
		}
		else
		{
			if (!strcasecmp(server->scheme, wrapper_name) ||
				!strcasecmp(server->scheme, wrapper_slave.c_str()))
			{
				++ret;
			}
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
	{
		return TRUE;
	}
	else
	{
		return FALSE;
	}
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
	DBUG_ASSERT(wrapper_name);
  ulong records = 0;
  FOREIGN_SERVER* server;
  string wrapper_slave = wrapper_name;
  mysql_rwlock_rdlock(&THR_LOCK_servers);
  records = servers_cache.records;
  if (with_slave)
  {
    wrapper_slave +=  "_SLAVE";
  }

  for (ulong i = 0; i < records; i++)
  {
    if (!(server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i)))
    {
      server = (FOREIGN_SERVER*)NULL;
    }
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

/**
  generate a new unique server_name by wrapper
	server_name = wrapper_name + increase id

  @wrapper_name TDBCTL|SPIDER_SLAVE|SPIDER|mysql

	@retval return an unique server_name which
	        not exist in mysql.servers
	TODO: generate continuous number, gap maybe exist after
	frequent ADD/DROP node
*/
string get_new_server_name_by_wrapper(
    const char* wrapper_name,
	  bool with_slave
)
{
	ostringstream server_name;
  string wrapper_slave = wrapper_name;
  ulong records = 0;
	ulong max_suffix_num = 0;

	server_name.str("");
	server_name << get_wrapper_prefix_by_wrapper(wrapper_name);
  if (with_slave)
    wrapper_slave +=  "_SLAVE";

  mysql_rwlock_rdlock(&THR_LOCK_servers);
  records = servers_cache.records;

  for (ulong i = 0; i < records; i++)
  {
    if (auto server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i))
    {
			//for spider, total SPIDER and SPIDER_SLAVE's server_name must be unique
      if (!strcasecmp(server->scheme, wrapper_name) ||
				(with_slave && !strcasecmp(server->scheme, wrapper_slave.c_str())))
      {
				ulong suffix_num = 0;
				string prefix = server->scheme;
				regex pattern(wrapper_name, regex::icase);
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

const char *get_wrapper_prefix_by_wrapper(
	const char *wrapper_name
)
{
	if (strcasecmp(wrapper_name, SPIDER_WRAPPER) == 0 ||
		strcasecmp(wrapper_name, SPIDER_SLAVE_WRAPPER) == 0)
		return tdbctl_spider_wrapper_prefix;
	else if (strcasecmp(wrapper_name, TDBCTL_WRAPPER) == 0)
		return tdbctl_control_wrapper_prefix;
	else if (strcasecmp(wrapper_name, MYSQL_WRAPPER) == 0 ||
		strcasecmp(wrapper_name, MYSQL_SLAVE_WRAPPER) == 0)
		return tdbctl_mysql_wrapper_prefix;
	else
		return wrapper_name;
}

/**
get server info from mysql.servers and generate SQL statement

  @Note
	for Tdbctl node, not dump all tdbctl nodes info, we only chose one.
	MGR scenario:
	   Multi-Primary: use first member(store to map, use map's order)
		 Single-Primary: use Primary member
	None-MGR scenario:
	   always use first member
*/
static string dump_servers_to_sql()
{
  ulong records = 0;
  FOREIGN_SERVER* server;
	map<string, string> tdbctl_sql_map;
  mysql_rwlock_rdlock(&THR_LOCK_servers);
  records = servers_cache.records;
  string replace_sql_all = "replace into mysql.servers"
    "(Server_name, Host, Db, Username, Password, Port, Socket, Wrapper, Owner)  values";
  stringstream ss;
  string quotation = "\"";
  string comma = ",";

  if (records == 0)
  {
    mysql_rwlock_unlock(&THR_LOCK_servers);
    return "";
  }

  for (ulong i = 0; i < records; i++)
  {
    server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i);
    if (server)
    {
      string replace_sql_cur = "(";
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
			/* for tdbctl node, need special deal subsequent */
			if (strcasecmp(server->scheme, TDBCTL_WRAPPER) == 0)
			{
				/* NOTE: at present, ip#port must be unique for tdbctl */
				string ip_port = string(server->host) + "#" + ss.str();
				tdbctl_sql_map.insert(pair<string, string>(ip_port, replace_sql_cur));
				continue;
			}
      replace_sql_all += replace_sql_cur;
    }
  }

	if (!tdbctl_sql_map.empty())
	{
		string ip_port;
		string primary_host = "";
		uint primary_port;
		int	ret = tc_get_primary_node(primary_host, &primary_port);
		if (ret != -1)
		{
			ss.str("");
			ss << primary_port;
			ip_port = primary_host + "#" + ss.str();

			if (tdbctl_sql_map.count(ip_port) == 1)
				//add tdbctl insert sql
				replace_sql_all += tdbctl_sql_map[ip_port];
			else {
				sql_print_warning("primary node not in mysql.servers, return null sql");
				mysql_rwlock_unlock(&THR_LOCK_servers);
				return "";
			}
		}
		else
		{// unknown error, such as network partition.
			sql_print_warning("get primary node info failed");
			mysql_rwlock_unlock(&THR_LOCK_servers);
			return "";
		}
	}

  replace_sql_all.erase(replace_sql_all.end() - 1);
  mysql_rwlock_unlock(&THR_LOCK_servers);
  return replace_sql_all;
}


bool get_servers_rollback_sql()
{
  int ret;
  std::map<std::string, MYSQL*> spider_conn_map;
  std::map<std::string, std::string> spider_user_map;
  std::map<std::string, std::string> spider_passwd_map;
  std::set<std::string> spider_ipport_set;
  spider_ipport_set = get_spider_ipport_set(&mem, 
                                            spider_user_map, 
                                            spider_passwd_map, 
                                            FALSE);
  spider_conn_map = tc_spider_conn_connect(ret, spider_ipport_set, 
                                           spider_user_map, 
                                           spider_passwd_map);
  if (ret)
  {
    return TRUE;
  }

  string sql = "select * from mysql.servers";


  return FALSE;
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
//  string sql = "set global super_read_only=1";
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
      time_t to_tm_time = (time_t)time((time_t*)0);
      struct tm lt;
      struct tm* l_time = localtime_r(&to_tm_time, &lt);
      fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [ERROR TDBCTL] "
        "failed to set remote data node read only %s, and some error happened when modify spider routing \n",
        l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, l_time->tm_hour,
        l_time->tm_min, l_time->tm_sec, remote_ipport_map.begin()->second.c_str());
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

  diff_server_list.clear();
  remote_passwd_map.clear();
  remote_user_map.clear();
  remote_ipport_map.clear();
  free_root(&mem_root, MYF(0));
  return result;
}

/**
  do GRANT PRIVILEGES on tdbctl, remote, spider

	NOTES
    spider node: need simple privileges to connect any tdbctl to transfer sql; need
      INSERT, DELETE, UPDATE, DROP to connect any remote and do DML
    tdbctl node: need ALL PRIVILEGES to connect spider, remote do DDL etc.
    remote node: N/A

	  tdbctl only need to do grants on primary node at present
		tdbctl, spider, remote must all exists in mysql.servers, otherwise, return 0(ok).

	RETURN VALUE
	  zero is success, no zero for error.
*/
int tc_do_grants_internal()
{
	int ret = 0;
  MEM_ROOT mem_root;
	map<string, MYSQL*> spider_conn_map;
	map<string, string> spider_user_map;
	map<string, string> spider_passwd_map;
	map<string, tc_exec_info> spider_result_map;
	string spider_do_sql;

	map<string, MYSQL*> remote_conn_map;
	map<string, string> remote_user_map;
	map<string, string> remote_passwd_map;
  map<string, tc_exec_info> remote_result_map;
	string remote_do_sql;

	map<string, MYSQL*> tdbctl_conn_map;
	map<string, string> tdbctl_user_map;
	map<string, string> tdbctl_passwd_map;
  map<string, tc_exec_info> tdbctl_result_map;
	MYSQL *tdbctl_primary_conn = NULL;
	string tdbctl_do_sql;

  init_sql_alloc(key_memory_servers , &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
	/* not include SPIDER_SLAVE at present */
	set<string>  spider_ipport_set = get_spider_ipport_set(
			&mem_root,
			spider_user_map,
			spider_passwd_map,
			FALSE);
	map<string, string> remote_ipport_map = get_remote_ipport_map(
			&mem_root,
			remote_user_map,
			remote_passwd_map);
	map<string, string> tdbctl_ipport_map = get_tdbctl_ipport_map(
			&mem_root,
			tdbctl_user_map,
			tdbctl_passwd_map);

	if (spider_ipport_set.empty() ||
		remote_ipport_map.empty() || tdbctl_ipport_map.empty())
	{
		ret = 0;
		sql_print_information("skip do internal grant,",
			"at least exist one of each spider, tdbctl and remote in mysql.servers");
		goto exit;
	}

	tdbctl_do_sql = tc_get_tdbctl_grant_sql(spider_ipport_set, spider_user_map,
		spider_passwd_map, tdbctl_ipport_map, tdbctl_user_map, tdbctl_passwd_map);

	spider_conn_map = tc_spider_conn_connect(ret,
		spider_ipport_set, spider_user_map, spider_passwd_map);
	if (ret) {
		goto exit;
	}
	remote_conn_map = tc_remote_conn_connect(ret,
		remote_ipport_map, remote_user_map, remote_passwd_map);
	if (ret) {
		goto exit;
	}
	//tdbctl_conn_map = tc_tdbctl_conn_connect(ret,
	//	tdbctl_ipport_map, tdbctl_user_map, tdbctl_passwd_map);
	//if (ret) {
	//	goto exit;
	//}
	tdbctl_primary_conn = tc_tdbctl_conn_primary(ret, tdbctl_ipport_map, tdbctl_user_map, tdbctl_passwd_map);
	if (ret) {
		sql_print_information("connect to tdbctl primary failed");
		goto exit;
	}

	//spider do grant
	init_result_map(spider_result_map, spider_ipport_set);
	//set ddl_execute_by_ctl to off on spider, only need execute on spider node
	spider_do_sql = "set ddl_execute_by_ctl = off;";
	spider_do_sql += tc_get_spider_grant_sql(spider_ipport_set, spider_user_map,
		spider_passwd_map, tdbctl_ipport_map, tdbctl_user_map, tdbctl_passwd_map);
	if (tc_exec_sql_paral(spider_do_sql, spider_conn_map,
		spider_result_map, spider_user_map, spider_passwd_map, FALSE))
	{/* return, close conn, reconnect + retry all */
		ret = 1;
		sql_print_information("spider do internal grant failed");
		goto exit;
	}

	//remote do grant
	init_result_map2(remote_result_map, remote_ipport_map);
	remote_do_sql = tc_get_remote_grant_sql(spider_ipport_set, spider_user_map,
		spider_passwd_map, remote_ipport_map, remote_user_map, remote_passwd_map,
		tdbctl_ipport_map, tdbctl_user_map, tdbctl_passwd_map);
	if (tc_exec_sql_paral(remote_do_sql, remote_conn_map,
		remote_result_map, remote_user_map, remote_passwd_map, FALSE))
	{/* return, close conn, reconnect + retry all */
		ret = 1;
		sql_print_information("remote do internal grant failed");
		goto exit;
	}

	//tdbctl do grant
	init_result_map2(tdbctl_result_map, tdbctl_ipport_map);
	tdbctl_do_sql = "set tc_admin = off;";
	tdbctl_do_sql += tc_get_tdbctl_grant_sql(spider_ipport_set, spider_user_map,
		spider_passwd_map, tdbctl_ipport_map, tdbctl_user_map, tdbctl_passwd_map);
	if (tc_exec_sql_up(tdbctl_primary_conn, tdbctl_do_sql, &(tdbctl_result_map.begin()->second)))
	{
		ret = 1;
		sql_print_information("tdbctl do internal grant failed");
		goto exit;
	}

exit:
	tc_conn_free(spider_conn_map);
	tc_conn_free(remote_conn_map);
	tc_conn_free(tdbctl_conn_map);
	if (tdbctl_primary_conn != NULL)
		mysql_close(tdbctl_primary_conn);
	spider_conn_map.clear();
	remote_conn_map.clear();
	tdbctl_conn_map.clear();
	spider_ipport_set.clear();
	remote_ipport_map.clear();
	tdbctl_ipport_map.clear();
	spider_user_map.clear();
	spider_passwd_map.clear();
	remote_user_map.clear();
	remote_passwd_map.clear();
	tdbctl_user_map.clear();
	tdbctl_passwd_map.clear();
	spider_result_map.clear();
	remote_result_map.clear();
	tdbctl_result_map.clear();
	free_root(&mem_root, MYF(0));

  return ret;
}

int tc_flush_spider_routing(map<string, MYSQL*>& spider_conn_map,
  map<string, tc_exec_info>& result_map,
  map<string, string> spider_user_map,
  map<string, string> spider_passwd_map,
  bool is_force)
{
  string flush_priv_sql = "flush privileges";
  string flush_table_sql = "flush tables";
  string flush_rdlock_sql = "flush table with read lock";
  string set_mdl_timeout_sql = "set lock_wait_timeout = 60";
  string set_interactive_timeout_sql = "set wait_timeout = 180";
  string set_option_sql = set_mdl_timeout_sql + ";" + set_interactive_timeout_sql;
  string unlock_sql = "unlock tables";
  string replace_sql = dump_servers_to_sql();


  if (tc_exec_sql_paral(set_option_sql, spider_conn_map, result_map, spider_user_map, spider_passwd_map, FALSE))
  {/* return, close conn, reconnect + retry all */
    return 1;
  }

  if (!is_force)
  {
    if (tc_exec_sql_paral(flush_table_sql, spider_conn_map, result_map, spider_user_map, spider_passwd_map, FALSE) ||
      tc_exec_sql_paral(flush_rdlock_sql, spider_conn_map, result_map, spider_user_map, spider_passwd_map, FALSE))
    {/* unlock tables;
        return, close con, reconnect + retry all, */
      tc_exec_sql_paral(unlock_sql, spider_conn_map, result_map, spider_user_map, spider_passwd_map, FALSE);
      return 1;
    }
  }
  if (tc_exec_sql_paral(replace_sql, spider_conn_map, result_map, spider_user_map, spider_passwd_map, TRUE))
  {/* unlock tables; retry (--force) */
    tc_exec_sql_paral(unlock_sql, spider_conn_map, result_map, spider_user_map, spider_passwd_map, FALSE);
    /* if failed to replace mysql.servers; set changed data node read only */
    tc_set_changed_remote_read_only();
    return 2;
  }
  if (tc_exec_sql_paral(flush_priv_sql, spider_conn_map, result_map, spider_user_map, spider_passwd_map, TRUE))
  {/* unlock tables; retry (--force) retry to flush privileges */
    tc_exec_sql_paral(unlock_sql, spider_conn_map, result_map, spider_user_map, spider_passwd_map, FALSE);
    return 2;
  }
  if (!is_force)
  {
    tc_exec_sql_paral(unlock_sql, spider_conn_map, result_map, spider_user_map, spider_passwd_map, FALSE);
  }
  return 0;
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

/*
at present, CREATE/ALTER/DROP NODE also do tc_flush_routing
*/
bool tc_flush_routing(LEX* lex)
{
  int ret = 0;
  bool result = FALSE;
	bool is_force = lex->is_tc_flush_force;
  int retry_times = 3;
  map<string, MYSQL*> spider_conn_map;
  map<string, string> spider_user_map;
  map<string, string> spider_passwd_map;


  set<string>::iterator its;
  map<string, tc_exec_info> result_map;
  MEM_ROOT mem_root;
  init_sql_alloc(key_memory_servers , &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
	/* with_slave muster be false here, SPIDER_SLAVE's flush not support at present */
  set<string>  all_spider_ipport_set = get_spider_ipport_set(
                                         &mem_root,
                                         spider_user_map, 
                                         spider_passwd_map, 
                                         FALSE);

  set<string> to_flush_ipport_set;


  switch (lex->tc_flush_type)
  {
  case FLUSH_ALL_ROUTING:
    to_flush_ipport_set = all_spider_ipport_set;
    break;
  case FLUSH_ROUTING_BY_SERVER:
  {
	  string ipport = tc_get_ipport_from_server_by_wrapper(&lex->server_options, SPIDER_WRAPPER);
		/* at present, only SPIDER wrapper server do flush */
    if (!ipport.length() || !all_spider_ipport_set.count(ipport))
    {
      result = TRUE;
      goto finish;
    }
    to_flush_ipport_set.insert(ipport);
    /*TODO check ipport validate */
    break;
  }
  default:
    break;
  }

  for (its = to_flush_ipport_set.begin(); its != to_flush_ipport_set.end(); its++)
  {/* init for exec result: result_map */
    string ipport = (*its);
    tc_exec_info exec_info;
    exec_info.err_code = 0;
    exec_info.row_affect = 0;
    exec_info.err_msg = "";
    result_map.insert(pair<string, tc_exec_info>(ipport, exec_info));
  }

  while (retry_times-- > 0)
  {
    int exec_ret = 0;
		if (lex->do_grants && tc_do_grants_internal()) {
			sleep(2);
			continue;
		}
    spider_conn_map = tc_spider_conn_connect(ret, to_flush_ipport_set, spider_user_map, spider_passwd_map);
    if (ret)
    {
      result = TRUE;
      goto finish;
    }

    exec_ret = tc_flush_spider_routing(spider_conn_map, result_map, spider_user_map, spider_passwd_map, is_force);
    if (exec_ret)
    {
      tc_conn_free(spider_conn_map);
      spider_conn_map.clear();
      /* exec_ret == 2 mean "flush table with read" is ok,
      but "replace mysql.servers" or "flush privileges" failed
      so we just retry --force */
      if (exec_ret == 2)
        is_force = TRUE; /* switch force */
      sleep(2);
    }
    else
    {
      result = FALSE;
      goto finish;
    }
  }
  if (retry_times == -1)
    result = TRUE;


finish:
  tc_conn_free(spider_conn_map);
  spider_conn_map.clear();
  all_spider_ipport_set.clear();
  to_flush_ipport_set.clear();
  spider_user_map.clear();
  spider_passwd_map.clear();
  result_map.clear();
  free_root(&mem_root, MYF(0));
  return result;
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
    if (tc_check_repair_routing)
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

int tc_check_and_repair_routing()
{
  int ret = 0;
  int result = 0;
  map<string, MYSQL*> spider_conn_map;
  map<string, string> spider_user_map;
  map<string, string> spider_passwd_map;
  map<string, MYSQL*>::iterator its;
  string sql = "select Server_name,Host,Db,Username,Password,Port,Socket,Wrapper,Owner "
    "from mysql.servers order by Server_name";
  string flush_priv_sql = "flush privileges";
  string del_sql = "delete from mysql.servers";
  string replace_sql = dump_servers_to_sql();
  string repair_sql = del_sql + ";" + replace_sql + ";" + flush_priv_sql;
  FOREIGN_SERVER* server;
  list<FOREIGN_SERVER*> all_list;
  ulong records;
  MEM_ROOT mem_root;
  tc_exec_info exec_info;
  init_sql_alloc(key_memory_servers, &mem_root, ACL_ALLOC_BLOCK_SIZE, 0);
  set<string>  spider_ipport_set = get_spider_ipport_set(
                                      &mem_root, 
                                      spider_user_map, 
                                      spider_passwd_map,
                                      FALSE);

  mysql_rwlock_rdlock(&THR_LOCK_servers);
  records = servers_cache.records;
  for (ulong i = 0; i < records; i++)
  {
    if ((server = (FOREIGN_SERVER*)my_hash_element(&servers_cache, i)))
    {
      FOREIGN_SERVER* cur_server = clone_server(&mem_root, server, NULL);
      all_list.push_back(cur_server);
    }
  }
  mysql_rwlock_unlock(&THR_LOCK_servers);
  all_list.sort(server_compare);

  spider_conn_map = tc_spider_conn_connect(
                      ret, 
                      spider_ipport_set, 
                      spider_user_map, 
                      spider_passwd_map);
  if (ret)
  {
    result = 1;
    goto finish;
  }

  for (its = spider_conn_map.begin(); its != spider_conn_map.end(); its++)
  {
    string ipport = its->first;
    MYSQL* mysql = its->second;
    MYSQL_RES* res;
    time_t to_tm_time = (time_t)time((time_t*)0);
    struct tm lt;
    struct tm* l_time = localtime_r(&to_tm_time, &lt);
    res = tc_exec_sql_with_result(mysql, sql);
    if (res)
    {
      MYSQL_ROW row = NULL;
      list<FOREIGN_SERVER*> li;
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
        cur_server = clone_server(&mem_root, &tmp_server, NULL);
        li.push_back(cur_server);
      }
      li.sort(server_compare);
      if (compare_server_list(all_list, li))
      {
        fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN TDBCTL] "
          "ipport is %s, routing mismatch\n",
          l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, 
          l_time->tm_hour, l_time->tm_min, l_time->tm_sec, ipport.c_str());
        if (tc_exec_sql_up(mysql, repair_sql, &exec_info))
        {
          fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [ERROR TDBCTL] "
            "ipport is %s, routing repair failed\n",
            l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, 
            l_time->tm_hour, l_time->tm_min, l_time->tm_sec, ipport.c_str());
          result = 2;
        }
        else
        {
          fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [INFO TDBCTL] "
            "ipport is %s, routing repair succeed\n",
            l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, 
            l_time->tm_hour, l_time->tm_min, l_time->tm_sec, ipport.c_str());
        }
      }
      li.clear();
      mysql_free_result(res);
    }
    else
    {
      fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN TDBCTL] "
        "ipport is %s, routing mismatch\n",
        l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, 
        l_time->tm_hour, l_time->tm_min, l_time->tm_sec, ipport.c_str());
      if (tc_exec_sql_up(mysql, repair_sql, &exec_info))
      {
        fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [ERROR TDBCTL] "
          "ipport is %s, routing repair failed\n",
          l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, l_time->tm_hour,
          l_time->tm_min, l_time->tm_sec, ipport.c_str());
        result = 2;
      }
      else
      {
        fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [INFO TDBCTL] "
          "ipport is %s, routing repair succeed\n",
          l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, l_time->tm_hour,
          l_time->tm_min, l_time->tm_sec, ipport.c_str());
      }
    }
  }


finish:

  tc_conn_free(spider_conn_map);
  spider_conn_map.clear();
  spider_ipport_set.clear();
  spider_user_map.clear();
  spider_passwd_map.clear();
  all_list.clear();
  free_root(&mem_root, MYF(0));
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
  tmp->server_name = strdup_root(&mem_bak, server->server_name);
  tmp->host = strdup_root(&mem_bak, server->host);
  tmp->username = strdup_root(&mem_bak, server->username);
  tmp->password = strdup_root(&mem_bak, server->password);
  tmp->db = strdup_root(&mem_bak, server->db);
  tmp->scheme = strdup_root(&mem_bak, server->scheme);
  tmp->socket = strdup_root(&mem_bak, server->socket);
  tmp->owner = strdup_root(&mem_bak, server->owner);
  tmp->sport = strdup_root(&mem_bak, server->sport);
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



bool update_server_version(bool* version_updated)
{
  FOREIGN_SERVER* server_bak;
  FOREIGN_SERVER* server;
  ulong share_records = servers_cache.records;
  ulong share_records_bak = servers_cache_bak.records;
  if (share_records != share_records_bak)
  {
	  *version_updated = TRUE;
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
        strcmp(server->sport, server_bak->sport) ||
        server->port != server_bak->port)
      {/* not equal: 1.update server_v; 2.version++ */
        server_bak->version++;
        *version_updated = TRUE;
      }
      server->version = server_bak->version;
    }
  }
  DBUG_RETURN(FALSE);
}


void get_deleted_servers()
{
  FOREIGN_SERVER* server_bak;
  FOREIGN_SERVER* server;
  ulong records = servers_cache.records;
  ulong bak_records = servers_cache_bak.records;

  if (bak_records > records)
  {/* delete some servers */
    for (ulong i = 0; i < bak_records; i++)
    {
      server_bak = (FOREIGN_SERVER*)my_hash_element(&servers_cache_bak, i);
      if (!(server = (FOREIGN_SERVER*)my_hash_search(&servers_cache,
        (uchar*)server_bak->server_name, server_bak->server_name_length)))
      {/* don't exits */
        string name = server_bak->server_name;
        to_delete_servername_list.push_back(name);
      }
    }
  }
}


int get_remote_changed_servers(
  MEM_ROOT *mem_root, 
  list<FOREIGN_SERVER*> *diff_serverlist
)
{
  int ret = 0;
  int result = 0;
  map<string, MYSQL*> spider_conn_map;
  map<string, string> spider_user_map;
  map<string, string> spider_passwd_map;
  map<string, MYSQL*>::iterator its;
  string sql = "select Server_name,Host,Db,Username,Password,Port,Socket,Wrapper,Owner "
               "from mysql.servers where Wrapper = \"mysql\" order by Server_name";
  list<FOREIGN_SERVER*> new_list;
  tc_exec_info exec_info;
  const char *mysql_wrapper = "mysql";
  set<string>  spider_ipport_set = get_spider_ipport_set(
                                      mem_root, 
                                      spider_user_map, 
                                      spider_passwd_map,
                                      FALSE);


  get_server_by_wrapper(new_list, mem_root, mysql_wrapper, FALSE);
  new_list.sort(server_compare);

  spider_conn_map = tc_spider_conn_connect(
                      ret, 
                      spider_ipport_set, 
                      spider_user_map, 
                      spider_passwd_map);
  if (ret)
  {
    result = 1;
    goto finish;
  }


  for (its = spider_conn_map.begin(); its != spider_conn_map.end(); its++)
  {
    string ipport = its->first;
    MYSQL* mysql = its->second;
    MYSQL_RES* res;
    time_t to_tm_time = (time_t)time((time_t*)0);
    struct tm lt;
    struct tm* l_time = localtime_r(&to_tm_time, &lt);
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
      fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN TDBCTL] "
        "ipport is %s, routing mismatch\n",
        l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday, l_time->tm_hour,
        l_time->tm_min, l_time->tm_sec, ipport.c_str());
      result = 2;
    }
    break;
  }


finish:
  tc_conn_free(spider_conn_map);
  spider_conn_map.clear();
  spider_ipport_set.clear();
  spider_user_map.clear();
  spider_passwd_map.clear();
  new_list.clear();
  return result;
}

string get_delete_routing_sql()
{
  list<string>::iterator its;
  string sql = "";
  mysql_rwlock_rdlock(&THR_LOCK_servers);
  if (to_delete_servername_list.size() > 0)
  {
    string del_sql = "delete from mysql.servers where Server_name in(";
    string quotation = "\"";
    for (its = to_delete_servername_list.begin();
      its != to_delete_servername_list.end(); its++)
    {
      string name = *its;
      del_sql = del_sql + quotation + name + quotation;
    }
    sql = del_sql + ")";
  }
  mysql_rwlock_unlock(&THR_LOCK_servers);
  return sql;
}

void delete_redundant_routings()
{
  string del_sql = get_delete_routing_sql();
  if (del_sql.length() > 0)
  {
    int ret = 0;
    map<string, MYSQL*> spider_conn_map;
    map<string, string> spider_user_map;
    map<string, string> spider_passwd_map;
    MEM_ROOT mem_root;
    init_sql_alloc(key_memory_servers, &mem_root,  ACL_ALLOC_BLOCK_SIZE, 0);
    set<string>  spider_ipport_set = get_spider_ipport_set(
                                        &mem_root, 
                                        spider_user_map, 
                                        spider_passwd_map, 
                                        FALSE);
    string flush_priv_sql = "flush privileges";
    string sql;
    map<string, MYSQL*>::iterator its2;
    sql = del_sql + ";" + flush_priv_sql;

    spider_conn_map = tc_spider_conn_connect(
                        ret, 
                        spider_ipport_set, 
                        spider_user_map, 
                        spider_passwd_map);
    if (ret)
    {
      goto finish;
    }

    for (its2 = spider_conn_map.begin(); its2 != spider_conn_map.end(); its2++)
    {
      string ipport = its2->first;
      MYSQL* mysql = its2->second;
      tc_exec_info exec_info;
      tc_exec_sql_without_result(mysql, sql, &exec_info);
    }

  finish:
    tc_conn_free(spider_conn_map);
    spider_conn_map.clear();
    spider_ipport_set.clear();
    spider_user_map.clear();
    spider_passwd_map.clear();
    to_delete_servername_list.clear();
    free_root(&mem_root, MYF(0));
  }
}

