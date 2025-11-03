#ifndef TC_DUMP_INCLUDED
#define TC_DUMP_INCLUDED


#include "my_global.h"
#include "my_sys.h"
#include "sql_class.h"
#include <cstdio>
#include <string>
#include <unordered_set>
#include <functional>


class Query_exception : public std::exception {
public:
    Query_exception(const char *query, MYSQL *mysql): std::exception() {
      snprintf(err_buff, sizeof(err_buff), "Couldn't execute '%s': %s (%d)", 
              query, mysql_error(mysql), mysql_errno(mysql));
    }
    
    const char* what() const noexcept override {
      return err_buff;
    }
private:
    char err_buff[1024];
};

class DB_exception : public std::exception {
public:
    DB_exception(MYSQL *mysql, const char* when): std::exception() {
      snprintf(err_buff, sizeof(err_buff), "Got error: %d: %s %s",
          mysql_errno(mysql), mysql_error(mysql), when);
    }
    
    const char* what() const noexcept override {
      return err_buff;
    }
private:
    char err_buff[1024];
};

class IO_exception : public std::exception {
public:
    IO_exception(): std::exception() {
      snprintf(err_buff, sizeof(err_buff), "Got errno %d on write",
          errno);
    }
    
    const char* what() const noexcept override {
      return err_buff;
    }
private:
    char err_buff[1024];
};

class DStr_exception : public std::exception {
public:
    DStr_exception(const char *msg): std::exception() {
      snprintf(err_buff, sizeof(err_buff), "%s", msg);
    }
    
    const char* what() const noexcept override {
      return err_buff;
    }
private:
    char err_buff[1024];
};

class TC_Dump_exception : public std::exception {
public:
    TC_Dump_exception(const char *msg): std::exception() {
      snprintf(err_buff, sizeof(err_buff), "%s", msg);
    }
    
    const char* what() const noexcept override {
      return err_buff;
    }
private:
    char err_buff[1024];
};

void mysql_free_result_with_set_null(MYSQL_RES *&res);

/**
 * RAII wrapper for managing MYSQL_RES resources.
 * Automatically frees the MYSQL_RES pointer when the MYSQL_RES_Mgr object goes out of scope.
 * 
 * @warning The constructor takes a reference to a MYSQL_RES pointer. Ensure the pointer is valid
 *          during the lifetime of the MYSQL_RES_Mgr object, otherwise behavior is undefined.
 * @note This class does not handle NULL pointers explicitly. If the MYSQL_RES pointer is NULL,
 *       the destructor will not perform any action.
 */
class MYSQL_RES_Mgr
{
  public:
    MYSQL_RES_Mgr(MYSQL_RES *&res): res(res) {}
    ~MYSQL_RES_Mgr() { mysql_free_result_with_set_null(res); }
  private:
    MYSQL_RES *&res;
};

class Dump_Handler
{
public:
  enum enum_set_gtid_purged_mode {
    SET_GTID_PURGED_OFF= 0,
    SET_GTID_PURGED_AUTO= 1,
    SET_GTID_PURGED_ON= 2
  };

  typedef enum {
    KEY_TYPE_NONE,
    KEY_TYPE_PRIMARY,
    KEY_TYPE_UNIQUE,
    KEY_TYPE_NON_UNIQUE
  } key_type_t;

private:
  THD *thd;
  const FOREIGN_SERVER * const target_server;

  const std::string result_file_name;
  const std::string log_error_file_name;
  FILE *result_file;
  FILE *error_file;

  MYSQL mysql_connection, *mysql = NULL;

  bool seen_views = false;
  char *order_by = NULL;
  bool server_supports_switching_charsets = true;
  bool insert_pat_inited = false;
  DYNAMIC_STRING insert_pat;
  LIST *skipped_keys_list = NULL;
  HASH processed_compression_dictionaries;

  std::string default_charset;
  CHARSET_INFO *charset_info;
  ulong opt_max_allowed_packet = 24*1024*1024;
  ulong opt_net_buffer_length = 1024*1024L-1025;

  /* The default value of opt_alldbs differs from 
     that of mysqldump for the current usage environment. */
  bool opt_alldbs = true;
  /* The default value of opt_no_data differs from 
     that of mysqldump for the current usage environment. */
  bool opt_no_data = true;
  std::unordered_set<std::string> ignore_databases;
  std::unordered_set<std::string> ignore_tables;
  bool opt_databases = false;
  bool opt_dump_triggers = true;
  bool opt_routines = false;
  bool opt_events = false;
  bool opt_alltspcs = false;
  bool opt_notspcs = false;
  bool opt_autocommit = false;

  char compatible_mode_normal_str[255];

  bool opt_print_tc_admin_info = false;
  bool opt_tz_utc = true;
  bool opt_set_charset = true;
  bool opt_lock = true;
  bool opt_quick = true;
  bool opt_create_options = true;
  bool opt_extended_insert = true;
  bool opt_disable_keys = true;

  bool opt_single_transaction = false;
  bool opt_lock_for_backup = false;
  bool opt_lock_all_tables = false;
  bool opt_lock_tables = true;
  bool opt_flush_logs = false;

  bool opt_verbose = false;
  bool opt_quoted = true;
  bool opt_drop_database = false;
  bool opt_drop_table = true;
  bool opt_drop_trigger = false;
  bool opt_add_not_exists = false;
  bool opt_flush_privileges = false;
  bool opt_no_create_info = false;

  my_bool is_binlog_disabled = FALSE;
  int checked_ndbinfo= 0;
  int have_ndbinfo= 0;

  enum enum_set_gtid_purged_mode 
    opt_set_gtid_purged_mode = SET_GTID_PURGED_AUTO;

  class DUMP_MYSQL_GUARD {
  public:
    DUMP_MYSQL_GUARD(Dump_Handler *dhdl) : dhdl(dhdl) {}
    ~DUMP_MYSQL_GUARD() { 
      if(dhdl)
        dhdl->dbDisconnect(); 
    }
  private:
    Dump_Handler *dhdl;
  };

  class DUMP_FILE_GUARD {
  public:
    DUMP_FILE_GUARD(FILE *file) : file(file) {}
    ~DUMP_FILE_GUARD() { 
      if (file && file != stdout)
        my_fclose(file, MYF(0));
    }
  private:
    FILE *file;
  };

public:
  Dump_Handler(THD *thd, 
               const FOREIGN_SERVER * const target_server,
               const std::string &result_file,
               const std::string &log_error_file)
  : thd(thd),
  target_server(target_server),
  result_file_name(result_file),
  log_error_file_name(log_error_file),
  result_file(NULL),
  error_file(NULL),
  default_charset(MYSQL_UNIVERSAL_CLIENT_CHARSET),
  charset_info(&my_charset_latin1)
  {
    compatible_mode_normal_str[0]= 0;
  }

  ~Dump_Handler() {}

  bool dump_schema_main(std::string &errmsg) noexcept;

  void set_default_charset(const std::string &charset) {
    default_charset = charset;
  }
  void set_opt_print_tc_admin_info(bool opt_print_tc_admin) {
    opt_print_tc_admin_info = opt_print_tc_admin;
  }
  void set_skip_optimization() {
    opt_extended_insert = false;
    opt_drop_table = false;
    opt_lock = false;
    opt_quick = false;
    opt_create_options = false;
    opt_disable_keys = false;
    opt_lock_tables = false;
    opt_set_charset = false;
  }
  void set_opt_single_transaction(bool opt_single_transaction) {
    this->opt_single_transaction = opt_single_transaction;
  }
  void set_opt_lock_for_backup(bool opt_lock_for_backup) {
    this->opt_lock_for_backup = opt_lock_for_backup;
  }
  void set_opt_lock_all_tables(bool opt_lock_all_tables) {
    this->opt_lock_all_tables = opt_lock_all_tables;
  }
  void set_opt_set_gtid_purged_mode(
        enum enum_set_gtid_purged_mode mode) {
    opt_set_gtid_purged_mode = mode;
  }
  void set_opt_routines(bool opt_routines) {
    this->opt_routines = opt_routines;
  }
  void set_ignore_database(const std::string &dbname) {
    ignore_databases.insert(dbname);
  }
  void set_ignore_table(const std::string &tablename) {
    ignore_tables.insert(tablename);
  }
  void set_opt_drop_database(bool opt_drop_database) {
    this->opt_drop_database = opt_drop_database;
  }
  void set_opt_quick(bool opt_quick) {
    this->opt_quick = opt_quick;
  }
  void set_opt_create_options(bool opt_create_options) {
    this->opt_create_options = opt_create_options;
  }
  void set_opt_autocommit(bool opt_autocommit) {
    this->opt_autocommit = opt_autocommit;
  }



private:
  bool check_options(std::string &errmsg);
  bool connect_to_server();
  void set_session_tc_admin();
  void print_comment(const char *format, ...);
  void verbose_msg(const char *fmt, ...);
  void print_error(const char *fmt, ...);
  void write_header();
  void write_footer();
  bool server_supports_backup_locks();
  int start_transaction(MYSQL *mysql_con);
  bool process_set_gtid_purged(MYSQL* mysql_con);
  void set_session_binlog(my_bool flag);
  bool add_set_gtid_purged(MYSQL *mysql_con);
  void dbDisconnect();
  int dump_tablespaces(char* ts_where);
  int dump_all_tablespaces();
  int is_ndbinfo(const char* dbname);
  bool include_database(const char *dbname, size_t dbname_len);
  bool include_table(const char *tbname, size_t tbname_len);
  my_bool test_if_special_chars(const char *str);
  char * quote_name(const char *name, char *buff, my_bool force);
  char * unquote_name(const char *opt_quoted_name, char *buff);
  void unescape(FILE *file,char *pos, size_t length);
  int init_dumping(const char *database, 
                   const std::function<int(char *)> &init_func);
  int init_dumping_tables(char *qdatabase);
  char check_if_ignore_table(const char *table_name, char *table_type);
  my_bool has_primary_key(const char *table_name);
  char *primary_key_fields(const char *table_name, const my_bool desc);
  int switch_character_set_results(const char *cs_name);
  inline my_bool general_log_or_slow_log_tables(const char *db,
                                                const char *table) {
  return (!my_strcasecmp(charset_info, db, "mysql")) &&
          (!my_strcasecmp(charset_info, table, "general_log") ||
           !my_strcasecmp(charset_info, table, "slow_log"));
  }
  inline my_bool replication_metadata_tables(const char *db,
                                             const char *table) {
  return (!my_strcasecmp(charset_info, db, "mysql")) &&
          (!my_strcasecmp(charset_info, table, "slave_master_info") ||
           !my_strcasecmp(charset_info, table, "slave_relay_log_info"));
  }
  void print_optional_drop_table(FILE *sql_file, const char* db,
                                const char *table,
                                const char *opt_quoted_table);
  LIST * find_matching_skipped_key(const char *id_from,
                                  const char *id_to);
  my_bool contains_autoinc_column(const char *autoinc_column,
                                  const char *keydef,
                                  key_type_t type);
  void skip_secondary_keys(char *create_str, my_bool has_pk);
  void skip_compressed_columns(char *create_str, LIST **dictionaries);
  void print_optional_create_compression_dictionary(FILE* sql_file,
                                          const char *dictionary_name);
  char *getTableName(int reset);
  void print_value(FILE *file, MYSQL_RES  *result, MYSQL_ROW row,
                    const char *prefix, const char *name,
                    int string_value);
  uint get_table_structure(char *table, char *db, char *table_type,
                          char *ignore_flag, my_bool real_columns[]);
  void dump_skipped_keys(const char *table);
  void restore_secondary_keys(char *table);
  void print_blob_as_hex(FILE *output_file, const char *str, ulong len);
  void dump_table(char *table, char *db);
  int fetch_db_collation(const char *db_name,
                        char *db_cl_name,
                        int db_cl_size);
  void dump_trigger_old(FILE *sql_file, MYSQL_RES *show_triggers_rs,
                        MYSQL_ROW *show_trigger_row,
                        const char *table_name);
  int switch_db_collation(FILE *sql_file,
                          const char *db_name,
                          const char *delimiter,
                          const char *current_db_cl_name,
                          const char *required_db_cl_name,
                          int *db_cl_altered);
  int restore_db_collation(FILE *sql_file,
                          const char *db_name,
                          const char *delimiter,
                          const char *db_cl_name);
  void switch_cs_variables(FILE *sql_file,
                          const char *delimiter,
                          const char *character_set_client,
                          const char *character_set_results,
                          const char *collation_connection);
  void restore_cs_variables(FILE *sql_file, const char *delimiter);
  void switch_sql_mode(FILE *sql_file,
                      const char *delimiter,
                      const char *sql_mode);
  void restore_sql_mode(FILE *sql_file, const char *delimiter);
  void switch_time_zone(FILE *sql_file,
                        const char *delimiter,
                        const char *time_zone);
  void restore_time_zone(FILE *sql_file, const char *delimiter);
  int dump_trigger(FILE *sql_file, MYSQL_RES *show_create_trigger_rs,
                   const char *db_name,
                   const char *db_cl_name);
  int dump_triggers_for_table(char *table_name, char *db_name);
  uint dump_events_for_db(char *db);
  uint dump_routines_for_db(char *db);
  int dump_all_tables_in_db(char *database);
  int init_dumping_views(char *qdatabase);
  my_bool get_view_structure(char *table, char* db);
  my_bool dump_all_views_in_db(char *database);
  int dump_all_databases();
  void free_resources();

public:
  static bool tc_dump_node_schema(THD *thd, 
                         const FOREIGN_SERVER * const target_server,
                         const std::string &result_file,
                         const std::string &log_error_file);
};



/*
  The functions migrated from libmysql.c
*/

void STDCALL client_mysql_data_seek(MYSQL_RES *result, my_ulonglong row);
MYSQL_FIELD_OFFSET STDCALL client_mysql_field_seek(MYSQL_RES *result, MYSQL_FIELD_OFFSET field_offset);
MYSQL_RES * STDCALL client_mysql_list_tables(MYSQL *mysql, const char *wild);
ulong STDCALL client_mysql_real_escape_string_quote(MYSQL *mysql, char *to, const char *from, ulong length, char quote);
MYSQL_FIELD * STDCALL client_mysql_fetch_field(MYSQL_RES *result);
MYSQL_FIELD * STDCALL client_mysql_fetch_field_direct(MYSQL_RES *res, uint fieldnr);
int STDCALL client_mysql_refresh(MYSQL *mysql, uint options);
MYSQL_RES * STDCALL client_mysql_use_result(MYSQL *mysql);
const char *STDCALL client_mysql_sqlstate(MYSQL *mysql);

#endif