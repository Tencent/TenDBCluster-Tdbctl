/*
  Copyright (C) 2020 THL A29 Limited, a Tencent company. All rights reserved.
*/
#include <map>
#include <string>
#include <list>
#include <unordered_set>

#include "auth_common.h"
#include "sql_class.h"
#include "sql_db.h"
#include "sql_show.h" // IS_COLUMNS_* indices

#include "tc_base.h"
#include "log.h" 

using std::string;
using std::map;
using std::unordered_set;
using std::list;


/* Select all fields for scalability concerns */
#define SQL_SELECT_IS_COLUMNS                                                  \
  "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' AND "      \
  "TABLE_NAME='%s'"
#define SQL_SELECT_IS_TABLES                                                   \
  "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' AND "       \
  "TABLE_NAME='%s'"

/* Select specific fields for time/space efficiency considerations */
#define SQL_SELECT_IS_COLUMNS_PARTIAL                                          \
  "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, "           \
  "DATA_TYPE, COLLATION_NAME, COLUMN_KEY "                                     \
  "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'"
#define SQL_SELECT_IS_TABLES_PARTIAL                                           \
  "SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, TABLE_COLLATION "                  \
  "FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'"

#define IS_COLUMNS_PARTIAL_TABLE_SCHEMA         0
#define IS_COLUMNS_PARTIAL_TABLE_NAME           1
#define IS_COLUMNS_PARTIAL_COLUMN_NAME          2
#define IS_COLUMNS_PARTIAL_ORDINAL_POSITION     3
#define IS_COLUMNS_PARTIAL_DATA_TYPE            4
#define IS_COLUMNS_PARTIAL_COLLATION_NAME       5
#define IS_COLUMNS_PARTIAL_COLUMN_KEY           6

#define IS_TABLES_PARTIAL_TABLE_SCHEMA          0
#define IS_TABLES_PARTIAL_TABLE_NAME            1
#define IS_TABLES_PARTIAL_ENGINE                2
#define IS_TABLES_PARTIAL_TABLE_COLLATION       3

/* Get table names for multi-table checks */
#define SQL_GET_ALL_TABLES_IN_DB                                               \
  "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s'"
#define SQL_GET_TABLES_IN_DB_LIKE                                              \
  "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' "  \
  "AND TABLE_NAME LIKE '%s'"

#define IS_TABLES_TABLE_SCHEMA          1
#define IS_TABLES_TABLE_NAME            2
#define IS_TABLES_TABLE_ENGINE          4
#define IS_TABLES_TABLE_COLLATION      17

#define EMPTY_STRING std::string()
#define DATA_STRING(A, B, C) ((A)[(C)] ? std::string((A)[(C)], (B)[(C)]) : EMPTY_STRING)

#define COLUMN_DIFF_COLUMN_NAME           (1 << 0)
#define COLUMN_DIFF_ORDINAL_POSITION      (1 << 1)
#define COLUMN_DIFF_DATA_TYPE             (1 << 2)
#define COLUMN_DIFF_COLUMN_LENGTH         (1 << 3)
#define COLUMN_DIFF_COLLATION             (1 << 4)
#define COLUMN_DIFF_COLUMN_KEY            (1 << 5)

#define MAX_NUM_CHECK_THREADS 20UL

struct Column_record;
struct Table_info;
struct Message_writer;
class Check_tables_handling;

typedef std::map<std::string, Column_record> Column_records;

static std::string fix_db_name(const std::string &db_name,
                               const std::string &server_name,
                               enum_node_type node_type);
static bool get_column_records_for_server(THD *thd, const std::string &db_name,
                                          const std::string &table_name,
                                          const std::string &server_name,
                                          MYSQL *mysql, Column_records &records,
                                          enum_node_type node_type);
static bool get_table_info_for_server(THD *thd, const std::string &db_name,
                                      const std::string &table_name,
                                      const std::string &server_name,
                                      MYSQL *mysql, Table_info &info,
                                      enum_node_type node_type);
static bool fill_tables_list(MYSQL *conn, const char *db, String *wild,
                            list<std::string> &tables);
static bool do_send_metadata(THD *thd);
static bool do_check_one_table(THD *thd, Cluster_conn_manager *conn_mgr,
                               const string &db_name, const string &table_name, bool do_send);
static bool validate_db_grants(THD *thd, const char *db);

static void *query_table_and_column_info_thread(void *arg);
static bool do_check_one_table_by_parallel_query(THD *thd, Cluster_conn_manager *conn_mgr,
                                                 Check_tables_handling *check_tables_hdl,
                                                 const string &db_name, const string &table_name,
                                                 bool do_send); 
static bool check_redundant_tables(THD *thd, Cluster_conn_manager *conn_mgr, 
                                   string db_name, String *wild,
                                   const list<string> &my_table_list, bool do_send);
static bool get_table_cache(THD *thd, Cluster_conn_manager *conn_mgr, 
                            map<string, string> &open_cache_map, 
                            map<string, string> &def_cache_map);
static bool set_table_cache(THD *thd, Cluster_conn_manager *conn_mgr, 
                            const map<string, string> &open_cache_map, 
                            const map<string, string> &def_cache_map);
static bool set_table_cache(THD *thd, Cluster_conn_manager *conn_mgr, 
                            const string &open_cache_val, 
                            const string &def_cache_val);
bool compare_table_info(const Table_info &expected, const Table_info &actual, 
                        enum_node_type node_type, Message_writer &writer);
bool compare_columns_info(const Column_records &expected, const Column_records &actual, 
                          enum_node_type node_type, Message_writer &writer);

static const int max_message_length = 512;

/*
  Only store some selected fields that are considered important.
*/
struct Column_record {
  string table_schema;
  string table_name;
  string column_name;
  ulong ordinal_position;
  string data_type;
  string collation_name;
  string column_key;
};

struct Table_info {
  bool exists;
  string table_schema;
  string table_name;
  string table_engine;
  string table_collation;
};

static ulonglong column_record_cmp(const Column_record &a,
                                   const Column_record &b) {
  ulonglong ret = 0;
  if (a.column_name != b.column_name)
    ret |= COLUMN_DIFF_COLUMN_NAME;
  if (a.ordinal_position != b.ordinal_position)
    ret |= COLUMN_DIFF_ORDINAL_POSITION;
  if (a.data_type != b.data_type)
    ret |= COLUMN_DIFF_DATA_TYPE;
  if (a.collation_name != b.collation_name)
    ret |= COLUMN_DIFF_COLLATION;
  if (a.column_key != b.column_key)
    ret |= COLUMN_DIFF_COLUMN_KEY;
  return ret;
}

#define write_column_diff_error(W, C, F, ...)                                     \
  do {                                                                         \
    (W).write(Message_writer::MSG_LEVEL_ERROR, "Column '%s': " F, C, __VA_ARGS__); \
  } while (0)

struct Message_writer {
  enum enum_msg_level {
    MSG_LEVEL_INFO = 0,
    MSG_LEVEL_WARN = 1,
    MSG_LEVEL_ERROR = 2
  };

  Message_writer() : message_str(), idx(0), error(false),
                     separator(" ") {}

  void write(enum_msg_level level, const char *fmt, ...) {
    char buff[512];
    va_list args;

    if (level == MSG_LEVEL_ERROR) error = true;

    va_start(args, fmt);
    vsnprintf(buff, sizeof(buff), fmt, args);
    va_end(args);

    if(idx > 0) {
      message_str.append(separator);
    }

    message_str.append("(");
    message_str.append(std::to_string(++idx));
    message_str.append(")");
    message_str.append(buff);
  }

  const char *raw_str() const { return message_str.c_str(); }
  size_t length() const { return message_str.length(); }
  bool has_error() const { return error; }

private:
  string message_str;
  int idx;
  bool error;
  string separator;
};

class Table_MDL_lock_guard {
public:
  Table_MDL_lock_guard(THD *thd, const std::string &db,
                       const std::string &table)
      : m_thd(thd) {
    string name = db + "#" + table;

    MDL_REQUEST_INIT(&mdl_request, MDL_key::USER_LEVEL_LOCK, "", name.c_str(),
                     MDL_SHARED, MDL_EXPLICIT);
    locked = !m_thd->mdl_context.acquire_lock(&mdl_request,
                                              thd->variables.lock_wait_timeout);
  }

  ~Table_MDL_lock_guard() {
    if (locked) {
      // m_thd->mdl_context.release_all_locks_for_name(mdl_request.ticket);
      m_thd->mdl_context.release_lock(mdl_request.ticket);
    }
  }

  bool lock_successful() const { return locked; }

private:
  Table_MDL_lock_guard() {}

  bool locked;

  THD *m_thd;

  MDL_request mdl_request;
};

/*
  Enumeration type for identifying the completion status of 
  query job.
*/
enum Query_job_status {
  QUERY_JOB_NOT_STARTED = 0,  // The query job is not started.
  QUERY_JOB_IN_PROCESS,       // The query job is in progress.
  QUERY_JOB_COMPLETED,        // The query job is completed.
  QUERY_TABLE_FAILED,         // The query job is failed when querying table info.
  QUERY_COLUMN_FAILED,        // The query job is failed when querying column info.
};


/*
  Class for checking table structure consistency through 
  concurrent queries of metadata across multiple cluster nodes.
*/
class Check_tables_handling
{
  friend void *query_table_and_column_info_thread(void *arg);
public:
  Check_tables_handling(THD *thd): m_thd(thd),  
                                   m_query_table_mgr(thd), 
                                   m_query_column_mgr(thd) {
    DBUG_ASSERT(m_thd != NULL);
    /*
      Prepare resources required for concurrent queries.
    */
    Cluster_conn_manager *conn_mgr = m_thd->cluster_conn_manager;

    string my_server = conn_mgr->get_my_server_name();
    m_nodes_to_check = {{my_server, Query_job_status::QUERY_JOB_COMPLETED}};
    m_nodes_type = {{my_server, NODE_TYPE_CTL}};

    for(enum_node_type node_type: {NODE_TYPE_SPIDER, NODE_TYPE_REMOTE}) {
      const map<string, MYSQL *> &conns_map = conn_mgr->get_conn_map(node_type);
      for(const std::pair<const std::string, MYSQL *> &conn_item: conns_map) {
        m_nodes_to_check.insert({conn_item.first, Query_job_status::QUERY_JOB_COMPLETED});
        m_nodes_type.insert({conn_item.first, node_type});
      }
    }

    m_num_threads = min(m_nodes_to_check.size(), MAX_NUM_CHECK_THREADS);
    m_need_stop = false;

    /*
      Prepare mutexes and semaphores required for concurrent queries.
    */
    mysql_mutex_init(PSI_NOT_INSTRUMENTED, &m_lock_nodes_to_check, MY_MUTEX_INIT_FAST);
    mysql_mutex_init(PSI_NOT_INSTRUMENTED, &m_lock_stop_flag, MY_MUTEX_INIT_FAST);
    mysql_cond_init(PSI_NOT_INSTRUMENTED, &m_cond_query_finish);
    mysql_cond_init(PSI_NOT_INSTRUMENTED, &m_cond_new_query);
  }

  ~Check_tables_handling() {
    /*
      Ensure that child threads terminate properly and release all resources.
    */
    mysql_mutex_destroy(&m_lock_nodes_to_check);
    mysql_mutex_destroy(&m_lock_stop_flag);
    mysql_cond_destroy(&m_cond_query_finish);
    mysql_cond_destroy(&m_cond_new_query);
    free_query_result();
  }

  /*
    Launch sub-threads for concurrent queries.

    Return:
      true, Error
      false, Success
  */
  bool launch_parallel_query_threads() {
    set_stop_flag(false);
    for (int i = 0; i < m_num_threads; ++i)
    {
      my_thread_handle thr_handle;
      my_thread_attr_t thr_attr;
      my_thread_attr_init(&thr_attr);
      my_thread_attr_setdetachstate(&thr_attr, MY_THREAD_CREATE_DETACHED);
      if (mysql_thread_create(PSI_NOT_INSTRUMENTED, &thr_handle, &thr_attr,
                          query_table_and_column_info_thread, (void *)this)) {
        terminate_parallel_query_threads();
        return true;
      }
      my_thread_attr_destroy(&thr_attr);
    }
    return false;
  }

  /*
    Specify the next database and table name to query and 
    prepare for concurrent execution.
  */
  void prepare_for_next_query(const string &database_name, const string &table_name) {
    free_query_result();
    m_query_table_mgr.build_server_maps(m_thd->cluster_conn_manager);
    m_query_table_mgr.reset_error();
    m_query_column_mgr.build_server_maps(m_thd->cluster_conn_manager);
    m_query_column_mgr.reset_error();
    m_schema_to_check = database_name;
    m_table_to_check = table_name;
  }

  /*
    Wait the completion of query jobs across all querying nodes.
  */
  void wait_parallel_query_to_finish() {
    mysql_mutex_lock(&m_lock_nodes_to_check);
    for(const std::pair<std::string, Query_job_status> &one_node: m_nodes_to_check) {
      m_nodes_to_check[one_node.first] = Query_job_status::QUERY_JOB_NOT_STARTED;
    }
    mysql_cond_broadcast(&m_cond_new_query);
    mysql_mutex_unlock(&m_lock_nodes_to_check);

    mysql_mutex_lock(&m_lock_nodes_to_check);
    while(!is_parallel_query_finished(false)) {
      mysql_cond_wait(&m_cond_query_finish, &m_lock_nodes_to_check);
    }
    mysql_mutex_unlock(&m_lock_nodes_to_check);
  }

  /*
    Terminate sub-threads for concurrent queries.
  */
  void terminate_parallel_query_threads() {
    mysql_mutex_lock(&m_lock_nodes_to_check);
    set_stop_flag(true);
    mysql_cond_broadcast(&m_cond_new_query);
    mysql_mutex_unlock(&m_lock_nodes_to_check);
  }

  /*
    Get the queried table information.
    If the query fails, return the corresponding error code and error message.
  */
  bool get_table_info(const string &server_name, enum_node_type node_type, 
                      Table_info &table_info, uint &err_code, string &err_msg) {
    tc_exec_info exec_info;
    if(m_query_table_mgr.get_exec_info(server_name, exec_info, node_type)) {
      err_code = 1;
      err_msg = "Unknown " + string(get_wrapper_name_by_node_type(node_type))
                + " node: " + server_name; 
      return true;
    }

    err_code = exec_info.err_code;
    if(err_code) {
      err_msg = exec_info.err_msg;
      return true;
    }

    MYSQL_ROW row;
    table_info.exists = false;
    if ((row = mysql_fetch_row(exec_info.res))) {
      table_info.exists = true;
      table_info.table_schema = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                            IS_TABLES_PARTIAL_TABLE_SCHEMA);
      table_info.table_name = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                          IS_TABLES_PARTIAL_TABLE_NAME);
      table_info.table_engine = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                            IS_TABLES_PARTIAL_ENGINE);
      table_info.table_collation = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                               IS_TABLES_PARTIAL_TABLE_COLLATION);
    }
    return false;
  }

  /*
    Get the queried column information.
    If the query fails, return the corresponding error code and error message.
  */
  bool get_column_records(const string &server_name, enum_node_type node_type, 
                          Column_records &column_records, uint &err_code, string &err_msg) {
    tc_exec_info exec_info;
    if(m_query_column_mgr.get_exec_info(server_name, exec_info, node_type)) {
      err_code = 1;
      err_msg = "Unknown " + string(get_wrapper_name_by_node_type(node_type))
                + " node: " + server_name; 
      return true;
    }

    err_code = exec_info.err_code;
    if(err_code) {
      err_msg = exec_info.err_msg;
      return true;
    }

    MYSQL_ROW row;
    column_records.clear();
    while ((row = mysql_fetch_row(exec_info.res))) {
      Column_record rec;
      rec.table_schema = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                     IS_COLUMNS_PARTIAL_TABLE_SCHEMA);
      rec.table_name = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                   IS_COLUMNS_PARTIAL_TABLE_NAME);
      rec.column_name = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                    IS_COLUMNS_PARTIAL_COLUMN_NAME);
      rec.ordinal_position = strtoul(row[IS_COLUMNS_PARTIAL_ORDINAL_POSITION], NULL, 10);
      rec.data_type = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                  IS_COLUMNS_PARTIAL_DATA_TYPE);
      rec.collation_name = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                       IS_COLUMNS_PARTIAL_COLLATION_NAME);
      rec.column_key = DATA_STRING(row, mysql_fetch_lengths(exec_info.res), 
                                   IS_COLUMNS_PARTIAL_COLUMN_KEY);
      column_records[rec.column_name] = rec;
    }
    return false;
  }

private:

  void free_query_result() {
    tc_exec_info exec_info;
    for(const std::pair<string, enum_node_type> &one_node: m_nodes_type) {
      for(Query_exec_manager *query_mgr: {&m_query_table_mgr, &m_query_column_mgr}) {
        if(!query_mgr->get_exec_info(one_node.first, exec_info, one_node.second)) {
          if(exec_info.res) {
            mysql_free_result(exec_info.res);
            exec_info.res = NULL;
          }
        }
      }
    }
    return;
  }

  bool is_parallel_query_finished(bool need_lock=true) {
    bool is_finished = true;
    if(need_lock)
      mysql_mutex_lock(&m_lock_nodes_to_check);
    for(const std::pair<std::string, Query_job_status> &one_node: m_nodes_to_check) {
      if(one_node.second == Query_job_status::QUERY_JOB_NOT_STARTED ||
         one_node.second == Query_job_status::QUERY_JOB_IN_PROCESS) {
        is_finished = false;
        break;
      }
    }
    if(need_lock)
      mysql_mutex_unlock(&m_lock_nodes_to_check);
    return is_finished;
  }

  bool get_unstarted_node(string &server_name, bool need_lock=true) {
    bool has_unfinished_node = false;
    if(need_lock)
      mysql_mutex_lock(&m_lock_nodes_to_check);
    for(const std::pair<std::string, Query_job_status> one_node: m_nodes_to_check) {
      if(one_node.second == Query_job_status::QUERY_JOB_NOT_STARTED) {
        has_unfinished_node = true;
        server_name = one_node.first;
        break;
      }
    }
    if(need_lock)
      mysql_mutex_unlock(&m_lock_nodes_to_check);
    return has_unfinished_node;
  }

  bool need_stop_query() {
    bool res;
    mysql_mutex_lock(&m_lock_stop_flag);
    res = m_need_stop;
    mysql_mutex_unlock(&m_lock_stop_flag);
    return res;
  }

  void set_stop_flag(bool flag) {
    mysql_mutex_lock(&m_lock_stop_flag);
    m_need_stop = flag;
    mysql_mutex_unlock(&m_lock_stop_flag);
  }
  
  /*
    Actual Implementation of Worker Functions for Parallel Querying Threads
  */
  void query_table_and_column_info_worker() {
    sql_print_information("Checking tables worker %lu is running", my_thread_self());
    string server_name;
    while(true) {
      /*
        Get a server name to query table and column info
      */
      mysql_mutex_lock(&m_lock_nodes_to_check);
      /* Thread needs to terminate */ 
      if(need_stop_query()) {  
        mysql_mutex_unlock(&m_lock_nodes_to_check);
        sql_print_information("Checking tables worker %lu is terminated", my_thread_self());
        return;
      }
      while(!get_unstarted_node(server_name, false)) {
        mysql_cond_wait(&m_cond_new_query, &m_lock_nodes_to_check);

        /* Thread needs to terminate */ 
        if(need_stop_query()) {  
          mysql_mutex_unlock(&m_lock_nodes_to_check);
          sql_print_information("Checking tables worker %lu is terminated", my_thread_self());
          return;
        }
      }
      m_nodes_to_check[server_name] = Query_job_status::QUERY_JOB_IN_PROCESS;
      mysql_mutex_unlock(&m_lock_nodes_to_check);

      /*
        Do the query job
      */
      Query_job_status job_status = do_query_table_and_column_info(server_name, 
                                            m_nodes_type[server_name], 
                                            m_schema_to_check, m_table_to_check);

      /*
        Signal the main thread that the current query task is complete.
      */
      mysql_mutex_lock(&m_lock_nodes_to_check);
      m_nodes_to_check[server_name] = job_status;
      mysql_mutex_unlock(&m_lock_nodes_to_check);
      mysql_cond_signal(&m_cond_query_finish);
    }
  }

  /*
    Function that executes the actual SQL query. Ensure this function is thread-safe.

    Return:
      true, success
      false, error
  */
  Query_job_status do_query_table_and_column_info(const string &server_name,
                                                  enum_node_type node_type, 
                                                  const string &db_name,
                                                  const string &table_name) {
    MYSQL *mysql = m_thd->cluster_conn_manager->get_conn_map(node_type).at(server_name);
    tc_exec_info exec_info;
    char query_buff[512];

    /* For Remotes, add a numeric suffix according to their server_name */
    string fixed_db_name = fix_db_name(db_name, server_name, node_type);

    /* select specific fields of information_schema.tables */
    snprintf(query_buff, sizeof(query_buff), SQL_SELECT_IS_TABLES_PARTIAL,
            fixed_db_name.c_str(), table_name.c_str());

    m_query_table_mgr.store_exec_query(server_name, string(query_buff), node_type);
    tc_get_query_result(&m_query_table_mgr, server_name, mysql, node_type);

    m_query_table_mgr.get_exec_info(server_name, exec_info, node_type);
    if(exec_info.err_code) {
      return Query_job_status::QUERY_TABLE_FAILED;
    }

    /* select specific fields of information_schema.columns */
    snprintf(query_buff, sizeof(query_buff), SQL_SELECT_IS_COLUMNS_PARTIAL,
            fixed_db_name.c_str(), table_name.c_str());

    m_query_column_mgr.store_exec_query(server_name, string(query_buff), node_type);
    tc_get_query_result(&m_query_column_mgr, server_name, mysql, node_type);

    m_query_column_mgr.get_exec_info(server_name, exec_info, node_type);
    if(exec_info.err_code) {
      return Query_job_status::QUERY_COLUMN_FAILED;
    }

    return Query_job_status::QUERY_JOB_COMPLETED;
  }

private:
  THD *m_thd;
  int m_num_threads;  // Number of concurrent query threads

  /*
    Mutexes and semaphores required for concurrent queries.
  */
  mysql_cond_t m_cond_query_finish, m_cond_new_query;
  mysql_mutex_t m_lock_nodes_to_check;
  mysql_mutex_t m_lock_stop_flag;
  

  /*
    Common resources
  */

  /* Query manager for table or column info queries across multiple nodes */
  Query_exec_manager m_query_table_mgr, m_query_column_mgr;

  /* Store the nodes and their query status for concurrent queries */
  map<string, Query_job_status> m_nodes_to_check;

  /* Store the nodes and their types for concurrent queries */
  map<string, enum_node_type> m_nodes_type;  

  /* Name of the database or table to be queried */
  string m_table_to_check, m_schema_to_check;

  /* The bool variable used to control thread termination for concurrent queries */
  bool m_need_stop;

};

/*
  ​​Thread worker function for parallel querying table schema information​.

  Arguments:
    arg: ​​Pointer to Check_tables_handling object​
*/
static void *query_table_and_column_info_thread(void *arg)
{
  Check_tables_handling *handler = (Check_tables_handling*) arg;
  handler->query_table_and_column_info_worker();
  return 0;
}

static string fix_db_name(const string &db_name, const string &server_name,
                          enum_node_type node_type) {
  string ret = db_name;
  if (node_type == NODE_TYPE_REMOTE || node_type == NODE_TYPE_REMOTE_SLAVE) {
    uint prefix_len = strlen(tdbctl_mysql_wrapper_prefix);
    /* Append the numeric suffix of this server */
    ret.push_back('_');
    ret.append(server_name.substr(prefix_len));
  }
  return ret;
}

static bool get_column_records_for_server(THD *thd,
                                          const string &db_name,
                                          const string &table_name, const string &server_name,
                                          MYSQL *mysql,
                                          Column_records &records,
                                          enum_node_type node_type) {
  char query_buff[512];
  MYSQL_ROW row;
  MYSQL_RES *res;
  Cluster_conn_manager *conn_mgr;
  Query_exec_manager query_mgr(thd);
  DBUG_ENTER("get_column_records_for_server");

  snprintf(query_buff, sizeof(query_buff), SQL_SELECT_IS_COLUMNS, db_name.c_str(), table_name.c_str());

  conn_mgr = thd->cluster_conn_manager;
  query_mgr.build_server_maps(conn_mgr);
  query_mgr.store_exec_query(server_name, string(query_buff), node_type);
  query_mgr.reset_error();

  tc_real_query(&query_mgr, server_name, mysql, node_type);
  if (query_mgr.get_error() || !(res = mysql_store_result(mysql))) {
    DBUG_RETURN(TRUE);
  }
  
  records.clear();
  while ((row = mysql_fetch_row(res))) {
    Column_record rec;
    rec.table_schema = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_TABLE_SCHEMA);
    rec.table_name = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_TABLE_NAME);
    rec.column_name = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_COLUMN_NAME);
    rec.ordinal_position = strtoul(row[IS_COLUMNS_ORDINAL_POSITION], NULL, 10);
    rec.data_type = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_DATA_TYPE);
    rec.collation_name = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_COLLATION_NAME);
    rec.column_key = DATA_STRING(row, mysql_fetch_lengths(res), IS_COLUMNS_COLUMN_KEY);
    records[rec.column_name] = rec;
  }
  mysql_free_result(res);

  DBUG_RETURN(FALSE);
}

static bool get_table_info_for_server(THD *thd, const string &db_name,
                                        const string &table_name,
                                        const string &server_name,
                                        MYSQL *mysql,
                                        Table_info &info,
                                        enum_node_type node_type) {
  char query_buff[512];
  MYSQL_ROW row;
  MYSQL_RES *res;
  Cluster_conn_manager *conn_mgr;
  Query_exec_manager query_mgr(thd);
  DBUG_ENTER("get_table_info_for_server");

  snprintf(query_buff, sizeof(query_buff), SQL_SELECT_IS_TABLES,
           db_name.c_str(), table_name.c_str());

  conn_mgr = thd->cluster_conn_manager;
  query_mgr.build_server_maps(conn_mgr);
  query_mgr.store_exec_query(server_name, string(query_buff), node_type);
  query_mgr.reset_error();

  tc_real_query(&query_mgr, server_name, mysql, node_type);
  if (query_mgr.get_error() || !(res = mysql_store_result(mysql))) {
    DBUG_RETURN(TRUE);
  }

  info.exists = FALSE;
  if ((row = mysql_fetch_row(res))) {
    info.exists = TRUE;
    info.table_schema = DATA_STRING(row, mysql_fetch_lengths(res), IS_TABLES_TABLE_SCHEMA);
    info.table_name = DATA_STRING(row, mysql_fetch_lengths(res), IS_TABLES_TABLE_NAME);
    info.table_engine = DATA_STRING(row, mysql_fetch_lengths(res), IS_TABLES_TABLE_ENGINE);
    info.table_collation = DATA_STRING(row, mysql_fetch_lengths(res), IS_TABLES_TABLE_COLLATION);
  }
  mysql_free_result(res);

  DBUG_RETURN(FALSE);
}

/**
 * Send metadata for check-table results.
 *
 * Fields:
 *  - SERVER_NAME   VARCHAR(64)         Server's identifier
 *  - DB            VARCHAR(64)         Corresponding db on the server
 *  - TABLE         VARCHAR(64*3*2)     Corresponding table on the server
 *  - STATUS        CHAR(10)            Check result: [OK, Error]
 *  - MESSAGE       VARCHAR(512)        Error message
 *
 * @retval True on error, False on success
 * */
static bool do_send_metadata(THD *thd) {
  List<Item> field_list;
  Item *item;
  DBUG_ENTER("do_send_metadata");

  field_list.push_back(item =
                           new Item_empty_string("Server_name", NAME_CHAR_LEN));
  field_list.push_back(item = new Item_empty_string("Db", NAME_CHAR_LEN));
  field_list.push_back(item = new Item_empty_string("Table", NAME_LEN * 2));
  field_list.push_back(item = new Item_empty_string("Status", 10));
  field_list.push_back(
      item = new Item_empty_string("Message", max_message_length));
  if (thd->send_result_metadata(&field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(TRUE);

  DBUG_RETURN(FALSE);
}

/**
 * Get names of tables in specified database that match the filter.
 *
 * @param conn: Connection to this Tdbctl server
 * @param db: Specified database
 * @param wild: Table name wildcard (fetch all tables if null)
 * @param[out] tables: List of results
 *
 * @retval True on error, False on success
 * */
static bool fill_tables_list(MYSQL *conn, const char *db, String *wild,
                            list<std::string> &tables) {
  char query_buff[512];
  MYSQL_ROW row;
  MYSQL_RES *res;
  DBUG_ENTER("fill_tables_list");

  if (wild) {
    snprintf(query_buff, sizeof(query_buff), SQL_GET_TABLES_IN_DB_LIKE, db,
             wild->ptr());
  } else {
    /* No wildcards: get all tables in <db> */
    snprintf(query_buff, sizeof(query_buff), SQL_GET_ALL_TABLES_IN_DB, db);
  }

  if (mysql_real_query(conn, query_buff, strlen(query_buff)) ||
      !(res = mysql_store_result(conn)))
    DBUG_RETURN(TRUE);

  while ((row = mysql_fetch_row(res))) {
    tables.push_back(string(row[0], mysql_fetch_lengths(res)[0]));
  }

  DBUG_RETURN(FALSE);
}

/**
 * Check a table.
 *
 * @param thd: Thread handler
 * @param tables: A table list with only the target table
 *
 * @retval True on error, False on success
 * */
bool tdbctl_check_table(THD *thd, TABLE_LIST *tables) {
  Cluster_conn_manager *conn_mgr;
  string db_name, table_name;
  DBUG_ENTER("tdbctl_check_table");

  if (check_some_access(thd, SHOW_CREATE_TABLE_ACLS, tables) ||
      !(tables->grant.privilege & SHOW_CREATE_TABLE_ACLS)) {
    my_error(ER_TABLEACCESS_DENIED_ERROR, MYF(0), "TDBCTL CHECK TABLE",
             thd->security_context()->priv_user().str,
             thd->security_context()->host_or_ip().str, tables->alias);
    DBUG_RETURN(TRUE);
  }

  /*
    DB name on this Tdbctl server should be considered a base name. A suffix is
    needed when checking the table on remotes.
  */
  db_name = string(tables->db, tables->db_length);
  table_name = string(tables->table_name, tables->table_name_length);

  if (init_cluster_conn_manager(thd, TRUE, FALSE, TRUE))
    DBUG_RETURN(TRUE);
  conn_mgr = thd->cluster_conn_manager;

  /* Send metadata for check results */
  if (do_send_metadata(thd))
    DBUG_RETURN(TRUE);

  if (do_check_one_table(thd, conn_mgr, db_name, table_name, TRUE))
    DBUG_RETURN(TRUE);

  my_eof(thd);
  DBUG_RETURN(FALSE);
}

/**
 * Check multiple tables.
 *
 * @param thd: Thread handler
 * @param db: Target database, use current db if null
 * @param wild: Table name wildcard, null value means all tables in target db
 *
 * @retval True on error, False on success
 * */
int tdbctl_check_tables(THD *thd, const char *db, String *wild) {
  MYSQL *my_conn;
  string my_server;
  Cluster_conn_manager *conn_mgr;
  list<string> tables;
  string db_name;
  DBUG_ENTER("tdbctl_check_tables");

  if (db) {
    db_name.assign(db);
  } else {
    LEX_STRING tmp;
    if (thd->copy_db_to(&tmp.str, &tmp.length))
      /* ERROR: No database selected */
      DBUG_RETURN(TRUE);
    db_name.assign(tmp.str, tmp.length);
  }

  /*
    Since we are checking multiple (or could be all) tables in the same db. The
    user is required to have access to the entire db.
  */
  if (validate_db_grants(thd, db_name.c_str()))
    DBUG_RETURN(TRUE);

  if (init_cluster_conn_manager(thd, TRUE, FALSE, TRUE))
    DBUG_RETURN(TRUE);
  conn_mgr = thd->cluster_conn_manager;

  my_server = conn_mgr->get_my_server_name();
  my_conn = conn_mgr->get_conn_map(NODE_TYPE_CTL).at(my_server);
  if (fill_tables_list(my_conn, db_name.c_str(), wild, tables)) {
    my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), "failed to fetch tables of current tdbctl node");
    DBUG_RETURN(TRUE);
  }

  /*
    Get the table_open_cache and table_definition_cache values for all nodes 
    related to ​table consistency validation, in order to facilitate recovery.
  */
  map<string, string> old_open_cache_map;
  map<string, string> old_def_cache_map;
  if(get_table_cache(thd, conn_mgr, old_open_cache_map, old_def_cache_map)) {
    DBUG_RETURN(TRUE);
  }

  /*
    Set the system variables table_open_cache and table_definition_cache 
    on cluster nodes to the values of the current primary tdbctl 
    to minimize the risk of OOM (Out Of Memory).
  */
  string my_open_cache = old_open_cache_map[my_server];
  string my_def_cache = old_def_cache_map[my_server];
  if(set_table_cache(thd, conn_mgr, my_open_cache, my_def_cache)) {
    DBUG_RETURN(TRUE);
  }

  /*
    Create and launch sub-threads for concurrent queries.
  */
  Check_tables_handling check_tables_hdl(thd);
  if(check_tables_hdl.launch_parallel_query_threads()) {
    my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), "failed to create sub-threads");
    DBUG_RETURN(TRUE);
  }

  /* Send metadata for check results */
  if (do_send_metadata(thd))
    DBUG_RETURN(TRUE);

  int res = 0;
  for (const string &table: tables) {
    if (do_check_one_table_by_parallel_query(thd, conn_mgr, &check_tables_hdl, db_name, table, TRUE)) {
      res = 1;
      break;
    }
  }

  /*
    Terminate sub-threads of concurrent queries.
  */
  check_tables_hdl.terminate_parallel_query_threads();

  /*
    Process tables that are unexpectedly present compared to the primary tdbctl node.
  */
  if(!res) {
    if(check_redundant_tables(thd, conn_mgr, db_name, wild, tables, TRUE)) {
      res = 1;
    }
  }

  /*
    Restore the table_open_cache and table_definition_cache values to their original values.
  */
  if(set_table_cache(thd, conn_mgr, old_open_cache_map, old_def_cache_map)) {
    DBUG_RETURN(TRUE);
  }

  my_eof(thd);
  DBUG_RETURN(res);
}

/**
 * Helper function that actually does the checking.
 *
 * @retval True on error, False on success
 * */
static bool do_check_one_table(THD *thd, Cluster_conn_manager *conn_mgr,
                               const string &db_name, const string &table_name,
                              bool do_send) {
  Protocol *protocol = (do_send ? thd->get_protocol() : NULL);
  DBUG_ENTER("do_check_one_table");
  char err_buff[1024];

  /* Lock against possible DDL actions */
  Table_MDL_lock_guard guard(thd, db_name, table_name);
  if (!guard.lock_successful()) {
    snprintf(err_buff, sizeof(err_buff), 
             "failed to lock table %s.%s on current server", 
             db_name.c_str(), table_name.c_str());
    my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), err_buff);
    DBUG_RETURN(TRUE);
  }

  /* Get column records for the table on this server */
  string my_server = conn_mgr->get_my_server_name();
  MYSQL *my_conn = conn_mgr->get_conn_map(NODE_TYPE_CTL).at(my_server);
  Table_info my_table_info;
  Column_records my_records;
  if (get_table_info_for_server(thd, db_name, table_name, my_server, my_conn,
                                my_table_info, NODE_TYPE_CTL) ||
      get_column_records_for_server(thd, db_name, table_name, my_server,
                                    my_conn, my_records, NODE_TYPE_CTL)) {
    my_error(ER_TCADMIN_INTERNAL_ERROR, MYF(0),
             "failed to get table definition from current server");
    DBUG_RETURN(TRUE);
  }
  if (!my_table_info.exists) {
    string tmp_tbl_name = db_name + "." + table_name;
    my_error(ER_BAD_TABLE_ERROR, MYF(0), tmp_tbl_name.c_str());
    DBUG_RETURN(TRUE);
  }

  /*
    Compare table & column definition from each Spider/Remote server against
    this server's.
  */
  enum_node_type type = NODE_TYPE_SPIDER;
  while (type != NODE_TYPE_END) {
    map<string, MYSQL *>::const_iterator conn_it;
    map<string, MYSQL *> conns = conn_mgr->get_conn_map(type);
    for (conn_it = conns.begin(); conn_it != conns.end(); ++conn_it) {
      const string &server_name = conn_it->first;
      MYSQL *mysql = conn_it->second;
      Table_info table_info;
      Column_records records;
      const char *status;
      Message_writer writer;

      /* For Remotes, add a numeric suffix according to their server_name */
      string fixed_db_name = fix_db_name(db_name, server_name, type);

      if (do_send && (protocol != NULL)) {
        protocol->start_row();
        protocol->store(server_name.c_str(), server_name.length(),
                        system_charset_info);
        protocol->store(fixed_db_name.c_str(), fixed_db_name.length(),
                        system_charset_info);
        protocol->store(table_name.c_str(), table_name.length(),
                        system_charset_info);
      }

      /* STAGE 1: check table info */
      if (get_table_info_for_server(thd, fixed_db_name, table_name, server_name,
                                    mysql, table_info, type)) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "failed to get table info");
        goto send_row;
      }

      if (!table_info.exists) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "table '%s.%s' does not exist", fixed_db_name.c_str(),
                     table_name.c_str());
        goto send_row;
      }

      compare_table_info(my_table_info, table_info, type, writer);

      /* STAGE 2: check column definitions */
      if (get_column_records_for_server(thd, fixed_db_name, table_name,
                                        server_name, mysql, records, type)) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "failed to get field definitions");
        goto send_row;
      }

      compare_columns_info(my_records, records, type, writer);

    send_row:
      if (do_send && (protocol != NULL)) {
        status = (writer.has_error() ? "Error" : "OK");
        protocol->store(status, strlen(status), system_charset_info);
        protocol->store(writer.raw_str(),
                        MY_MIN(writer.length(), max_message_length),
                        system_charset_info);
        if (protocol->end_row())
          DBUG_RETURN(TRUE);
      }
    }
    /* Once Spiders are finished, move on to Remotes */
    if (type == NODE_TYPE_SPIDER)
      type = NODE_TYPE_REMOTE;
    else /* Remotes are done, end the loop */
      type = NODE_TYPE_END;
  }

  DBUG_RETURN(FALSE);
}

static bool validate_db_grants(THD *thd, const char *db) {
  char db_name[NAME_LEN];
  Security_context *sctx = thd->security_context();
  uint db_access;

  strcpy(db_name, db);
  if (lower_case_table_names)
    my_casedn_str(files_charset_info, db_name);

#ifndef NO_EMBEDDED_ACCESS_CHECKS
  if (sctx->check_access(DB_ACLS))
    db_access = DB_ACLS;
  else
    db_access = (acl_get(sctx->host().str, sctx->ip().str,
                         sctx->priv_user().str, db_name, 0) |
                 sctx->master_access());
  if (!(db_access & DB_ACLS)) {
    my_error(ER_DBACCESS_DENIED_ERROR, MYF(0), sctx->priv_user().str,
             sctx->host_or_ip().str, db_name);
    return TRUE;
  }
#endif

  if (!is_infoschema_db(db_name) && check_db_dir_existence(db_name)) {
    /* ERROR: Unknown database */
    my_error(ER_BAD_DB_ERROR, MYF(0), db_name);
    return TRUE;
  }

  return FALSE;
}

/**
 * Check one table by parallel queries.
 *
 * @retval True on error, False on success
 * */
static bool do_check_one_table_by_parallel_query(THD *thd, 
                                                 Cluster_conn_manager *conn_mgr,
                                                 Check_tables_handling *check_tables_hdl,
                                                 const string &db_name, 
                                                 const string &table_name,
                                                 bool do_send) 
{
  Protocol *protocol = (do_send ? thd->get_protocol() : NULL);
  uint err_code = 0;
  string err_msg;
  char err_buff[1024];
  DBUG_ENTER("do_check_one_table_by_parallel_query");

  /* Lock against possible DDL actions */
  Table_MDL_lock_guard guard(thd, db_name, table_name);
  if (!guard.lock_successful()) {
    snprintf(err_buff, sizeof(err_buff), 
             "failed to lock table %s.%s on current server", 
             db_name.c_str(), table_name.c_str());
    my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), err_buff);
    DBUG_RETURN(TRUE);
  }
  
  /* Prepare for concurrent queries */
  check_tables_hdl->prepare_for_next_query(db_name, table_name);

  /* Wait for parallel queries to complete. */
  check_tables_hdl->wait_parallel_query_to_finish();

  /* Get column records for the table on this server */
  Table_info my_table_info;
  Column_records my_records;
  string my_server = conn_mgr->get_my_server_name();

  if (check_tables_hdl->get_table_info(my_server, NODE_TYPE_CTL, my_table_info, 
                                       err_code, err_msg)) {
    snprintf(err_buff, sizeof(err_buff), 
             "failed to get table information " \
             "of %s.%s from current server, err_code: %d, err_msg: %s", 
             db_name.c_str(), table_name.c_str(), err_code, err_msg.c_str());
    my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), err_buff);
    DBUG_RETURN(TRUE);
  }

  if (!my_table_info.exists) {
    snprintf(err_buff, sizeof(err_buff), 
             "Table %s.%s has been unexpectedly removed", 
             db_name.c_str(), table_name.c_str());
    my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), err_buff);
    DBUG_RETURN(TRUE);
  }

  if (check_tables_hdl->get_column_records(my_server, NODE_TYPE_CTL, my_records,
                                           err_code, err_msg)) {
    snprintf(err_buff, sizeof(err_buff), 
             "failed to get columns information " \
             "of %s.%s from current server, err_code: %d, err_msg: %s", 
             db_name.c_str(), table_name.c_str(), err_code, err_msg.c_str());
    my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), err_buff);
    DBUG_RETURN(TRUE);
  }

  /*
    Compare table & column definition from each Spider/Remote server against
    this server's.
  */
  enum_node_type type = NODE_TYPE_SPIDER;
  while (type != NODE_TYPE_END) {
    map<string, MYSQL *>::const_iterator conn_it;
    map<string, MYSQL *> conns = conn_mgr->get_conn_map(type);
    for (conn_it = conns.begin(); conn_it != conns.end(); ++conn_it) {
      const string &server_name = conn_it->first;
      Table_info table_info;
      Column_records records;
      const char *status;
      Message_writer writer;

      /* For Remotes, add a numeric suffix according to their server_name */
      string fixed_db_name = fix_db_name(db_name, server_name, type);

      if (do_send && (protocol != NULL)) {
        protocol->start_row();
        protocol->store(server_name.c_str(), server_name.length(),
                        system_charset_info);
        protocol->store(fixed_db_name.c_str(), fixed_db_name.length(),
                        system_charset_info);
        protocol->store(table_name.c_str(), table_name.length(),
                        system_charset_info);
      }

      /* STAGE 1: check table info */
      if (check_tables_hdl->get_table_info(server_name, type, table_info, 
                                           err_code, err_msg)) {
        writer.write(Message_writer::MSG_LEVEL_ERROR, "failed to get table information" \
                     ", err_code: %d, err_msg: %s", err_code, err_msg.c_str());
        goto send_row;
      }

      if (!table_info.exists) {
        writer.write(Message_writer::MSG_LEVEL_ERROR,
                      "This table does not exist");
        goto send_row;
      }

      compare_table_info(my_table_info, table_info, type, writer);

      /* STAGE 2: check column definitions */
      if (check_tables_hdl->get_column_records(server_name, type, records,
                                               err_code, err_msg)) {
        writer.write(Message_writer::MSG_LEVEL_ERROR, "failed to get columns information"
                     ", err_code: %d, err_msg: %s", err_code, err_msg.c_str());
        goto send_row;
      }

      compare_columns_info(my_records, records, type, writer);

    send_row:
      if (do_send && (protocol != NULL)) {
        status = (writer.has_error() ? "Error" : "OK");
        protocol->store(status, strlen(status), system_charset_info);
        protocol->store(writer.raw_str(),
                        MY_MIN(writer.length(), max_message_length),
                        system_charset_info);
        if (protocol->end_row()) {
          snprintf(err_buff, sizeof(err_buff), 
             "failed to send check result for table %s.%s of node %s", 
             fixed_db_name.c_str(), table_name.c_str(), server_name.c_str());
          my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), err_buff);
          DBUG_RETURN(TRUE);
        }
      }
    }
    /* Once Spiders are finished, move on to Remotes */
    if (type == NODE_TYPE_SPIDER)
      type = NODE_TYPE_REMOTE;
    else /* Remotes are done, end the loop */
      type = NODE_TYPE_END;
  }

  DBUG_RETURN(FALSE);
}

/*
  Detecting extra tables on cluster nodes relative to the primary tdbctl node​.

  Arguments:
    thd: Thread handler
    conn_mgr: Connection manager.
    db_name: Target database
    wild: Table name wildcard, null value means all tables in target db
    my_table_list: List of table names selected from the primary tdbctl node.
*/
static bool check_redundant_tables(THD *thd, Cluster_conn_manager *conn_mgr, 
                                   string db_name, String *wild,
                                   const list<string> &my_table_list, bool do_send) {
  Protocol *protocol = (do_send ? thd->get_protocol() : NULL);
  unordered_set<string> my_tables_set(my_table_list.begin(), my_table_list.end());

  /*
    Find tables present on Spider/Remote nodes but not found on the current node.
  */
  for(enum_node_type node_type: {NODE_TYPE_SPIDER, NODE_TYPE_REMOTE}) {
    map<std::string, MYSQL *> conns_map = conn_mgr->get_conn_map(node_type);
    for(const std::pair<std::string, MYSQL *> &node_conn: conns_map) {
      string server_name = node_conn.first;
      string fixed_db_name = fix_db_name(db_name, server_name, node_type);

      /*
        Fetch tables of Spider/Remote nodes.
      */
      list<string> node_tables;
      if (fill_tables_list(node_conn.second, fixed_db_name.c_str(), wild, node_tables)) {
        string err_buff = "failed to fetch tables of node " + server_name;
        my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), err_buff.c_str());
        DBUG_RETURN(TRUE);
      }

      /*
        Send redundant tables.
      */
      for(const string &table_name: node_tables) {
        if(my_tables_set.find(table_name) == my_tables_set.end()) {
          Message_writer writer;
          const char *status;
          writer.write(Message_writer::MSG_LEVEL_ERROR,
                     "table '%s.%s' exists unexpectedly", fixed_db_name.c_str(),
                     table_name.c_str());
          
          if (do_send && (protocol != NULL)) {
            protocol->start_row();
            protocol->store(server_name.c_str(), server_name.length(),
                            system_charset_info);
            protocol->store(fixed_db_name.c_str(), fixed_db_name.length(),
                            system_charset_info);
            protocol->store(table_name.c_str(), table_name.length(),
                            system_charset_info);
            status = (writer.has_error() ? "Error" : "OK");
            protocol->store(status, strlen(status), system_charset_info);
            protocol->store(writer.raw_str(),
                            MY_MIN(writer.length(), max_message_length),
                            system_charset_info);
            if (protocol->end_row())
              DBUG_RETURN(TRUE);
          }
        }
      }
    }
  }

  DBUG_RETURN(FALSE);
}

/*
  Get the values of table_open_cache and table_definition_cache 
  for all nodes related to table consistency validation.

  Args:
    open_cache_map: the mapping from node names to their respective table_open_cache values.
    def_cache_map: the mapping from node names to their respective table_definition_cache values.
  
  Return:
    true       error
    false      ok
*/
static bool get_table_cache(THD *thd, Cluster_conn_manager *conn_mgr, 
                            map<string, string> &open_cache_map, 
                            map<string, string> &def_cache_map) {
  const string sys_table_open_cache = "@@GLOBAL.table_open_cache";
  const string sys_table_def_cache = "@@GLOBAL.table_definition_cache";
  string server_name;
  MYSQL *server_conn = NULL;

  open_cache_map.clear();
  def_cache_map.clear();

  for(enum_node_type node_type: {NODE_TYPE_CTL, NODE_TYPE_SPIDER, NODE_TYPE_REMOTE}) {
    map<std::string, MYSQL *> conns_map = conn_mgr->get_conn_map(node_type);
    for(const std::pair<std::string, MYSQL *> &node_conn: conns_map) {
      server_name = node_conn.first;
      server_conn = node_conn.second;
      if((node_type == NODE_TYPE_CTL) && 
         (server_name != conn_mgr->get_my_server_name())) {
        continue;
      }

      if(tc_get_variable_value(server_conn, sys_table_open_cache, 
                               open_cache_map[server_name]) || 
         tc_get_variable_value(server_conn, sys_table_def_cache, 
                               def_cache_map[server_name])) {
        string err_buff = "failed to get table_open_cache or table_definition_cache" \
                          " of node " + server_name;
        my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), err_buff.c_str());
        return true;
      }
    }
  }

  return false;
}

/*
  Set the values of table_open_cache and table_definition_cache 
  for all nodes related to table consistency validation.

  Args:
    open_cache_map: the mapping from node names to their respective table_open_cache values.
    def_cache_map: the mapping from node names to their respective table_definition_cache values.
  
  Return:
    true       error
    false      ok
*/
static bool set_table_cache(THD *thd, Cluster_conn_manager *conn_mgr, 
                            const map<string, string> &open_cache_map, 
                            const map<string, string> &def_cache_map) {
  const string sys_table_open_cache = "@@GLOBAL.table_open_cache";
  const string sys_table_def_cache = "@@GLOBAL.table_definition_cache";
  string server_name;
  MYSQL *server_conn = NULL;
  uint err_code;
  string err_msg;
  char info_buff[512];

  for(enum_node_type node_type: {NODE_TYPE_CTL, NODE_TYPE_SPIDER, NODE_TYPE_REMOTE}) {
    map<std::string, MYSQL *> conns_map = conn_mgr->get_conn_map(node_type);
    for(const std::pair<std::string, MYSQL *> &node_conn: conns_map) {
      server_name = node_conn.first;
      server_conn = node_conn.second;
      if((node_type == NODE_TYPE_CTL) && 
         (server_name != conn_mgr->get_my_server_name())) {
        continue;
      }

      err_code = tc_set_variable_value(server_conn, sys_table_open_cache, 
                                       open_cache_map.at(server_name), err_msg);
      if(err_code > 0) {
        snprintf(info_buff, sizeof(info_buff),
                 "failed to set table_open_cache of node %s, err_code: %u, err_msg: %s", 
                 server_name.c_str(), err_code, err_msg.c_str());
        my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), info_buff);
        return true;
      }
      err_code = tc_set_variable_value(server_conn, sys_table_def_cache, 
                                       def_cache_map.at(server_name), err_msg);
      if(err_code > 0) {
        snprintf(info_buff, sizeof(info_buff),
                 "failed to set table_definition_cache of node %s, err_code: %u, err_msg: %s", 
                 server_name.c_str(), err_code, err_msg.c_str());
        my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), info_buff);
        return true;
      }
    }
  }

  return false;
}

/*
  Set the table_open_cache and table_definition_cache to the same value 
  for all nodes related to table consistency validation.

  Args:
    open_cache_val: the value of table_open_cache.
    def_cache_val: the value of table_definition_cache.
  
  Return:
    true       error
    false      ok
*/
static bool set_table_cache(THD *thd, Cluster_conn_manager *conn_mgr, 
                            const string &open_cache_val, 
                            const string &def_cache_val) {
  const string sys_table_open_cache = "@@GLOBAL.table_open_cache";
  const string sys_table_def_cache = "@@GLOBAL.table_definition_cache";
  string server_name;
  MYSQL *server_conn = NULL;
  uint err_code;
  string err_msg;
  char info_buff[512];

  for(enum_node_type node_type: {NODE_TYPE_CTL, NODE_TYPE_SPIDER, NODE_TYPE_REMOTE}) {
    map<std::string, MYSQL *> conns_map = conn_mgr->get_conn_map(node_type);
    for(const std::pair<std::string, MYSQL *> &node_conn: conns_map) {
      server_name = node_conn.first;
      server_conn = node_conn.second;
      if((node_type == NODE_TYPE_CTL) && 
         (server_name != conn_mgr->get_my_server_name())) {
        continue;
      }

      err_code = tc_set_variable_value(server_conn, sys_table_open_cache, 
                                       open_cache_val, err_msg);
      if(err_code > 0) {
        snprintf(info_buff, sizeof(info_buff),
                 "failed to set table_open_cache of node %s, err_code: %u, err_msg: %s", 
                 server_name.c_str(), err_code, err_msg.c_str());
        my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), info_buff);
        return true;
      }
      err_code = tc_set_variable_value(server_conn, sys_table_def_cache, 
                                       def_cache_val, err_msg);
      if(err_code > 0) {
        snprintf(info_buff, sizeof(info_buff),
                 "failed to set table_definition_cache of node %s, err_code: %u, err_msg: %s", 
                 server_name.c_str(), err_code, err_msg.c_str());
        my_error(ER_TCADMIN_CHECK_TABLES_ERROR, MYF(0), info_buff);
        return true;
      }
    }
  }

  return false;
}

/*
  Compare two table structure information and 
  log inconsistent table details to a Writer object.

  Return:
    true     Is not consistent
    false    Is consistent
*/
bool compare_table_info(const Table_info &expected, const Table_info &actual, 
                        enum_node_type node_type, Message_writer &writer) {
  bool ret = false;
  if (!actual.exists) {
    writer.write(Message_writer::MSG_LEVEL_ERROR,
                  "This table does not exist");
    return true;
  }

  /* Compare default charsets */
  if (expected.table_collation != actual.table_collation) {
    writer.write(Message_writer::MSG_LEVEL_ERROR,
                  "Expected table collation to be '%s', but actually is '%s'",
                  expected.table_collation.c_str(), 
                  actual.table_collation.c_str());
    ret = true;
  }

  /* Compare engines (for Spiders) */
  if (node_type == NODE_TYPE_SPIDER &&
      "SPIDER" != actual.table_engine) {
    writer.write(Message_writer::MSG_LEVEL_ERROR,
                  "Expected table engine to be '%s', but actually is '%s'",
                  "SPIDER", actual.table_engine.c_str());
    ret = true;
  }

  /* Compare engines (skip Spiders) */
  if (node_type != NODE_TYPE_SPIDER &&
      expected.table_engine != actual.table_engine) {
    writer.write(Message_writer::MSG_LEVEL_ERROR,
                  "Expected table engine to be '%s', but actually is '%s'",
                  expected.table_engine.c_str(), 
                  actual.table_engine.c_str());
    ret = true;
  }

  return ret;
}

/*
  Compare columns information of two tables and 
  log inconsistent details to a Writer object.

  Return:
    true     Is not consistent
    false    Is consistent
*/
bool compare_columns_info(const Column_records &expected, const Column_records &actual, 
                          enum_node_type node_type, Message_writer &writer) {
  bool ret = false;
  for (Column_records::const_iterator it = expected.begin();
        it != expected.end(); ++it) {
    const string &column_name = it->first;
    const Column_record &my_rec = it->second;
    ulonglong flags;
    if (!actual.count(column_name)) {
      writer.write(Message_writer::MSG_LEVEL_ERROR, "missing column '%s'",
                    column_name.c_str());
      ret = true;
    } else {
      const Column_record &tmp_rec = actual.at(column_name);
      if (!(flags = column_record_cmp(my_rec, tmp_rec)))
        /* consistent definition, skip */
        continue;
      if (flags & COLUMN_DIFF_DATA_TYPE)
        write_column_diff_error(writer, column_name.c_str(), 
          "Expected data type to be '%s', but actually is '%s'",
          my_rec.data_type.c_str(), tmp_rec.data_type.c_str());
      if (flags & COLUMN_DIFF_ORDINAL_POSITION)
        write_column_diff_error(writer, column_name.c_str(), 
          "Expected ordinal position to be %lu, but actually is %lu",
          my_rec.ordinal_position, tmp_rec.ordinal_position);
      if (flags & COLUMN_DIFF_COLLATION)
        write_column_diff_error(writer, column_name.c_str(), 
          "Expected collation to be '%s', but actually is '%s'",
          my_rec.collation_name.c_str(), tmp_rec.collation_name.c_str());
      if (flags & COLUMN_DIFF_COLUMN_KEY)
        write_column_diff_error(writer, column_name.c_str(), 
          "Expected column key to be '%s', but actually is '%s'",
          my_rec.column_key.c_str(), tmp_rec.column_key.c_str());
      ret = true;
    }
  }

  for (Column_records::const_iterator it = actual.begin();
        it != actual.end(); ++it) {
    const string &column_name = it->first;
    if (!expected.count(column_name)) {
      writer.write(Message_writer::MSG_LEVEL_ERROR, "extra column '%s'",
                    column_name.c_str());
      ret = true;
    }
  }

  return ret;
}