/*
    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
*/

#ifndef TC_BASE_INCLUDED
#define TC_BASE_INCLUDED

#include <iostream>
#include <string>
#include <map>
#include <set>
#include <sstream>
#include <regex>
#include <mutex>
#include <stdlib.h>
#ifdef _WIN32
#include <winsock2.h>
#include <iphlpapi.h>
#include <ws2tcpip.h>
#else
#include <ifaddrs.h>
#include <netinet/in.h> 
#include <net/if.h>
#endif
#include <sys/types.h>
#include "mysql.h"
using namespace std;

//wrapper name map to mysql.servers's Wrapper field
#define MYSQL_WRAPPER "mysql"
#define MYSQL_SLAVE_WRAPPER "mysql_slave"
#define SPIDER_WRAPPER "SPIDER"
#define SPIDER_SLAVE_WRAPPER "SPIDER_SLAVE"
#define TDBCTL_WRAPPER "TDBCTL"
#define NULL_WRAPPER ""

//value of server_name in cluster_monitor.cluster_heartbeat
#define CLUSTER_FLAG "cluster"

//some user define shard errors
#define TCADMIN_PARSE_TABLE_COMMENT_OK 0
#define TCADMIN_PARSE_TABLE_COMMENT_ERROR 1
#define TCADMIN_PARSE_TABLE_COMMENT_UNSUPPORTED 2
#define TCADMIN_PARSE_SHARD_COUNT_INVALID 3
#define TCADMIN_PARSE_SHARD_FUNCTION_INVALID 4
#define TCADMIN_PARSE_SHARD_TYPE_INVALID 5

enum tspider_shard_func {
  tspider_shard_func_crc32,
  tspider_shard_func_crc32_ci,
  tspider_shard_func_none,
  tspider_shard_func_murmur_jump_hash,
};
enum tspider_shard_type { tspider_shard_type_list, tspider_shard_type_range };

#define TC_CONN_READ_TIMEOUT 600
#define TC_CONN_WRITE_TIMEOUT 600
#define TC_CONN_CONNECT_TIMEOUT 60
#define TC_CONN_MAX_RETRIES_ON_FAILS 3

//all spider node(include spider slave node) need execute sql
#define TC_SPIDER_NEED_EXECUTE 1
//all remote node need execute sql
#define TC_REMOTE_NEED_EXECUTE 2
//all tdbctl node need execute sql
#define TC_TDBCTL_NEED_EXECUTE 4
//all spider node executed before other nodes.
#define TC_SPIDER_EXECUTE_FIRST 8
//only one spider node execute sql
#define TC_ONLY_ONE_SPIDER_NEED_EXECUTE 16
//the designated node need execute sql
#define TC_DESIGNATED_NODE_NEED_EXECUTE 32

#define RETURN_RESULT_SET_FROM_ONE_NODE 1
#define RETURN_RESULT_SET_FROM_MULTI_NODES 2

enum enum_node_type {
  NODE_TYPE_SPIDER = 0, /* this should ALWAYS be the first */
  NODE_TYPE_SPIDER_SLAVE = 1,
  NODE_TYPE_REMOTE = 2,
  NODE_TYPE_REMOTE_SLAVE = 3,
  NODE_TYPE_CTL = 4, /* this should ALWAYS be the second to last */
  NODE_TYPE_END = 5, /* this should ALWAYS be the last */
};

#define ENUM_NODE_TYPE_BEGIN NODE_TYPE_SPIDER
#define ENUM_NODE_TYPE_END NODE_TYPE_END
#define ENUM_NODE_TYPE_COUNT int(NODE_TYPE_END)
#define ENUM_NODE_TYPE_COUNT_EXCLUDE_TDBCTL int(NODE_TYPE_END) - 1

#define TC_STR_MOD " % "
#define TC_STR_COMMA ", "
#define TC_STR_DELIMITER ";"

#define TC_STR_IDENTIFIER(a) std::string("`" + (a) + "`")
#define TC_STR_DOUBLE_QUOTED(a) std::string("\"" + (a) + "\"")
#define TC_STR_SINGLE_QUOTED(a) std::string("'" + (a) + "'")
#define TC_STR_BACK_QUOTED(a) std::string("`" + (a) + "`")

//mysql guard to free mysql connection
#define MYSQL_GUARD(p) std::shared_ptr<MYSQL> p##p(p, \
[](MYSQL *p) {mysql_close(p);});
//mysql_res guard to free mysql_result
#define MYSQL_RES_GUARD(p) std::shared_ptr<MYSQL_RES> p##p(p, \
[](MYSQL_RES *p) {mysql_free_result(p);});
#define MEM_ROOT_GUARD(p) std::shared_ptr<MEM_ROOT> p##p(&p, \
[](MEM_ROOT *p) {free_root(p, MYF(0));});void gettype_create_filed(Create_field *cr_field, String &res);
void filed_add_zerofill_and_unsigned(String &res, bool unsigned_flag, bool zerofill);
int parse_get_shard_key_for_spider(
    const char*		table_comment,
    char*		key_buf,
    uint		key_len
);
int parse_get_config_table_for_spider(
    const char*		table_comment,
    char*		key_buf,
    uint		key_len
);
enum_sql_command tc_get_sql_type(THD *thd, LEX *lex);
int tc_get_shard_key(THD *thd, LEX *lex, char *buf, uint len);
const char* tc_get_cur_tbname(THD *thd, LEX *lex);
const char* tc_get_cur_dbname(THD *thd, LEX *lex);
const char* tc_get_new_tbname(THD *thd, LEX *lex);
const char* tc_get_new_dbname(THD *thd, LEX *lex);
bool tc_is_with_shard(THD *thd, LEX *lex);

bool tc_unsupport_sql_type(int sql_type);
bool tc_distribute_spider_only(int sql_type);
bool tc_distribute_spider_and_remote(int sql_type);

const char* get_stmt_type_str(int type);


typedef struct tc_exec_info
{
    uint err_code;
    // 0 means no sql statment was sent to the node
    // 1 means some sql statements are ready to be sent to the node
    bool prepare_sql;
    MYSQL_RES *res;
    string err_msg;
    ulonglong row_affect;
} TC_EXEC_INFO;

typedef struct tc_execute_result
{
    bool result; // TURE, error happened; FALASE, SUCCEED
    map<string, tc_exec_info> result_info[ENUM_NODE_TYPE_COUNT];
} TC_EXEC_RESULT;

typedef struct tc_parse_result
{
    string db_name;
    string table_name;
    string new_table_name;
    string new_db_name;

    //original query
    LEX_CSTRING query_string;
    string spider_sql;
    map<string, string> remote_sql_map;
    string designated_node_sql;
    int execute_flag;
    int result_set_flag;

    string shard_key;
    int shard_count;
    tspider_shard_func shard_func;
    tspider_shard_type shard_type;
} TC_PARSE_RESULT;

struct AUTH_INFO {
  uint port;
  std::string host;
  std::string ipport_str;
  std::string user;
  std::string passwd;
  std::string wrapper;
};

inline void fill_auth_info(AUTH_INFO *info, const std::string &host, uint port,
                           const std::string &user, const std::string &passwd,
                           const std::string &wrapper) {
  info->host = host;
  info->port = port;
  info->user = user;
  info->passwd = passwd;
  info->ipport_str = host + "#" + std::to_string(port);
  info->wrapper = wrapper;
}

class Query_exec_manager {
public:
  friend class Cluster_conn_manager;

  Query_exec_manager(THD *thd);

  inline void reset_error() { error = 0; }
  inline int get_error() const { return error; }

  /**
   * @brief Clear error and maps
   * */
  void clear();

  /**
   * @brief Get real query of a node for execution
   *
   * @param[in]   server_name node's identifier
   * @param[out]  real_query stores a copy of the real query
   * @param[in]   node_type node's type
   *
   * @retval FALSE if node's real query is found, TRUE otherwise
   * */
  bool get_real_query(const std::string &server_name, std::string &real_query,
                     enum_node_type node_type);

  /**
   * @brief Store a query execution result from a node, into the result map
   *
   * @note a lock is used to ensure thread safety
   *
   * @param[in] server_name node's identifier
   * @param[in] exec_info a struct containing exec results
   * @param[in] node_type node's type
   * */
  void store_exec_info(const std::string &server_name,
                   const tc_exec_info &exec_info,
                   enum_node_type node_type);

  int get_exec_info(const std::string &server_name,
                    tc_exec_info &exec_info,
                    enum_node_type node_type);

  /**
   * @brief Store an exec query for a node identified by server_name
   *
   * @note make_real_query() is called to generate and store a real query for
   * the execution
   *
   * @param[in] server_name node's identifier
   * @param[in] query exec query for the node
   * @param[in] node_type node's type
   * */
  void store_exec_query(const std::string &server_name,
                        const std::string &query, enum_node_type node_type);

  /**
   * @brief Store an exec query for all nodes of the type specified
   *
   * @note make_real_query() is called to generate and store a real query for
   * the execution
   *
   * @param[in] query exec query for the nodes
   * @param[in] node_type nodes' type
   * */
  void store_exec_query(const std::string &query, enum_node_type node_type);

  /**
   * @brief Store exec queries for nodes specified by a map
   *
   * @note make_real_query() is called to generate and store a real query for
   * the execution
   *
   * @param[in] sql_map a map (key: server_name) specifying query for each node
   * @param[in] query exec query for the node
   * @param[in] node_type node's type
   * */
  void store_exec_query(const std::map<std::string, std::string> &sql_map,
                       enum_node_type node_type);

  /**
   * @brief Register a position for every server (node) in the query and result
   * maps
   *
   * This has to be called before using a query_exec_manager for generating or
   * executing queries, any query or exec result must be assigned to a existing
   * server in the maps.
   *
   * @param[in] conn_mgr a connection manager reference, we friend this class so
   * we can iterate over all the servers
   * */
  void build_server_maps(Cluster_conn_manager *conn_mgr);

  /* TODO: get rid of it */
  int get_results(tc_execute_result *res) const;

  /* set exec_flag */
  void set_exec_flag(int flag) { exec_flag = flag; };

  /* get exec_flag */
  int get_exec_flag() { return exec_flag; };

private:
  Query_exec_manager() {} /* =delete */

  /**
   * @brief Append "before queries" in front of an "exec query" to generate a
   * "real query"
   *
   * @param[in] exec_query execution query
   * @param[out] real_query generated real query
   * @param[in] node_type node's type
   * */
  int make_real_query(const std::string &exec_query, std::string &real_query,
                      enum_node_type node_type);

  THD *m_thd;

  int error;

  std::mutex result_mtx;

  /* Execution results for each server store here */
  std::map<std::string, tc_exec_info> exec_results[ENUM_NODE_TYPE_COUNT];

  /*
    Intermediate queries for each server store here
    @TODO: remove it in the future?
  */
  std::map<std::string, std::string> exec_queries[ENUM_NODE_TYPE_COUNT];

  /* Real execution queries for each server store here */
  std::map<std::string, std::string> real_queries[ENUM_NODE_TYPE_COUNT];

  /* flag to control execution logic */
  int exec_flag;
};

class Cluster_conn_manager {
public:
  friend class Query_exec_manager;

  Cluster_conn_manager();

  ~Cluster_conn_manager();

  /**
   * @brief Read mysql.servers table and initialize auth info & conns
   *
   * Unless no_connect is TRUE, this function would build connections to every
   * server and report an error if either attempt fails.
   *
   * @param force If false, refresh only when server_version is outdated
   * @param no_connect If true, do not build connections to any servers
   *
   * @retval FALSE on success, TRUE on error
   * */
  bool refresh(bool force, bool no_connect);

  /**
   * @brief Connect to a server identified by server_name (w/ node_type)
   *
   * @param server_name Server's identifier
   * @param type Node type of server
   * @param passive If true, simply set conn=NULL when connection fails, instead
   * of raising an error
   *
   * @retval TRUE on failure, FALSE on success
   * */
  bool connect(const std::string &server_name, enum_node_type type, bool passive);

  /**
   * @brief Connect to a server identified by server_name (w/o node_type)
   *
   * @param server_name Server's identifier
   * @param passive If true, simply set conn=NULL when connection fails, instead
   * of raising an error
   *
   * @retval TRUE on failure, FALSE on success
   * */
  bool connect(const std::string &server_name, bool passive);

  /**
   * @brief Connect to all nodes of a specific type
   *
   * @param type Node type
   * @param passive If true, simply set conn=NULL when connection fails, instead
   * of raising an error
   *
   * @retval TRUE on failure, FALSE on success
   * */
  bool connect(enum_node_type type, bool passive);

  /**
   * @brief Clear everything
   * */
  void clear();

  /**
   * @brief Identify this server among all TDBCTL nodes in mysql.servers
   *
   * We do this by comparing @@server_uuid
   *
   * @retval FALSE on success, TRUE on failure to identify
   * */
  bool identify_self();

  inline const std::string &get_my_server_name() const {
    return my_server_name;
  }

  inline const std::map<std::string, AUTH_INFO> &get_auth_map(enum_node_type type) {
    return server_auths[type];
  }

  inline const std::map<std::string, MYSQL *> &get_conn_map(enum_node_type type) {
    return server_conns[type];
  }

  inline const std::map<std::string, MYSQL *> &get_spider_conn_map() const {
    return server_conns[NODE_TYPE_SPIDER];
  }

  MY_ATTRIBUTE((unused)) inline uint get_spider_count() const { return spider_count; }

  inline const std::map<std::string, MYSQL *> &get_remote_conn_map() const {
    return server_conns[NODE_TYPE_REMOTE];
  }

  MY_ATTRIBUTE((unused)) inline uint get_shard_count() const { return shard_count; }

  /*
    Check if every server of any type is registered in the query exec manager
  */
  bool check_query_manager_validity(Query_exec_manager *query_mgr);

private:

  bool initialized;

  MEM_ROOT mem_root;

  uint spider_count;
  uint shard_count;

  ulong server_version;

  std::string my_server_name;

  /**
   * For each map we have:
   * @key: "<server_name>"
   * @val: AUTH_INFO
   * */
  std::map<std::string, AUTH_INFO> server_auths[ENUM_NODE_TYPE_COUNT];

  /**
   * For each map we have:
   * @key: "<server_name>"
   * @val: CONN
   * */
  std::map<std::string, MYSQL *> server_conns[ENUM_NODE_TYPE_COUNT];

  /**
   * @brief Check if current server_version is outdated, and update it if so
   *
   * @note server_version should not be modified anywhere else, and should only
   * be maintained here
   *
   * @retval TRUE if current server_version is outdated, FALSE otherwise
   * */
  bool check_server_version();

  int ping(MYSQL *mysql);
};

bool init_cluster_conn_manager(THD *thd, bool force_refresh, bool no_connect,
                               bool identify_self);

int tc_grant_single_node(THD *thd, MYSQL *mysql, const AUTH_INFO &auth);
int tc_grant_single_to_multi(THD *thd, MYSQL *mysql,
                             const AUTH_INFO &target_auth,
                             enum_node_type node_type);
int tc_grant_multi_to_single(THD *thd, const AUTH_INFO &target_auth,
                             enum_node_type node_type);

void tc_parse_result_init(TC_PARSE_RESULT *parse_result_t);
bool is_add_or_drop_unique_key(THD *thd, LEX *lex);

int parse_get_spider_user_comment(
  const char* comment, 
  int* shard_count,
  tspider_shard_func* shard_func,
  tspider_shard_type* shard_type
);

int tcadmin_validate_comment_keyword(const char* buf);

int tcadmin_validate_and_fill_value(
  const char* key_buf,
  const char* value_buf,
  int* shard_count,
  tspider_shard_func* shard_func,
  tspider_shard_type* shard_type
);

/*
sub convert_spider_use_db() {
sub convert_spider_set_db() {
sub convert_spider_privilege_sql {
sub convert_spider_create_event {
sub convert_spider_dml {
**/
bool tc_parse_getkey_for_spider(
  THD *thd, 
  char *key_name, 
  char *result, 
  int buf_len, 
  bool *is_unique_key,
  bool *is_unsigned_key
);

string tc_get_only_spider_ddl_withdb(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tc_get_only_spider_ddl(
  TC_PARSE_RESULT *tc_parse_result_t, 
  int shard_count
);

string tcadmin_get_shard_range_by_index(
  int index, 
  int shard_count, 
  bool is_unsigned
);

void tc_parse_spider_create_table(TC_PARSE_RESULT *tc_parse_result_t,
                                  bool is_unsigned_key, size_t part_start);
void tc_parse_remote_create_table(TC_PARSE_RESULT *tc_parse_result_t);


void tc_parse_spider_rename_table(
  TC_PARSE_RESULT *tc_parse_result_t
);

void tc_parse_remote_rename_table(
  TC_PARSE_RESULT *tc_parse_result_t
);

void tc_parse_spider_create_table_like(
  TC_PARSE_RESULT *tc_parse_result_t
);

void tc_parse_remote_create_table_like(
  TC_PARSE_RESULT *tc_parse_result_t
);

void tc_parse_spider_drop_table(
  TC_PARSE_RESULT *tc_parse_result_t
);

void tc_parse_remote_drop_table(
  TC_PARSE_RESULT *tc_parse_result_t
);

void tc_parse_remote_create_database(TC_PARSE_RESULT *tc_parse_result_t);
void tc_parse_remote_drop_database(TC_PARSE_RESULT *tc_parse_result_t);
void tc_parse_remote_change_database(TC_PARSE_RESULT *tc_parse_result_t);

void tc_parse_spider_alter_table(
  TC_PARSE_RESULT *tc_parse_result_t
);

void tc_parse_remote_alter_table(
  TC_PARSE_RESULT *tc_parse_result_t
);

void tc_parse_spider_create_or_drop_index(
  TC_PARSE_RESULT *tc_parse_result_t
);
void tc_parse_remote_create_or_drop_index(
  TC_PARSE_RESULT *tc_parse_result_t
);

bool tc_query_parse(
  THD *thd, 
  LEX *lex, 
  TC_PARSE_RESULT *tc_parse_result_t
);


bool tc_command_convert(THD *thd, LEX *lex, TC_PARSE_RESULT *tc_parse_result_t);
bool tc_dry_run_command(THD *thd, TC_PARSE_RESULT *parse_result);

MYSQL* tc_conn_connect(
  string ipport, 
  string user, 
  string passwd
);

MYSQL *tc_conn_connect(const AUTH_INFO &auth);

MYSQL *tc_conn_connect(const string &host, uint port, const string &user,
                       const string &passwd, const string &wrapper);

map<string, MYSQL*> tc_remote_conn_connect(
  int &ret, 
  map<string, string> remote_ipport_map, 
  map<string, string> remote_user_map, 
  map<string, string> remote_passwd_map);

map<string, MYSQL*> tc_tdbctl_conn_connect(
  int &ret,
  map<string, string> tdbctl_ipport_map, 
  map<string, string> tdbctl_user_map,
  map<string, string> tdbctl_passwd_map
);

MYSQL *tc_tdbctl_conn_primary(
	int &ret,
	map<string, string> &tdbctl_ipport_map,
	map<string, string> &tdbctl_user_map,
	map<string, string> &tdbctl_passwd_map
);

int tc_do_grants_internal(THD *thd, LEX *lex);
/**
 * @brief fill the options of alter node. These options
 *        include host,port,user,password
 * 
 * @param lex 
 * @param server 
 */
void fill_lex_to_alter_node(LEX* lex, FOREIGN_SERVER *server);
Server_options foreign_server_to_server_options(FOREIGN_SERVER *fs);

set<string> get_spider_ipport_set(
  MEM_ROOT *mem, 
  map<string, string> &spider_user_map, 
  map<string, string> &spider_passwd_map, 
  bool with_slave,
  string wrapper_name = SPIDER_WRAPPER
);

map<string, string> get_remote_ipport_map(
  MEM_ROOT *mem, 
  map<string, string> &remote_user_map, 
  map<string, string> &remote_passwd_map,
  bool with_slave = false
);

map<string, string> get_server_name_map(
	MEM_ROOT *mem,
	const char *wrapper,
	bool with_slave
);

void get_server_name_set(
	MEM_ROOT *mem,
  std::set<std::string> &server_set, 
	const char* wrapper
);

map<string, string> get_tdbctl_ipport_map(
	MEM_ROOT *mem,
	map<string, string> &tdbctl_user_map,
	map<string, string> &tdbctl_passwd_map
);


bool tc_conn_free( map<string, MYSQL*> &conn_map);
int tc_mysql_next_result(MYSQL* mysql);

/**
 * @brief send sql statement to multiple nodes and execute the sql parallelly.
 *        each execution without error_retry
 * 
 * @param exec_sql 
 * @param conn_map 
 * @param result_map 
 * @return true means failure
 * @return false means success
 */
bool tc_exec_sql_paral(
    string exec_sql,
    map<string, MYSQL *> &conn_map,
    map<string, tc_exec_info> &result_map);

bool tc_exec_sql_paral(string exec_sql, map<string, MYSQL*>& conn_map,
  map<string, tc_exec_info>& result_map,
  map<string, string> user_map,
  map<string, string> passwd_map,
  bool error_retry);

bool tc_reconnect(string ipport,
  map<string, MYSQL*>& spider_conn_map,
  map<string, string> spider_user_map,
  map<string, string> spider_passwd_map);

bool tc_exec_sql_up(MYSQL* mysql, string sql, tc_exec_info* exec_info);
MYSQL_RES* tc_exec_sql_with_result(MYSQL* mysql, string sql);
bool tc_exec_sql_without_result(MYSQL* mysql, string sql, tc_exec_info* exec_info);
MYSQL_RES* tc_exec_sql_up_with_result(MYSQL* mysql, string sql, MYSQL_RES** res);
MYSQL_RES* tc_exec_sql_with_result(MYSQL* mysql, string sql);
bool tc_exec_sql_paral_with_result(
  string exec_sql,
  map<string, MYSQL*>& conn_map,
  map<string, MYSQL_RES*>& result_map,
  map<string, string>& user_map,
  map<string, string>& passwd_map,
  bool error_retry);
map<string, string> get_server_uuid_map(
	int &ret,
	MEM_ROOT *mem,
	const char* wrapper,
	bool with_slave
);
string tc_get_server_name(
	int &ret,
	MEM_ROOT *mem,
	const char* wrapper,
	bool with_slave);

string tc_get_user_name(
	int &ret,
	const char* wrapper,
	bool with_slave);

string tc_get_spider_grant_sql(
	set<string> &spider_ipport_set,
	map<string, string> &spider_user_map,
	map<string, string> &spider_passwd_map,
	map<string, string> &tdbctl_ipport_map,
	map<string, string> &tdbctl_user_map,
	map<string, string> &tdbctl_passwd_map);

string tc_get_tdbctl_grant_sql(
	set<string> &spider_ipport_set,
	map<string, string> &spider_user_map,
	map<string, string> &spider_passwd_map,
	map<string, string> &tdbctl_ipport_map,
	map<string, string> &tdbctl_user_map,
	map<string, string> &tdbctl_passwd_map);

string tc_get_remote_grant_sql(
	set<string> &spider_ipport_set,
	map<string, string> &spider_user_map,
	map<string, string> &spider_passwd_map,
	map<string, string> &remote_ipport_map,
	map<string, string> &remote_user_map,
	map<string, string> &remote_passwd_map,
	map<string, string> &tdbctl_ipport_map,
	map<string, string> &tdbctl_user_map,
	map<string, string> &tdbctl_passwd_map);

my_time_t string_to_timestamp(const string s);
void init_result_map(map<string, tc_exec_info>& result_map, set<string> &ipport_set);
void init_result_map2(map<string, tc_exec_info>& result_map, map<string, string> &ipport_map);
string concat_result_map(map<string, tc_exec_info> result_map);

string tc_get_variable_value(MYSQL *conn, const char *variable);

enum enum_ident_wrapper_check
{
	 IDENT_WRAPPER_OK,
	 IDENT_WRAPPER_WRONG,
};


enum_ident_wrapper_check tc_check_wrapper_name(LEX_STRING *org_name);

/* use GROUP REPLICATION's info */
extern char *report_host;
extern uint report_port;

int tc_is_primary_tdbctl_node();
uint tc_get_primary_node(std::string &host, uint *port);
bool check_server_version(ulong& server_version);
void free_thd_connection(THD *thd);

int checked_getaddrinfo(const char *nodename, const char *servname, const struct addrinfo *hints, struct addrinfo **res);

/**
 This function translates hostnames to IP addresses.

 @param[in] host The hostname to translate.
 @param[out] ip  The IP address after translation.
 @return false on success, true otherwise.
 */
bool
get_ipv4_addr_from_hostname(const std::string& host, std::string& ip);

/**
  This function gets all network addresses on this host. Linux host ip only.
 @param[out] out local IP address
 @param filter_out_inactive If set to true, only active interfaces will be added
                            to out
 @return false on sucess, true otherwise.
 */
bool
get_ip_local_addresses(std::set<std::string>& out,
                         bool filter_out_inactive= false);

/**
 * @brief check validity of the host stored into mysql.servers.
 * mysql.servers can't contain both loopback network 
 * address and external network address, please change the value of 'host' column"
 * 
 * @param mem mem_root
 * @param server_host the host of server node
 * @return true on valid, false on invalid
 */
bool verify_validity_of_routing_host(MEM_ROOT *mem, const char *server_host);

/**
 * @brief parse the result from the mysqlconn, and then send 
newly constructed result set to client.
 * 
 * @param thd Thread handler
 * @param res MYSQL_RES
 * @param server_name the mysql_result from 
 * 
 * @retval 0 on success
 * @retval 1 on error 
 */
int tc_store_mysql_result_into_protocol(THD *thd, MYSQL_RES *res);

/**
 * @brief build a item according to field_type
 * 
 * @param field MYSQL_FIELD
 * @return Item* 
 */
Item* tc_make_item(MYSQL_FIELD *field);

/**
 * @brief store the row into protocol
 * 
 * @param protocol Protocol
 * @param field  MYSQL_FIELD
 * @param row 
 * @param length the length of the row
 */
void protocol_store_field(Protocol *protocol, MYSQL_FIELD &field, const char *row, ulong length);

/**
 * @brief clean MYSQL_RESULT* of exec_result.result_info
 * 
 * @param exec_result TC_EXEC_RESULT*
 */
void tc_clean_exec_result(TC_EXEC_RESULT* exec_result);

/**
 * @brief tc_command is disabled when tc_admin == 0
 * 
 * @param tc_admin 
 * @param lex 
 * @retval true means tc_command is allowed to execute
 * @retval false means tc_command is not allowed
 */
bool check_tc_command(bool tc_admin, LEX *lex);

void tc_real_query(Query_exec_manager *query_mgr, const string &server_name,
                   MYSQL *mysql, enum_node_type node_type);

/**
 * @brief get query results from mysql connection.
 *        query_mgr->store_result() only store the last query result from mysql connection.
 * 
 * @param query_mgr 
 * @param server_name 
 * @param mysql Mysql connection
 * @param node_type 
 */
void tc_get_query_result(Query_exec_manager *query_mgr, const string &server_name,
                         MYSQL *mysql, enum_node_type node_type);
bool tc_exec_query_paral(Query_exec_manager *query_mgr,
                      const std::map<std::string, MYSQL *> &conns,
                      enum_node_type node_type);

/**
 * @brief execute the prepared sql statements on [SPIDER|SPIDER_SLAVE|REMOTE|REMOTE_SLAVE] nodes,
 *        according to the exec_flag(query_mgr->get_exec_flag()). 
 * 
 * @param thd 
 * @param conn_mgr 
 * @param query_mgr 
 * @retval true  
 * @retval false 
 */
bool tc_run_command(THD *thd, Cluster_conn_manager *conn_mgr,
                Query_exec_manager *query_mgr);
const char *get_wrapper_name_by_node_type(enum_node_type type);

/**
 * @brief Handle commands regarding Tdbctl Primary Mode, including:
 *    - TDBCTL ENABLE PRIMARY [FORCE]
 *    - TDBCTL DISABLE PRIMARY
 *    - TDBCTL GET PRIMARY
 *
 * @param thd
 * @param lex
 *
 * @retval TRUE on error, FALSE on success
 * */
int tdbctl_handle_primary_cmd(THD *thd, LEX *lex);

bool tdbctl_check_table(THD *thd, TABLE_LIST *tables);

int tdbctl_check_tables(THD *thd, const char *db, String *wild);

bool tdbctl_check_routing(THD *thd);

#endif /* TC_BASE_INCLUDED */
