/*
    Copyright (C) 2020 Tencent.  All rights reserved.
*/

#ifndef TC_SHOW_INCLUDE
#define TC_SHOW_INCLUDE

#include "my_global.h"
#include "sql_class.h"
#include <vector>
#include <string>

int tc_show_processlist(THD *thd, bool verbose, LEX_CSTRING from_server);
int tc_show_variables(THD *thd, enum_var_type type, String *wild,
                      LEX_CSTRING from_server);

class tc_node_info {
public:
  enum Node_Status {
    NODE_STATUS_UNREACHABLE = 0,
    NODE_STATUS_ONLINE,
    NODE_STATUS_UNKNOWN
  };

  /* node basic info */
  std::string server_name;
  std::string host;
  uint port;
  std::string user;
  std::string passwd;
  std::string wrapper;

  /* node status */
  Node_Status status;

  /* node version string */
  std::string version;

  /* node other info */
  std::vector<std::pair<std::string, std::string>> feature_info;

public:
  static const char *NODE_INFO_UNKNOWN_STR;
  static const char *NODE_INFO_QUERY_FAILED_STR;
  static const char *NODE_INFO_WRONG_FILED_INDEX;
  static const char *NODE_INFO_KEY_SLAVE_STATUS;
  static const char *NODE_INFO_KEY_MASTER_NAME;

  tc_node_info() : status(NODE_STATUS_UNKNOWN), 
                   version(NODE_INFO_UNKNOWN_STR) {} 
  
  inline void set_basic_info(const std::string &server_name,
                            const std::string &host, 
                            uint port, 
                            const std::string &user,
                            const std::string &passwd, 
                            const std::string &wrapper) 
  {
    this->server_name = server_name;
    this->host = host;
    this->port = port;
    this->user = user;
    this->passwd = passwd;
    this->wrapper = wrapper;
  }

  /* Make slave status json string */
  static void make_slave_status_json(std::string master_host,
                                    std::string master_port,
                                    std::string slave_io_running,
                                    std::string slave_sql_running,
                                    std::string relay_master_log_file,
                                    std::string exec_master_log_pos,
                                    std::string &slave_status_str)
  {
    slave_status_str = "{";
    slave_status_str += "\"Master_Host\": \"" + master_host + "\", ";
    slave_status_str += "\"Master_Port\": " + master_port + ", ";
    slave_status_str += "\"Slave_IO_Running\": \"" + slave_io_running + "\", ";
    slave_status_str += "\"Slave_SQL_Running\": \"" + slave_sql_running + "\", ";
    slave_status_str += "\"Relay_Master_Log_File\": \"" + relay_master_log_file + "\", ";
    slave_status_str += "\"Exec_Master_Log_Pos\": \"" + exec_master_log_pos + "\"";
    slave_status_str += "}";
  }

  std::string get_node_status() const;

  void make_feature_info_json(std::string &feature_info_json) const;

};

void tc_get_cluster_nodes_info(THD *thd, bool verbose, std::vector<tc_node_info> &node_infos);
bool tc_show_cluster_nodes(THD *thd, bool verbose);
#endif
