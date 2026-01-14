#ifndef TC_FORWARDING_RULE_MGR_INCLUDED
#define TC_FORWARDING_RULE_MGR_INCLUDED


#include "my_global.h"
#include "my_sqlcommand.h"
#include "set_var.h" 
#include <string>
#include <vector>
#include <map>

/* forward declarations */
class THD;
class LEX;
class set_var;
class Json_dom;
class Json_object;


std::string string_strip(const std::string &str);

std::string trim_and_uppercase(const std::string &str);

typedef unsigned int Exec_Flag;

typedef struct Supported_sql_command
{
  const char *name;
  enum_sql_command sql_type;
  Exec_Flag primary_default_rule;  // default forwarding rule for the sql on primary tdbctl node.
  Exec_Flag secondary_default_rule;  // default forwarding rule for the sql on secondary tdbctl node.
  Exec_Flag max_supported_rule;  // max supported forwarding rule for the sql on primary tdbctl node.
  bool unchangeable;  // true means forwarding rule is unchangeable.
  const char *tips;
} Supported_SQL_Command;

typedef struct Supported_execute_flag 
{
  const char *name;
  Exec_Flag single_flag;
  bool cannot_set;  // ture means user cannot set this flag.
} Supported_Exec_Flag;


#define UNCHANGEABLE_DDL "Cannot change forwarding rules for DDL sql commands."
#define UNCHANGEABLE_TDBCTL_SQL "Cannot change forwarding rules for sql commands like \'TC_SQLCOM_XXX\'."

/**
  Store the default forwarding rules for sql commands.
*/
extern Supported_SQL_Command supported_sql_commands[];
extern const unsigned int supported_sql_count;

/*
  Store the supported execute flags.
*/
extern Supported_Exec_Flag supported_execute_flag[];
extern const unsigned int settable_flag_count;

/*
  return true if the sql_name is found in supported_sql_commands, else return false;
*/
bool get_sql_command_by_name(const char *sql_name, Supported_SQL_Command &sql_command);

/*
  return true if the flag_name is found in supported_execute_flag, else return false;
*/
bool get_execute_flag_by_name(const char *flag_name, Supported_Exec_Flag &exec_flag_item);

/*
  return false if conflicting execute flags exist, else return true.
*/
bool check_conflict_exec_flag(std::vector<Exec_Flag> &exec_flag_list);

/*
  return false if execute flags that are out of range exist, else return true.
*/
bool check_exec_flag_range(std::vector<Exec_Flag> &exec_flag_list, Exec_Flag max_supported_rule);

/*
  return true if the forwarding rule for the specified system variable cannot be modified, else return false.
*/
bool is_sys_var_unchangeable(sys_var *sysvar, std::string *tips=NULL);

/*
  return name of sql command if given enum value exists, else return empty string.
*/
std::string to_sql_command_name(enum_sql_command sql);

/*
  return name of execute flag if given single execute flag exists, else return empty string.
*/
std::string to_single_exec_flag_name(Exec_Flag flag);

/*
  Convert execute flag to a comma-separated flag string.
*/
std::string exec_flag_to_string(Exec_Flag flag, const char *separater=", ");


/**
  Forwarding_rule_mgr
  ---------------
  This class holds an object for THD class to cover forwarding rules of supported sql command.
*/
class Forwarding_rule_mgr
{
  typedef std::pair<enum enum_sql_command, std::vector<Exec_Flag> > SQL_Rule;
  typedef std::vector<SQL_Rule> SQL_Rule_Cache;
  typedef std::pair<std::string, std::vector<Exec_Flag> > Sys_Var_Rule;
  typedef std::vector<Sys_Var_Rule> Var_Rule_Cache; 
  typedef std::map<std::string, Exec_Flag> Var_Rule_Hash;
private:

  /* Mutex to protect access to global forwarding rules */
  static mysql_mutex_t LOCK_global_rules;

  /*
    Class members used to store the sql-level forwarding rules.
  */

  /* Store the checked session forwarding rules (Need storage between check and update). */
  SQL_Rule_Cache m_session_rules_cache; 
  /* Store the checked global forwarding rules (Need storage between check and update). */
  SQL_Rule_Cache m_global_rules_cache;
  /* Store the checked global forwarding rules. It will only be changed if the forwarding rules is verified as valid. */
  static SQL_Rule_Cache m_global_rules_valid_cache;
  /* Store the latest forwarding rules of primary tdbctl node. sql_command -> exec_flag */
  std::vector<Exec_Flag> m_primary_rules;
  /* Store the latest forwarding rules of secondary tdbctl node. sql_command -> exec_flag */
  std::vector<Exec_Flag> m_secondary_rules;

  /*
    Class members used to store the sysvar-level forwarding rules.
  */

  /* Store the checked session system variables' forwarding rules (Need storage between check and update). */
  Var_Rule_Cache m_session_var_rules_cache;
  /* Store the checked global system variables' forwarding rules (Need storage between check and update). */
  Var_Rule_Cache m_global_var_rules_cache;
  /* Store the checked global system variables' forwarding rules. It will only be changed if the forwarding rules is verified as valid. */
  static Var_Rule_Cache m_global_var_rules_valid_cache;
  /* Store the session-level new forwarding rules of specified system variables on primary tdbctl node. var_name -> exec_flag */
  Var_Rule_Hash m_primary_var_rules;

  static bool parse_forwarding_rules(const char *json_text, size_t json_text_len, std::string &err_str, 
                                  SQL_Rule_Cache *rules_cache=NULL, std::string *new_json_str=NULL);
  static bool parse_sysvar_rules(const char *json_text, size_t json_text_len, std::string &err_str, std::string &wrn_str, 
                                  THD *thd=NULL, Var_Rule_Cache *rules_cache=NULL, std::string *new_json_str=NULL);
  static Json_dom * parse_json_text_to_dom(const char *json_text, size_t json_text_len, std::string &err_str);
  static bool parse_exec_flag_list(const char *text, size_t text_len, std::string &err_str, std::vector<Exec_Flag> &exec_flag_list, 
                                  std::string *new_str=NULL);

  void reset_primary_rules();
  void cover_primary_forwarding_rules(const SQL_Rule_Cache &rules_cache);
  void reset_primary_var_rules();
  void cover_primary_var_rules(const Var_Rule_Cache &rules_cache);

  Exec_Flag get_var_execute_flag(set_var_base *var);
public:

  /** Constructor */
  Forwarding_rule_mgr();

  /** Destructor */
  ~Forwarding_rule_mgr() {
  }

private:
  /* disable copy assignment constructor. */
  Forwarding_rule_mgr(Forwarding_rule_mgr const &other);
  Forwarding_rule_mgr& operator= (Forwarding_rule_mgr const &rhs);

public:
  bool get_sql_execute_flag(THD *thd, LEX *lex, enum_sql_command sql_cmd, Exec_Flag &execute_flag);

  static bool check_forwarding_rules(THD *thd, set_var *var, std::string &err_msg);
  static bool update_forwarding_rules(THD *thd, enum_var_type type);

  static bool check_var_rules(THD *thd, set_var *var, std::string &err_msg, std::string &warn_msg);
  static bool update_var_rules(THD *thd, enum_var_type type);

  static bool server_boot_verify_forwarding_rules(const char *json_text, size_t json_text_len, std::string &err_str);
  static bool server_boot_verify_variable_rules(const char *json_text, size_t json_text_len, std::string &err_str, std::string &wrn_str);

  void init();
};

class Plugin_Locker {
  Plugin_Locker() {
    lock_plugin_mutex();
  }

  ~Plugin_Locker() {
    unlock_plugin_mutex();
  }
};

#endif /* TC_FORWARDING_RULE_MGR_INCLUDED */