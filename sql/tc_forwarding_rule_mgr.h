#ifndef TC_FORWARDING_RULE_MGR_INCLUDED
#define TC_FORWARDING_RULE_MGR_INCLUDED


#include "my_global.h"
#include "my_sqlcommand.h"
#include "set_var.h" 
#include <string>
#include <vector>

/* forward declarations */
class THD;
class LEX;
class set_var;
class Json_dom;
class Json_object;


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
  return name of sql command if given enum value exist, else return empty string.
*/
std::string to_sql_command_name(enum_sql_command sql);

/*
  return name of execute flag if given single execute flag exist, else return empty string.
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
  typedef std::vector<std::pair<enum enum_sql_command, std::vector<Exec_Flag> > > Rule_Cache;
private:
  /* Store the checked session forwarding rules (Need storage between check and update). */
  Rule_Cache m_session_rules_cache; 

  /* Store the checked global forwarding rules. It will only be changed if the forwarding rules is verified as valid. */
  static Rule_Cache m_global_rules_cache;

  /* Store the latest forwarding rules of primary tdbctl node. sql_command -> exec_flag */
  std::vector<Exec_Flag> m_primary_rules;  

  /* Store the latest forwarding rules of secondary tdbctl node. sql_command -> exec_flag */
  std::vector<Exec_Flag> m_secondary_rules;

  static bool parse_forwarding_rules(const char *json_text, size_t json_text_len, std::string &err_str, Rule_Cache *rules_cache=NULL, std::string *new_json_str=NULL);
  void cover_primary_forwarding_rules(const Rule_Cache &rules_cache);

public:

  /** Constructor */
  Forwarding_rule_mgr();

  /** Destructor */
  ~Forwarding_rule_mgr() {

  }

  /* disable copy assignment constructor. */
  Forwarding_rule_mgr(Forwarding_rule_mgr const &other) = delete;
  Forwarding_rule_mgr& operator= (Forwarding_rule_mgr const &rhs) = delete;

  Exec_Flag get_sql_execute_flag(THD *thd, LEX *lex, enum_sql_command sql_cmd);

  void reset_primary_rules();
  static bool check_forwarding_rules(THD *thd, set_var *var);
  static bool server_boot_verify(const char *json_text, size_t json_text_len, std::string &err_str);
  static bool update_forwarding_rules(THD *thd, enum_var_type type);

  void init();
};

#endif /* TC_FORWARDING_RULE_MGR_INCLUDED */