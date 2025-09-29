#ifndef TC_RESTORE_INCLUDED
#define TC_RESTORE_INCLUDED

#include "my_global.h"
#include "my_sys.h"
#include "sql_servers.h"
#include <string>
#include <cstdio>

using std::string;

typedef struct st_line_buffer
{
  File file;
  char *buffer;			/* The buffer itself, grown as needed. */
  char *end;			/* Pointer at buffer end */
  char *start_of_line, *end_of_line;
  uint bufread;			/* Number of bytes to get with each read(). */
  uint eof;
  ulong max_size;
  ulong read_length;		/* Length of last read string */
  int error;
  bool truncated;
} LINE_BUFFER;


typedef struct st_status
{
  int exit_status;
  ulong query_start_line;
  char *file_name;
  LINE_BUFFER *line_buff;
  bool batch, add_to_history;
} STATUS;


class Restore_Handler
{

  /* A structure which contains information on the commands this program
    can understand. */
  typedef struct {
    const char *name;		/* User printable name of the function. */
    char cmd_char;		/* msql command character */
    int (*func)(Restore_Handler *obj, 
                String *str,char *); /* Function to call to do the job. */
    bool takes_params;		/* Max parameters for command */
    const char *doc;		/* Documentation for this function.  */
  } COMMANDS;
  static COMMANDS commands[];
  
  class MY_FILE_GUARD {
    public:
      MY_FILE_GUARD(FILE *file) : file(file) {}
      ~MY_FILE_GUARD() { 
      if (file && file != stdout)
        my_fclose(file, MYF(0));
      }
    private:
        FILE *file;
  };

  enum enum_info_type { INFO_INFO, INFO_ERROR, INFO_RESULT};
  typedef enum enum_info_type INFO_TYPE;

private:
  const Server_options &server_options;
  string current_db, current_host, current_user;
  const string sql_file_name;
  const string output_file_name;
  FILE *sql_file = NULL;
  FILE *output_file = NULL;
  MYSQL mysql;
  STATUS status;

  int delimiter_index= -1;
  int charset_index= -1;
  char delimiter[16];
  bool connected = false;
  const CHARSET_INFO *charset_info= &my_charset_latin1;
  uint verbose = 0;
  bool real_binary_mode = false;
  String glob_buffer;
  bool named_cmds = false;
  bool preserve_comments = false;
  int interrupted_query = 0;
  bool opt_reconnect = false;
  bool opt_rehash = true;
  bool skip_updates = false;
  int executing_query = 0;
  int show_warnings = 0;
  int quick = 0;
  int opt_silent = 0;
  bool unbuffered = false;
  bool column_types_flag = false;
  int vertical = 0;
  bool column_names = true;
  bool opt_binhex = false;
  bool output_tables = false;
  bool opt_raw_data = false;

  bool opt_connect_expired_password = false;
  char *opt_init_command= NULL;
  ulong opt_connect_timeout = 0;
  char *opt_bind_addr = NULL;
  bool opt_compress = false;
  bool using_opt_local_infile = false;
  uint opt_local_infile = 0;
  uint opt_protocol=0;
  bool safe_updates = false;
  ulong select_limit = 1000L;
  ulong max_join_size = 1000000L;
  char *default_charset= (char*) MYSQL_AUTODETECT_CHARSET_NAME;
  char *opt_plugin_dir= 0;
  char *opt_default_auth= 0;
  bool using_opt_enable_cleartext_plugin = false;
  uint opt_enable_cleartext_plugin = 0;
  bool one_database = false;

private:
  static inline int get_command_index(char cmd_char);
  char *get_arg(char *line, my_bool get_next_arg);
  void print_help_item(MYSQL_ROW *cur, int num_name, 
                      int num_cat, char *last_char);
  int normalize_dbname(const char *line, char *buff, uint buff_size);
  void get_current_db();
  int com_server_help(String *buffer MY_ATTRIBUTE((unused)), 
                      char *line MY_ATTRIBUTE((unused)), 
                      char *help_arg);
  int com_help(String *buffer MY_ATTRIBUTE((unused)), 
              char *line MY_ATTRIBUTE((unused)));
  int com_clear(String *buffer,
                char *line MY_ATTRIBUTE((unused)));
  int com_connect(String *buffer, char *line);
  int com_delimiter(String *buffer MY_ATTRIBUTE((unused)),
                    char *line);
  int com_edit(String *buffer,char *line MY_ATTRIBUTE((unused)));
  int com_ego(String *buffer,char *line);
  int com_quit(String *buffer MY_ATTRIBUTE((unused)),
              char *line MY_ATTRIBUTE((unused)));
  int com_go(String *buffer, char *line MY_ATTRIBUTE((unused)));
  int com_nopager(String *buffer MY_ATTRIBUTE((unused)),
                  char *line MY_ATTRIBUTE((unused)));
  int com_notee(String *buffer MY_ATTRIBUTE((unused)),
                char *line MY_ATTRIBUTE((unused)));
  int com_pager(String *buffer MY_ATTRIBUTE((unused)),
                char *line MY_ATTRIBUTE((unused)));
  int com_print(String *buffer,
                char *line MY_ATTRIBUTE((unused)));
  int com_prompt(String *buffer MY_ATTRIBUTE((unused)),
                char *line);
  int com_rehash(String *buffer MY_ATTRIBUTE((unused)),
	              char *line MY_ATTRIBUTE((unused)));
  int com_source(String *buffer MY_ATTRIBUTE((unused)),
                char *line);
  int com_status(String *buffer MY_ATTRIBUTE((unused)),
                char *line MY_ATTRIBUTE((unused)));
  int com_shell(String *buffer MY_ATTRIBUTE((unused)),
                char *line MY_ATTRIBUTE((unused)));
  int com_tee(String *buffer MY_ATTRIBUTE((unused)),
              char *line MY_ATTRIBUTE((unused)));
  int com_use(String *buffer MY_ATTRIBUTE((unused)),
              char *line);
  int com_charset(String *buffer MY_ATTRIBUTE((unused)),
              char *line);
  int com_warnings(String *buffer MY_ATTRIBUTE((unused)),
              char *line MY_ATTRIBUTE((unused)));
  int com_nowarnings(String *buffer MY_ATTRIBUTE((unused)),
              char *line MY_ATTRIBUTE((unused)));
  int com_resetconnection(String *buffer MY_ATTRIBUTE((unused)),
                          char *line MY_ATTRIBUTE((unused)));

#define DEFINE_STATIC_FUNC(func_name, member_func) \
  static inline int func_name(Restore_Handler *obj, String *str, char *line) { \
    return obj->member_func(str, line); \
  }

DEFINE_STATIC_FUNC(com_help_static, com_help)
DEFINE_STATIC_FUNC(com_clear_static, com_clear)
DEFINE_STATIC_FUNC(com_connect_static, com_connect)
DEFINE_STATIC_FUNC(com_delimiter_static, com_delimiter)
DEFINE_STATIC_FUNC(com_edit_static, com_edit)
DEFINE_STATIC_FUNC(com_ego_static, com_ego)
DEFINE_STATIC_FUNC(com_quit_static, com_quit)
DEFINE_STATIC_FUNC(com_go_static, com_go)
DEFINE_STATIC_FUNC(com_nopager_static, com_nopager)
DEFINE_STATIC_FUNC(com_notee_static, com_notee)
DEFINE_STATIC_FUNC(com_pager_static, com_pager)
DEFINE_STATIC_FUNC(com_print_static, com_print)
DEFINE_STATIC_FUNC(com_prompt_static, com_prompt)
DEFINE_STATIC_FUNC(com_rehash_static, com_rehash)
DEFINE_STATIC_FUNC(com_source_static, com_source)
DEFINE_STATIC_FUNC(com_status_static, com_status)
DEFINE_STATIC_FUNC(com_shell_static, com_shell)
DEFINE_STATIC_FUNC(com_tee_static, com_tee)
DEFINE_STATIC_FUNC(com_use_static, com_use)
DEFINE_STATIC_FUNC(com_charset_static, com_charset)
DEFINE_STATIC_FUNC(com_warnings_static, com_warnings)
DEFINE_STATIC_FUNC(com_nowarnings_static, com_nowarnings)
DEFINE_STATIC_FUNC(com_resetconnection_static, com_resetconnection)

private:
  void init_status();
  int put_info(const char *str, INFO_TYPE info_type, 
              uint error=0, const char *sqlstate=0);
  int put_error(MYSQL *con);
  void print_warnings();
  void print_as_hex(FILE *output_file, const char *str, 
                    ulong len, ulong total_bytes_to_send);
  void tee_write(FILE *file, const char *s, 
                  size_t slen, int flags);
  void tee_print_sized_data(const char *data, 
                            unsigned int data_length, 
                            unsigned int total_bytes_to_send, 
                            bool right_justified);
  void init_connection_options(MYSQL *mysql);
  int sql_real_connect(const char *host, unsigned int port,
                      const char *user, const char *password,
                      const char *database);
  COMMANDS *find_command(char cmd_char);
  COMMANDS *find_command(char *name);
  int mysql_real_query_for_lazy(const char *buf, size_t length);
  int mysql_store_result_for_lazy(MYSQL_RES **result);
  int reconnect();
  void print_table_data_vertically(MYSQL_RES *result);
  void safe_put_field(const char *pos,ulong length);
  void print_tab_data(MYSQL_RES *result);
  void print_field_types(MYSQL_RES *result);
  void print_table_data(MYSQL_RES *result);
  bool add_line(String &buffer, char *line, size_t line_length,
                char *in_string, bool *ml_comment, bool truncated);
  void remove_cntrl(String &buffer);
  int read_and_execute();
  void mysql_end(int sig);

public:
  Restore_Handler(const Server_options &server_options,
                  const string &sql_file_name,
                  const string &output_file_name);
  bool import_data_main(string &errmsg) noexcept;
};


#endif