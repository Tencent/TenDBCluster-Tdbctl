--echo Set general log to original state for all cluster nodes.
--disable_query_log
--connection tdbctl0
SET tc_admin=OFF;
SET GLOBAL log_output = @old_log_output;
SET GLOBAL general_log = @old_general_log;
--connection tdbctl1
SET tc_admin=OFF;
SET GLOBAL log_output = @old_log_output;
SET GLOBAL general_log = @old_general_log;
--connection tdbctl2
SET tc_admin=OFF;
SET GLOBAL log_output = @old_log_output;
SET GLOBAL general_log = @old_general_log;
--connection spider0
SET GLOBAL log_output = @old_log_output;
SET GLOBAL general_log = @old_general_log;
--connection spider1
SET GLOBAL log_output = @old_log_output;
SET GLOBAL general_log = @old_general_log;
--connection spider2
SET GLOBAL log_output = @old_log_output;
SET GLOBAL general_log = @old_general_log;
--connection remote_master0
SET GLOBAL log_output = @old_log_output;
SET GLOBAL general_log = @old_general_log;
--connection remote_master1
SET GLOBAL log_output = @old_log_output;
SET GLOBAL general_log = @old_general_log;
--enable_query_log
