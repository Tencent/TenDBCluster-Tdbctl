--echo Set general log for all cluster nodes.
--disable_query_log
--connection tdbctl0
SET tc_admin=OFF;
SET @old_log_output = @@GLOBAL.log_output;
SET @old_general_log = @@GLOBAL.general_log;
SET GLOBAL log_output = 'TABLE';
SET GLOBAL general_log = ON;
--connection tdbctl1
SET tc_admin=OFF;
SET @old_log_output = @@GLOBAL.log_output;
SET @old_general_log = @@GLOBAL.general_log;
SET GLOBAL log_output = 'TABLE';
SET GLOBAL general_log = ON;
--connection tdbctl2
SET tc_admin=OFF;
SET @old_log_output = @@GLOBAL.log_output;
SET @old_general_log = @@GLOBAL.general_log;
SET GLOBAL log_output = 'TABLE';
SET GLOBAL general_log = ON;
--connection spider0
SET @old_log_output = @@GLOBAL.log_output;
SET @old_general_log = @@GLOBAL.general_log;
SET GLOBAL log_output = 'TABLE';
SET GLOBAL general_log = ON;
--connection spider1
SET @old_log_output = @@GLOBAL.log_output;
SET @old_general_log = @@GLOBAL.general_log;
SET GLOBAL log_output = 'TABLE';
SET GLOBAL general_log = ON;
--connection spider2
SET @old_log_output = @@GLOBAL.log_output;
SET @old_general_log = @@GLOBAL.general_log;
SET GLOBAL log_output = 'TABLE';
SET GLOBAL general_log = ON;
--connection remote_master0
SET @old_log_output = @@GLOBAL.log_output;
SET @old_general_log = @@GLOBAL.general_log;
SET GLOBAL log_output = 'TABLE';
SET GLOBAL general_log = ON;
--connection remote_master1
SET @old_log_output = @@GLOBAL.log_output;
SET @old_general_log = @@GLOBAL.general_log;
SET GLOBAL log_output = 'TABLE';
SET GLOBAL general_log = ON;
--enable_query_log
