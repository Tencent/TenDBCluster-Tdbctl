--echo execute 'TRUNCATE TABLE mysql.general_log;' on all cluster nodes.
--disable_query_log
--connection tdbctl0
SET tc_admin=OFF;
SET GLOBAL general_log = OFF; 
TRUNCATE TABLE mysql.general_log;
SET GLOBAL general_log = ON; 
--connection spider0
SET GLOBAL general_log = OFF; 
TRUNCATE TABLE mysql.general_log;
SET GLOBAL general_log = ON; 
--connection spider1
SET GLOBAL general_log = OFF; 
TRUNCATE TABLE mysql.general_log;
SET GLOBAL general_log = ON; 
--connection spider2
SET GLOBAL general_log = OFF; 
TRUNCATE TABLE mysql.general_log;
SET GLOBAL general_log = ON; 
--connection remote_master0
SET GLOBAL general_log = OFF; 
TRUNCATE TABLE mysql.general_log;
SET GLOBAL general_log = ON; 
--connection remote_master1
SET GLOBAL general_log = OFF; 
TRUNCATE TABLE mysql.general_log;
SET GLOBAL general_log = ON; 
--enable_query_log
