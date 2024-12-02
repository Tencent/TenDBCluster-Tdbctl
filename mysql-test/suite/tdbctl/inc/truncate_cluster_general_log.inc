--connection tdbctl0
SET tc_admin=OFF;
TRUNCATE TABLE mysql.general_log;
--connection spider0
TRUNCATE TABLE mysql.general_log;
--connection spider1
TRUNCATE TABLE mysql.general_log;
--connection spider2
TRUNCATE TABLE mysql.general_log;
--connection remote_master0
TRUNCATE TABLE mysql.general_log;
--connection remote_master1
TRUNCATE TABLE mysql.general_log;