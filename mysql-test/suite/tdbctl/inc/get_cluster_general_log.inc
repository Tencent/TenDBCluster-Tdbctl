--echo --------------------
--connection tdbctl0
SELECT argument AS tdbctl0_log FROM mysql.general_log WHERE (user_host LIKE '%172.18.0.9%' OR thread_id = CONNECTION_ID()) AND argument NOT LIKE '%tdbctl0_log%' AND command_type NOT LIKE 'Connect';
--echo --------------------
--connection spider0
SELECT argument AS spider0_log FROM mysql.general_log WHERE user_host LIKE '%172.18.0.9%' AND argument NOT LIKE '%spider0_log%' AND command_type NOT LIKE 'Connect';
--echo --------------------
--connection spider1
SELECT argument AS spider1_log FROM mysql.general_log WHERE user_host LIKE '%172.18.0.9%' AND argument NOT LIKE '%spider1_log%' AND command_type NOT LIKE 'Connect';
--echo --------------------
--connection spider2
SELECT argument AS spider2_log FROM mysql.general_log WHERE user_host LIKE '%172.18.0.9%' AND argument NOT LIKE '%spider2_log%' AND command_type NOT LIKE 'Connect';
--echo --------------------
--connection remote_master0
SELECT argument AS remote_master0_log FROM mysql.general_log WHERE user_host LIKE '%172.18.0.9%' AND argument NOT LIKE '%remote_master0_log%' AND command_type NOT LIKE 'Connect';
--echo --------------------
--connection remote_master1
SELECT argument AS remote_master1_log FROM mysql.general_log WHERE user_host LIKE '%172.18.0.9%' AND argument NOT LIKE '%remote_master1_log%' AND command_type NOT LIKE 'Connect';
--echo --------------------
