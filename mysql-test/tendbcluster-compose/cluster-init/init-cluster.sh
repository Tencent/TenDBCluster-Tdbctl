SPIDER0=$(getent hosts spider0 | awk '{print $1}')
SPIDER1=$(getent hosts spider1 | awk '{print $1}')
SPIDER2=$(getent hosts spider2 | awk '{print $1}')
MASTER0=$(getent hosts master0 | awk '{print $1}')
MASTER1=$(getent hosts master1 | awk '{print $1}')
SLAVE0=$(getent hosts slave0 | awk '{print $1}')
SLAVE1=$(getent hosts slave1 | awk '{print $1}')
CTL0=$(getent hosts ctl0 | awk '{print $1}')
CTL1=$(getent hosts ctl1 | awk '{print $1}')
CTL2=$(getent hosts ctl2 | awk '{print $1}')


MASTER_POS=$(mysql -uroot -p123 -hmaster0 -P3306 -se "show master status\G" | grep Position | awk -F":" '{print $2}' | sed 's/\s*//g')
MASTER_FILE=$(mysql -uroot -p123 -hmaster0 -P3306 -se "show master status\G" | grep File | awk -F":" '{print $2}' | sed 's/\s*//g')
mysql -uroot -p123 -hslave0 -P3306 -se "change master to master_host='${MASTER0}', master_port=3306, master_user='mysql', master_password='mysql', master_log_file='${MASTER_FILE}', master_log_pos=${MASTER_POS}; start slave;"


MASTER_POS=$(mysql -uroot -p123 -hmaster1 -P3306 -se "show master status\G" | grep Position | awk -F":" '{print $2}' | sed 's/\s*//g')
MASTER_FILE=$(mysql -uroot -p123 -hmaster1 -P3306 -se "show master status\G" | grep File | awk -F":" '{print $2}' | sed 's/\s*//g')
mysql -uroot -p123 -hslave1 -P3306 -se "change master to master_host='${MASTER1}', master_port=3306, master_user='mysql', master_password='mysql', master_log_file='${MASTER_FILE}', master_log_pos=${MASTER_POS}; start slave;"


for NODE in master0 master1 slave0 slave1
do
	mysql -uroot -h${NODE} -p123 -P3306 -se "create user mysql@'%' identified by 'mysql';grant all privileges on *.* to mysql@'%' with grant option;"
done

for NODE in ctl0 ctl1 ctl2
do
	mysql -uroot -h${NODE} -p123 -P3306 -se "set tc_admin=0;create user mysql@'%' identified by 'mysql';grant all privileges on *.* to mysql@'%' with grant option;"
done

for SPIDER in spider0 spider1 spider2
do
	mysql -uroot -h${SPIDER} -p123 -P3306 -se "set ddl_execute_by_ctl = off;create user mysql@'%' identified by 'mysql';grant all privileges on *.* to mysql@'%' with grant option;"
done

for CTL in ctl0 ctl1 ctl2
do
	echo "install ${CTL}"
	CHECK=$(mysql -uroot -h${CTL} -p123 -P3306 -se "select count(*) from information_schema.plugins where plugin_name='rpl_semi_sync_master'")
	echo $CHECK
	if [ "$CHECK" -le 0 ]
	then
		mysql -uroot -h${CTL} -p123 -P3306 -se "set tc_admin=0;INSTALL PLUGIN rpl_semi_sync_master SONAME 'semisync_master.so';"
	fi

	CHECK=$(mysql -uroot -h${CTL} -p123 -P3306 -se "select count(*) from information_schema.plugins where plugin_name='rpl_semi_sync_slave'")
	if [ "$CHECK" -le 0 ]
	then
		mysql -uroot -h${CTL} -p123 -P3306 -se "set tc_admin=0;INSTALL PLUGIN rpl_semi_sync_slave SONAME 'semisync_slave.so';"
	fi
done

for CTL in ctl1 ctl2
do
	echo "change ${CTL}"
	mysql -uroot -h${CTL} -p123 -P3306 -se "change master to master_host='${CTL0}', master_port=3306, master_user='mysql', master_password='mysql', master_auto_position=1; start slave;"
done


mysql -uroot -hctl0 -p123 -P3306<<EOF
tdbctl create node wrapper 'SPIDER' options(user 'mysql', password 'mysql', host "${SPIDER0}", port 3306);
tdbctl create node wrapper 'SPIDER' options(user 'mysql', password 'mysql', host "${SPIDER1}", port 3306);
tdbctl create node wrapper 'SPIDER' options(user 'mysql', password 'mysql', host "${SPIDER2}", port 3306);
tdbctl create node wrapper 'mysql' options(user 'mysql', password 'mysql', host "${MASTER0}", port 3306);
tdbctl create node wrapper 'mysql' options(user 'mysql', password 'mysql', host "${MASTER1}", port 3306);
tdbctl create node wrapper 'mysql_slave' options(user 'mysql', password 'mysql', host '${SLAVE0}', port 3306);
tdbctl create node wrapper 'mysql_slave' options(user 'mysql', password 'mysql', host '${SLAVE1}', port 3306);
tdbctl create node wrapper 'TDBCTL' options(user 'mysql', password 'mysql', host "${CTL0}", port 3306);
tdbctl create node wrapper 'TDBCTL' options(user 'mysql', password 'mysql', host "${CTL1}", port 3306);
tdbctl create node wrapper 'TDBCTL' options(user 'mysql', password 'mysql', host "${CTL2}", port 3306);
TDBCTL ENABLE PRIMARY;
tdbctl flush routing;
EOF
