SET SQL_LOG_BIN=0;
CREATE DATABASE IF NOT EXISTS `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;
update mysql.db set Insert_priv = 'Y' where db = 'test';
flush privileges;
USE test;
CREATE TABLE IF NOT EXISTS `test`.`free_space` (
  `a` int(11) DEFAULT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `test`.`conn_log` (
  `conn_id` bigint(20) DEFAULT NULL,
  `conn_time` datetime DEFAULT NULL,
  `user_name` varchar(128) DEFAULT NULL,
  `cur_user_name` varchar(128) DEFAULT NULL,
  `ip` varchar(15) DEFAULT NULL,
  KEY `conn_time` (`conn_time`)
) ENGINE=InnoDB;
CREATE DATABASE IF NOT EXISTS `db_infobase` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;
USE db_infobase;
CREATE TABLE IF NOT EXISTS `db_infobase`.`QUERY_RESPONSE_TIME` (
  `time_min` int(11) NOT NULL DEFAULT '0',
  `time` varchar(14) NOT NULL DEFAULT '',
  `count` int(11) unsigned NOT NULL DEFAULT '0',
  `total` varchar(100) NOT NULL DEFAULT '',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`time_min`,`time`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `db_infobase`.`check_heartbeat` (
  `uid` int(11) NOT NULL,
  `ck_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `db_infobase`.`master_slave_check` (
  `id` int(11) NOT NULL,
  `check_item` varchar(64) NOT NULL COMMENT 'check_item to check',
  `master` varchar(64) DEFAULT NULL COMMENT 'the check_item status on master',
  `slave` varchar(64) DEFAULT NULL COMMENT 'the check_item status on slave',
  `check_result` varchar(64) DEFAULT NULL COMMENT 'the different value of master and slave',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `db_infobase`.`checksum` (
  `db` char(64) NOT NULL,
  `tbl` char(64) NOT NULL,
  `chunk` int(11) NOT NULL,
  `boundaries` text,
  `this_crc` char(40) NOT NULL,
  `this_cnt` int(11) NOT NULL,
  `master_crc` char(40) DEFAULT NULL,
  `master_cnt` int(11) DEFAULT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `chunk_time` float DEFAULT NULL,
  `chunk_index` varchar(200) DEFAULT NULL,
  `lower_boundary` blob,
  `upper_boundary` blob,
  PRIMARY KEY (`db`,`tbl`,`chunk`),
  KEY `ts_db_tbl` (`ts`,`db`,`tbl`)
) ENGINE=InnoDB;
REPLACE INTO `db_infobase`.`check_heartbeat`(uid) VALUES(1);
