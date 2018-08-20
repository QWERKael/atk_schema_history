package main

import (
	"atk_schema_history/config"
	"atk_schema_history/schema"
	"fmt"
	"atk_schema_history/connect"
)

func main() {
	cfg := config.GetConfig("config.yml")
	managerDSN := cfg.MakeManagerDSN()
	fmt.Printf("%#v", cfg.CliNodes)
	managerConn := &schema.ManagerConn{DSN: managerDSN}
	managerConn.Conn = connect.GetConn(managerConn.DSN)
	initTables := make(map[string]string, 3)
	initTables["TABLES_LOG"] = "CREATE TABLE `TABLES_LOG` (\n  `ID` bigint(20) NOT NULL AUTO_INCREMENT,\n  `TABLE_CATALOG` varchar(512) NOT NULL DEFAULT '',\n  `TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',\n  `TABLE_NAME` varchar(64) NOT NULL DEFAULT '',\n  `TABLE_TYPE` varchar(64) NOT NULL DEFAULT '',\n  `ENGINE` varchar(64) DEFAULT NULL,\n  `VERSION` bigint(21) unsigned DEFAULT NULL,\n  `ROW_FORMAT` varchar(20) DEFAULT NULL,\n  `TABLE_ROWS` bigint(21) unsigned DEFAULT NULL,\n  `AVG_ROW_LENGTH` bigint(21) unsigned DEFAULT NULL,\n  `DATA_LENGTH` bigint(21) unsigned DEFAULT NULL,\n  `MAX_DATA_LENGTH` bigint(21) unsigned DEFAULT NULL,\n  `INDEX_LENGTH` bigint(21) unsigned DEFAULT NULL,\n  `DATA_FREE` bigint(21) unsigned DEFAULT NULL,\n  `AUTO_INCREMENT` bigint(21) unsigned DEFAULT NULL,\n  `CREATE_TIME` datetime DEFAULT NULL,\n  `UPDATE_TIME` datetime DEFAULT NULL,\n  `CHECK_TIME` datetime DEFAULT NULL,\n  `TABLE_COLLATION` varchar(32) DEFAULT NULL,\n  `CHECKSUM` bigint(21) unsigned DEFAULT NULL,\n  `CREATE_OPTIONS` varchar(255) DEFAULT NULL,\n  `TABLE_COMMENT` varchar(2048) NOT NULL DEFAULT '',\n  `insert_time` datetime DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	initTables["COLUMNS_LOG"] = "CREATE TABLE `COLUMNS_LOG` (\n  `ID` bigint(20) NOT NULL AUTO_INCREMENT,\n  `TABLE_CATALOG` varchar(512) NOT NULL DEFAULT '',\n  `TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',\n  `TABLE_NAME` varchar(64) NOT NULL DEFAULT '',\n  `COLUMN_NAME` varchar(64) NOT NULL DEFAULT '',\n  `ORDINAL_POSITION` bigint(21) unsigned NOT NULL DEFAULT '0',\n  `COLUMN_DEFAULT` longtext,\n  `IS_NULLABLE` varchar(3) NOT NULL DEFAULT '',\n  `DATA_TYPE` varchar(64) NOT NULL DEFAULT '',\n  `CHARACTER_MAXIMUM_LENGTH` bigint(21) unsigned DEFAULT NULL,\n  `CHARACTER_OCTET_LENGTH` bigint(21) unsigned DEFAULT NULL,\n  `NUMERIC_PRECISION` bigint(21) unsigned DEFAULT NULL,\n  `NUMERIC_SCALE` bigint(21) unsigned DEFAULT NULL,\n  `DATETIME_PRECISION` bigint(21) unsigned DEFAULT NULL,\n  `CHARACTER_SET_NAME` varchar(32) DEFAULT NULL,\n  `COLLATION_NAME` varchar(32) DEFAULT NULL,\n  `COLUMN_TYPE` longtext NOT NULL,\n  `COLUMN_KEY` varchar(3) NOT NULL DEFAULT '',\n  `EXTRA` varchar(30) NOT NULL DEFAULT '',\n  `PRIVILEGES` varchar(80) NOT NULL DEFAULT '',\n  `COLUMN_COMMENT` varchar(1024) NOT NULL DEFAULT '',\n  `GENERATION_EXPRESSION` longtext NOT NULL,\n  `insert_time` datetime DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	initTables["SCHEMA_CHANGE_LOG"] = "CREATE TABLE `SCHEMA_CHANGE_LOG` (\n  `ID` bigint(20) NOT NULL AUTO_INCREMENT,\n  `ddl_stmt` varchar(5000) NOT NULL DEFAULT '',\n  `create_stmt` varchar(5000) NOT NULL DEFAULT '',\n  `insert_time` datetime DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	managerConn.InitSchema(cfg.ManagerNode.Database, initTables)
	managerConn.InitData(cfg.MakeCliDSNs())
	for _, cliNode := range cfg.CliNodes {
		schema.ListeningBinglog(cliNode, managerConn)
	}
}
