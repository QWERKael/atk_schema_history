package schema

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
	"atk_schema_history/connect"
	"atk_schema_history/config"
)

type ManagerConn struct {
	DSN  string
	Conn *sql.DB
}

func (mc *ManagerConn) InitSchema(schemaName string, initTables map[string]string) {
	var tableNames = make([]string, 0)
	for tableName := range initTables {
		tableNames = append(tableNames, tableName)
	}
	sqlStmt := fmt.Sprintf("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME IN (%s)", schemaName, "'"+strings.Join(tableNames, "', '")+"'")
	//fmt.Print(sqlStmt)
	rows, err := mc.Conn.Query(sqlStmt)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var existentTable string
	for rows.Next() {
		err = rows.Scan(&existentTable)
		if err != nil {
			panic(err.Error())
		}
		delete(initTables, existentTable)
	}
	//初始化不存在的表
	for tableName, tableStmt := range initTables {
		fmt.Printf("\ntableName: %s\n", tableName)
		_, err := mc.Conn.Exec(tableStmt)
		if err != nil {
			panic(err.Error())
		}
	}
}

func (mc *ManagerConn) InitData(cliNodes []config.NodeConfig) {
	for _, cliNode := range cliNodes {
		now := time.Now()
		tablesStmt := fmt.Sprintf("SELECT *, '%s' AS host, '%d' AS port, '%s' AS insert_time FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'MYSQL', 'PERFORMANCE_SCHEMA', 'SYS')", cliNode.Host, cliNode.Port, now.Format("2006-01-02 15:04:05"))
		columnsStmt := fmt.Sprintf("SELECT *, '%s' AS host, '%d' AS port, '%s' AS insert_time FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'MYSQL', 'PERFORMANCE_SCHEMA', 'SYS')", cliNode.Host, cliNode.Port, now.Format("2006-01-02 15:04:05"))
		ds := config.MakeDSN(cliNode)
		db := connect.GetConn(ds)
		tableCN, _, tablesRst, _ := connect.CommonQuery(db, tablesStmt)
		columnCN, _, columnsRst, _ := connect.CommonQuery(db, columnsStmt)
		connect.CommonInsert(mc.Conn, "TABLES_LOG", tableCN, tablesRst)
		connect.CommonInsert(mc.Conn, "COLUMNS_LOG", columnCN, columnsRst)
	}
}

func (mc *ManagerConn) SyncSchema(DSN string, host string, port uint16, schemaName string, tableName string, ddlStmt string, binlogEventTime time.Time) {
	db := connect.GetConn(DSN)
	defer db.Close()
	tablesStmt := fmt.Sprintf("SELECT *, '%s' AS host, '%d' AS port, '%s' AS insert_time FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", host, port, binlogEventTime.Format("2006-01-02 15:04:05"), schemaName, tableName)
	columnsStmt := fmt.Sprintf("SELECT *, '%s' AS host, '%d' AS port, '%s' AS insert_time FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", host, port, binlogEventTime.Format("2006-01-02 15:04:05"), schemaName, tableName)
	tableCN, _, tablesRst, _ := connect.CommonQuery(db, tablesStmt)
	columnCN, _, columnsRst, _ := connect.CommonQuery(db, columnsStmt)
	connect.CommonInsert(mc.Conn, "TABLES_LOG", tableCN, tablesRst)
	connect.CommonInsert(mc.Conn, "COLUMNS_LOG", columnCN, columnsRst)
	sclCN := []string{"host", "port", "ddl_stmt", "create_stmt", "insert_time"}
	sclRst := [][]interface{}{{host, port, ddlStmt, connect.GetCreateTable(db, schemaName, tableName), binlogEventTime.Format("2006-01-02 15:04:05")}}
	connect.CommonInsert(mc.Conn, "SCHEMA_CHANGE_LOG", sclCN, sclRst)
}
