package parse

import (
	"github.com/xwb1989/sqlparser"
	"log"
)

func DDLParser(query string) (string, string, bool) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		//panic(err.Error())
		log.Println(query)
		log.Println(err.Error())
		return "", "", false

	}
	ddlStmt, ok := stmt.(*sqlparser.DDL)
	if ok == false {
		return "", "", false
	}
	switch ddlStmt.Action {
	case "create",
		"rename",
		"alter":
		schemaName := ddlStmt.NewName.Qualifier.String()
		tableName := ddlStmt.NewName.Name.String()
		return schemaName, tableName, true
	case "drop":
		schemaName := ddlStmt.Table.Qualifier.String()
		tableName := ddlStmt.Table.Name.String()
		return schemaName, tableName, false
	case "truncate",
		"create vindex",
		"add vindex",
		"drop vindex":
		return "", "", false
	default:
		log.Printf("\n无法解析该DDL语句：%#v\n", ddlStmt)
		return "", "", false
	}
}
