package parse

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
	"atk_schema_history/mylog"
)

func DDLParser(query string) (string, string, bool) {
	sqlParser := parser.New()
	stmtNodes, err := sqlParser.Parse(query, "", "")
	if err != nil {
		mylog.Log.Infof("parse error:\n%v\n%s", err, query)
		return "", "", false
	}
	v := visitor{isDDL: false}
	for _, stmtNode := range stmtNodes {
		stmtNode.Accept(&v)
	}
	if v.isDDL {
		return v.schemaName, v.tableName, true
	}
	return "", "", false
}

type visitor struct {
	isDDL      bool
	schemaName string
	tableName  string
}

func (v *visitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch in.(type) {
	case *ast.AlterTableStmt,
	*ast.CreateDatabaseStmt,
	*ast.CreateIndexStmt,
	*ast.CreateTableStmt,
	*ast.CreateViewStmt,
	*ast.DropDatabaseStmt,
	*ast.DropIndexStmt,
	*ast.DropTableStmt,
	*ast.RenameTableStmt,
	*ast.TruncateTableStmt:
		return in, false
	case *ast.TableName:
		//fmt.Printf("%T\n", in)
		//fmt.Printf("%#v\n", in)
		//fmt.Printf("%#v\n", in.(*ast.TableName).Name.O)
		v.schemaName = in.(*ast.TableName).Schema.O
		v.tableName = in.(*ast.TableName).Name.O
		v.isDDL = true
		return in, true
	default:
		return in, true
	}
	return in, false
}

func (v *visitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
