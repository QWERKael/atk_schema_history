package connect

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"fmt"
	"strings"
	"reflect"
	"github.com/go-sql-driver/mysql"
)

func GetConn(DSN string) *sql.DB {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		panic(err.Error())
	}
	return db
}


func GetCreateTable(db *sql.DB, schemaName string, tableName string) string {
	_, _, rst, err := CommonQuery(db, fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schemaName, tableName))
	if err != nil {
		return ""
	}
	return rst[0][1].(string)
}

func CommonQuery(db *sql.DB, stmt string) ([]string, []*sql.ColumnType, [][]interface{}, error) {
	rows, err := db.Query(stmt)
	if err != nil {
		return nil, nil, nil, err
	}
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, nil, nil, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, nil, err
	}
	values := make([]interface{}, len(columnNames))
	for i := range values {
		values[i] = reflect.New(columnTypes[i].ScanType()).Interface()
	}
	rst := make([][]interface{}, 0)
	for rows.Next() {
		row := make([]interface{}, 0)
		err = rows.Scan(values...)
		if err != nil {
			return nil, nil, nil, err
		}
		for i, col := range values {
			switch t := col.(type) {
			case *int8:
				v := *(*int8)(t)
				//fmt.Printf("\ntype is %s, value is %d\n", reflect.TypeOf(v), v)
				row = append(row, v)
			case *int16:
				v := *(*int16)(t)
				//fmt.Printf("\ntype is %s, value is %d\n", reflect.TypeOf(v), v)
				row = append(row, v)
			case *int32:
				v := *(*int32)(t)
				//fmt.Printf("\ntype is %s, value is %d\n", reflect.TypeOf(v), v)
				row = append(row, v)
			case *int64:
				v := *(*int64)(t)
				//fmt.Printf("\ntype is %s, value is %d\n", reflect.TypeOf(v), v)
				row = append(row, v)
			case *uint8:
				v := *(*uint8)(t)
				//fmt.Printf("\ntype is %s, value is %d\n", reflect.TypeOf(v), v)
				row = append(row, v)
			case *uint16:
				v := *(*uint16)(t)
				//fmt.Printf("\ntype is %s, value is %d\n", reflect.TypeOf(v), v)
				row = append(row, v)
			case *uint32:
				v := *(*uint32)(t)
				//fmt.Printf("\ntype is %s, value is %d\n", reflect.TypeOf(v), v)
				row = append(row, v)
			case *uint64:
				v := *(*uint64)(t)
				//fmt.Printf("\ntype is %s, value is %d\n", reflect.TypeOf(v), v)
				row = append(row, v)
			case *sql.RawBytes:
				v := *(*sql.RawBytes)(t)
				//fmt.Printf("\ntype is %s, value is %#v\n", reflect.TypeOf(v), v)
				if columnTypes[i].DatabaseTypeName() == "BLOB" {
					row = append(row, v)
				} else {
					row = append(row, string(v))
				}
			case *sql.NullString:
				v := *(*sql.NullString)(t)
				//fmt.Printf("\ntype is %s, value is %#v\n", reflect.TypeOf(v), v)
				if v.Valid {
					row = append(row, v.String)
				} else {
					row = append(row, nil)
				}
			case *sql.NullInt64:
				v := *(*sql.NullInt64)(t)
				//fmt.Printf("\ntype is %s, value is %#v\n", reflect.TypeOf(v), v)
				if v.Valid {
					row = append(row, v.Int64)
				} else {
					row = append(row, nil)
				}
			case *sql.NullFloat64:
				v := *(*sql.NullFloat64)(t)
				//fmt.Printf("\ntype is %s, value is %#v\n", reflect.TypeOf(v), v)
				if v.Valid {
					row = append(row, v.Float64)
				} else {
					row = append(row, nil)
				}
			case *sql.NullBool:
				v := *(*sql.NullBool)(t)
				fmt.Printf("\ntype is %s, value is %#v\n", reflect.TypeOf(v), v)
				if v.Valid {
					row = append(row, v.Bool)
				} else {
					row = append(row, nil)
				}
			case *mysql.NullTime:
				v := *(*mysql.NullTime)(t)
				//fmt.Printf("\ntype is %s, value is %#v\n, DB Type: %s\n", reflect.TypeOf(v), v, columnTypes[i].DatabaseTypeName())
				if v.Valid {
					row = append(row, v.Time)
				} else {
					row = append(row, nil)
				}
			default:
				return nil, nil, nil, fmt.Errorf("\ntype is undefined, value is %#v\n", t)
			}
			//fmt.Printf("\n%s: %#v, Value Type: %s, Scan Type: %s, DB Type: %s\n", columnNames[i], col, reflect.TypeOf(col), columnTypes[i].ScanType(), columnTypes[i].DatabaseTypeName())
		}
		rst = append(rst, row)
		//fmt.Println("-----------------------------------")
	}
	if err = rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	return columnNames, columnTypes, rst, nil
}

func CommonInsert(db *sql.DB, tableName string, columnNames []string, values [][]interface{}) []sql.Result {
	insertStmt := fmt.Sprintf("INSERT INTO %s(`%s`) VALUES(%s)", tableName, strings.Join(columnNames, "`, `"), "?"+strings.Repeat(", ?", len(columnNames)-1))
	//fmt.Println(insertStmt)
	ip, _ := db.Prepare(insertStmt)
	var rsts = make([]sql.Result, len(values))
	for i, val := range values {
		rst, err := ip.Exec(val...)
		if err != nil {
			panic(err.Error())
		}
		rsts[i] = rst
	}
	return rsts
}

//func (mc ManagerConn) GetAllTableSchema() map[string][]string {
//	//defer mc.Conn.Close()
//	rows, err := mc.Conn.Query("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA NOT IN ('sys','performance_schema','mysql','information_schema')")
//	if err != nil {
//		panic(err.Error())
//	}
//	var schemaName string
//	var tableName string
//	var columnName string
//	for rows.Next() {
//		err = rows.Scan(&schemaName, &tableName, &columnName)
//		if err != nil {
//			panic(err.Error())
//		}
//		fullName := schemaName + "." + tableName
//		TableSchemaCache[fullName] = append(TableSchemaCache[fullName], columnName)
//	}
//	return TableSchemaCache
//}
