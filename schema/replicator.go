package schema

import (
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/mysql"
	"context"
	"strings"
	"atk_schema_history/parse"
	"atk_schema_history/connect"
	"fmt"
	"time"
	"atk_schema_history/config"
)

func SyncSchema(nc config.NodeConfig,managerDSN string) {
	// Start sync with specified binlog file and position
	syncer := replication.NewBinlogSyncer(config.MakeBinlogSyncerConfig(nc))
	streamer, err := syncer.StartSync(mysql.Position{nc.Binlogfile, nc.Binlogpos})
	if err != nil {
		panic(err.Error())
	}
	managerConn := &ManagerConn{DSN: managerDSN}
	managerConn.Conn = connect.GetConn(managerConn.DSN)
	for {
		ev, _ := streamer.GetEvent(context.Background())
		if ev.Header.EventType == replication.QUERY_EVENT {
			queryEvent := ev.Event.(*replication.QueryEvent)
			query := string(queryEvent.Query)
			if strings.ToUpper(strings.Trim(query, " ")) == "BEGIN" {
				continue
			}
			schemaName, tableName, ok := parse.DDLParser(query)
			if schemaName == "" {
				schemaName = fmt.Sprintf("%s", queryEvent.Schema)
			}
			if ok {
				managerConn.SyncSchema(config.MakeDSN(nc), schemaName, tableName, query, time.Unix(int64(ev.Header.Timestamp), 0))
			}
		}
	}
}
