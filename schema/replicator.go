package schema

import (
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/mysql"
	"context"
	"strings"
	"atk_schema_history/parse"
	"fmt"
	"time"
	"atk_schema_history/config"
)

func SyncSchema(nc config.NodeConfig,managerConn *ManagerConn) {
	// Start sync with specified binlog file and position
	syncer := replication.NewBinlogSyncer(config.MakeBinlogSyncerConfig(nc))
	streamer, err := syncer.StartSync(mysql.Position{nc.Binlogfile, nc.Binlogpos})
	if err != nil {
		panic(err.Error())
	}
	for {
		ev, _ := streamer.GetEvent(context.Background())
		if ev.Header.EventType == replication.QUERY_EVENT {
			queryEvent := ev.Event.(*replication.QueryEvent)
			query := string(queryEvent.Query)
			if strings.ToUpper(strings.Trim(query, " ")) == "BEGIN" {
				continue
			}
			schemaName, tableName, ok := parse.DDLParser(query)
			if ok {
				if schemaName == "" {
					schemaName = fmt.Sprintf("%s", queryEvent.Schema)
				}
				managerConn.SyncSchema(config.MakeDSN(nc), schemaName, tableName, query, time.Unix(int64(ev.Header.Timestamp), 0))
			}
		}
	}
}
