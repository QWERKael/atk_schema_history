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
	"atk_schema_history/connect"
	"atk_schema_history/mylog"
)

func ListeningBinglog(nc config.NodeConfig, managerConn *ManagerConn) {
	dsn := config.MakeDSN(nc)
	if nc.AutoPos {
		_, _, rst, _ := connect.CommonQuery(connect.GetConn(dsn), "SHOW MASTER STATUS")
		nc.Binlogfile = rst[0][0].(string)
		nc.Binlogpos = uint32(rst[0][1].(uint64))
	}
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
				managerConn.SyncSchema(dsn, nc.Host, nc.Port, schemaName, tableName, query, time.Unix(int64(ev.Header.Timestamp), 0))
			}
		}
	}
	mylog.Log.Infof("%s:%d 监听已中断...", nc.Host, nc.Port)
}
