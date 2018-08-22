package config

import (
	"testing"
	"atk_schema_history/mylog"
)

func TestGetConfig(t *testing.T) {
	logFile := mylog.InitLogger("../schema_history.log")
	GetConfig("../config.yml")
	mylog.CloseLogger(logFile)
}
