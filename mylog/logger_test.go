package mylog

import "testing"

func TestGetConfig(t *testing.T) {
	file := InitLogger("../schema_history.Log")
	Log.Infoln("记录日志...")
	CloseLogger(file)
}
