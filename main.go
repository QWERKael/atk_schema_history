package main

import (
	"atk_schema_history/config"
	"atk_schema_history/schema"
	"fmt"
)

func main() {
	cfg := config.GetConfig("config.yml")
	managerDSN := cfg.MakeManagerDSN()
	fmt.Printf("%#v", cfg.CliNodes)
	for _, cliNode := range cfg.CliNodes {
		schema.SyncSchema(cliNode, managerDSN)
	}
}
