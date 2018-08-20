package main

import (
	"github.com/kardianos/service"
	"log"
	"atk_schema_history/daemon"
)

func main() {
	//服务的配置信息
	cfg := &service.Config{
		Name:        "Schema History",
		DisplayName: "Schema History",
		Description: "record DDL operation.",
	}
	// Interface 接口
	prg := &daemon.Program{}
	// 构建服务对象
	s, err := service.New(prg, cfg)
	if err != nil {
		log.Fatal(err)
	}
	// logger 用于记录系统日志
	logger, err := s.Logger(nil)
	if err != nil {
		log.Fatal(err)
	}
	err = s.Run()
	if err != nil {
		logger.Error(err)
	}
	if err != nil {
		logger.Error(err)
	}
}
