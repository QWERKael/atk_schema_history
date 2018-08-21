package config

import (
	"io/ioutil"
	"log"
	"gopkg.in/yaml.v2"
	"fmt"
	"github.com/siddontang/go-mysql/replication"
	"strings"
)

type YAMLConfig struct {
	CommonConfig CommonConfig `yaml:"commonConfig"`
	ManagerNode  NodeConfig   `yaml:"managerNode"`
	CliTemplate  NodeConfig   `yaml:"cliTemplate"`
	CliNodesTmp  []NodeConfig `yaml:"cliNodes"`
	CliNodes     []NodeConfig
}

type CommonConfig struct {
	Initdata bool `yaml:"initdata"`
}
type NodeConfig struct {
	Host       string `yaml:"host"`
	Hosts      string `yaml:"hosts"`
	Port       uint16 `yaml:"port"`
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	Database   string `yaml:"database"`
	Charset    string `yaml:"charset"`
	Binlogfile string `yaml:"binlogfile"`
	Binlogpos  uint32 `yaml:"binlogpos"`
	AutoPos    bool   `yaml:"autopos"`
}

func GetConfig(fileName string) *YAMLConfig {
	yamlFile, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	cfg := new(YAMLConfig)
	err = yaml.Unmarshal(yamlFile, cfg)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	for _, cliNode := range cfg.CliNodesTmp {
		if cliNode.Host == "" && cliNode.Hosts != "" {
			for _, host := range strings.Split(cliNode.Hosts, ",") {
				cliNode.Hosts = ""
				cliNode.Host = host
				cfg.CliNodes = append(cfg.CliNodes, cliNode)
			}
		} else if cliNode.Host != "" && cliNode.Hosts == "" {
			cfg.CliNodes = append(cfg.CliNodes, cliNode)
		} else if (cliNode.Host == "" && cliNode.Hosts == "") || (cliNode.Host != "" && cliNode.Hosts != "") {
			panic(fmt.Errorf("配置文件错误，host和hosts不能同时为空或者同时不为空"))
		}
	}
	//fmt.Printf("%#v", cfg.CliNodes)
	return cfg
}

func (yc *YAMLConfig) MakeBinlogSyncerConfigs() []replication.BinlogSyncerConfig {
	cliNodes := yc.CliNodes
	bscs := make([]replication.BinlogSyncerConfig, 0)
	for _, cliNode := range cliNodes {
		bsc := MakeBinlogSyncerConfig(cliNode)
		bscs = append(bscs, bsc)
	}
	return bscs
}

func (yc *YAMLConfig) MakeManagerDSN() string {
	return MakeDSN(yc.ManagerNode)
}

func (yc *YAMLConfig) MakeCliDSNs() []string {
	DSNs := make([]string, 0)
	for _, CliNode := range yc.CliNodes {
		DSN := MakeDSN(CliNode)
		DSNs = append(DSNs, DSN)
	}
	return DSNs
}

func MakeBinlogSyncerConfig(cliNode NodeConfig) replication.BinlogSyncerConfig {
	return replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     cliNode.Host,
		Port:     cliNode.Port,
		User:     cliNode.User,
		Password: cliNode.Password,
	}
}

func MakeDSN(nc NodeConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", nc.User, nc.Password, nc.Host, nc.Port, nc.Database, nc.Charset)
}
