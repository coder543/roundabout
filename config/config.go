package config

import (
	"io/ioutil"
	"log"
	"time"

	"gopkg.in/yaml.v2"
)

var Virtual VirtualPg
var Database DbConfig
var PprofAddr string

type config struct {
	Virtual   VirtualPg `yaml:"virtual"`
	Database  DbConfig  `yaml:"database"`
	PprofAddr string    `yaml:"pprof_addr"`
}

type VirtualPg struct {
	Listen                string `yaml:"listen"`
	Username              string `yaml:"username"`
	Password              string `yaml:"password"`
	Database              string `yaml:"database"`
	ProxyApplicationNames bool   `yaml:"proxy_application_names"`
	ClearApplicationNames bool   `yaml:"clear_application_names"`
}

type DbConfig struct {
	DatabaseName string         `yaml:"database_name"`
	Address      string         `yaml:"address"`
	Username     string         `yaml:"username"`
	Password     string         `yaml:"password"`
	Database     string         `yaml:"database"`
	NumConns     uint16         `yaml:"num_conns"`
	MaxLifetime  *time.Duration `yaml:"conn_max_lifetime"`
	ReadReplicas []string       `yaml:"read_replicas"`
}

func (c *config) virtualValidate() {
	if c.Virtual.Listen == "" {
		log.Panicln("No address given for roundabout to listen on!")
	}
	if c.Virtual.Username == "" {
		log.Panicln("No username specified for access to roundabout!")
	}
}

func (c *config) dbValidate() {
	if c.Database.NumConns == 0 {
		c.Database.NumConns = 20
	}

	if c.Database.Address == "" {
		log.Panicf("Database %q does not specify an address!\n", c.Database.DatabaseName)
	}
}

func init() {
	cfgBytes, err := ioutil.ReadFile("./roundabout.yml")
	if err != nil {
		log.Panicln(err)
	}

	var cfg config
	err = yaml.UnmarshalStrict(cfgBytes, &cfg)
	if err != nil {
		log.Panicln(err)
	}

	cfg.virtualValidate()
	cfg.dbValidate()

	Virtual = cfg.Virtual
	Database = cfg.Database
	PprofAddr = cfg.PprofAddr
}
