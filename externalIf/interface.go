package externalIf

import (
	"github.com/lf-edge/ekuiper/externalIf/rest"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/server"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/sirupsen/logrus"
)

type Store struct {
	Type     string
	Host     string
	Port     int
	Username string
	Password string
	Database string
	SslMode  string
}

type Config struct {
	Sources map[string]api.Source
	Sinks   map[string]api.Sink
	Store   *Store
	Log     *logrus.Logger
	Server  Server
	Version string
}

type Server struct {
	Port int
}

func initConf(config Config) {
	//
	conf.Config = &conf.KuiperConf{
		Rule: api.RuleOption{
			LateTol:            1000,
			Concurrency:        1,
			BufferLength:       1024,
			CheckpointInterval: 300000, //5 minutes
			SendError:          true,
		}, Basic: &conf.Basic{
			Debug:          false,
			ConsoleLog:     true,
			FileLog:        false,
			RotateTime:     24,
			MaxAge:         72,
			Ip:             "0.0.0.0",
			Port:           20498,
			RestIp:         "0.0.0.0",
			RestPort:       config.Server.Port,
			Prometheus:     false,
			PrometheusPort: 20499,
			PluginHosts:    "https://packages.emqx.net",
			Authentication: false,
			IgnoreCase:     true,
			RestTls:        nil,
		}, Sink: &conf.SinkConf{
			MemoryCacheThreshold: 1024,
			MaxDiskCache:         1024000,
			BufferPageSize:       256,
			EnableCache:          false,
			ResendInterval:       0,
			CleanCacheAtStop:     false,
		},
	}

	//setup store(db)
	conf.Config.Store.Type = config.Store.Type
	conf.Config.Store.Pg.Host = config.Store.Host
	conf.Config.Store.Pg.Port = config.Store.Port
	conf.Config.Store.Pg.Username = config.Store.Username
	conf.Config.Store.Pg.Password = config.Store.Password
	conf.Config.Store.Pg.Database = config.Store.Database
	conf.Config.Store.Pg.SslMode = config.Store.SslMode

	conf.Config.Portable.PythonBin = "python"
	*conf.Log = *config.Log
}

func ServerStartUp(config Config) {
	initConf(config)
	ruleProcessor, streamProcessor, registry := server.ExternalStartUp(config.Version, config.Sources, config.Sinks)
	rest.SetRuleProcessor(ruleProcessor)
	rest.SetStreamProcessor(streamProcessor)
	rest.SetRuleRegistry(registry)
}
