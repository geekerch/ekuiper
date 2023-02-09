package server

import (
	"sort"
	"time"

	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/binder/meta"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/topo/connection/factory"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/lf-edge/ekuiper/pkg/api"
)

// var (
// 	logger          = conf.Log
// 	startTimeStamp  int64
// 	version         = ""
// 	ruleProcessor   *processor.RuleProcessor
// 	streamProcessor *processor.StreamProcessor
// )

func ExternalStartUp(version string, sources map[string]api.Source, sinks map[string]api.Sink) (*processor.RuleProcessor, *processor.StreamProcessor, *RuleRegistry) {
	// version = conf.Config
	// conf.Log = config.Log
	conf.LoadFileType = "relative"
	startTimeStamp = time.Now().Unix()

	factory.InitClientsFactory()

	err := store.SetupWithKuiperConfig(conf.Config)
	if err != nil {
		panic(err)
	}

	ruleProcessor = processor.NewRuleProcessor()
	streamProcessor = processor.NewStreamProcessor()

	// register all extensions
	for k, v := range components {
		logger.Infof("register component %s", k)
		v.register()
	}

	// Bind the source, function, sink
	sort.Sort(entries)
	err = function.Initialize(entries)
	if err != nil {
		panic(err)
	}

	m := io.GetManager()

	for k, v := range sources {
		m.SetSource(k, v)
	}

	for k, v := range sinks {
		m.SetSink(k, v)
	}

	err = io.Initialize(entries)
	if err != nil {
		panic(err)
	}
	meta.Bind()

	registry = &RuleRegistry{internal: make(map[string]*rule.RuleState)}
	//Start lookup tables
	streamProcessor.RecoverLookupTable()
	//Start rules
	if rules, err := ruleProcessor.GetAllRules(); err != nil {
		logger.Infof("Start rules error: %s", err)
	} else {
		logger.Info("Starting rules")
		var reply string
		for _, rule := range rules {
			//err = server.StartRule(rule, &reply)
			if apiRule, err := ruleProcessor.GetRuleById(rule); err != nil {
				reply = recoverRule(apiRule)
				if len(reply) != 0 {
					logger.Info(reply)
				}
			}
		}
	}
	/*
			//Start rest service
			srvRest := createRestServer(conf.Config.Basic.RestIp, conf.Config.Basic.RestPort, conf.Config.Basic.Authentication)
			go func() {
				var err error
				if conf.Config.Basic.RestTls == nil {
					err = srvRest.ListenAndServe()
				} else {
					err = srvRest.ListenAndServeTLS(conf.Config.Basic.RestTls.Certfile, conf.Config.Basic.RestTls.Keyfile)
				}
				if err != nil && err != http.ErrServerClosed {
					logger.Errorf("Error serving rest service: %s", err)
				}
			}()

			// Start extend services
			for k, v := range servers {
				logger.Infof("start service %s", k)
				v.serve()
			}

			//Startup message
			restHttpType := "http"
			if conf.Config.Basic.RestTls != nil {
				restHttpType = "https"
			}
			msg := fmt.Sprintf("Serving kuiper (version - %s) on port %d, and restful api on %s://%s:%d. \n", version, conf.Config.Basic.Port, restHttpType, conf.Config.Basic.RestIp, conf.Config.Basic.RestPort)
			logger.Info(msg)
			fmt.Printf(msg)

			//Stop the services
			sigint := make(chan os.Signal, 1)
			signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
			<-sigint

			if err = srvRest.Shutdown(context.TODO()); err != nil {
				logger.Errorf("rest server shutdown error: %v", err)
			}
			logger.Info("rest server successfully shutdown.")

			// close extend services
			for k, v := range servers {
				logger.Infof("close service %s", k)
				v.close()
			}

		os.Exit(0)
	*/
	return ruleProcessor, streamProcessor, registry
}
