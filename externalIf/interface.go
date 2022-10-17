package externalIf

import (
	"github.com/lf-edge/ekuiper/internal/server"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func ServerStartUp(version, loadFileType string, sources map[string]api.Source, sinks map[string]api.Sink) {
	server.StartUp(version, loadFileType, sources, sinks)
}

func GetServer() {

}
