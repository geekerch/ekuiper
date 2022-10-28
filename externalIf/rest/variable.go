package rest

import (
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/server"
)

var (
	ruleProcessor   *processor.RuleProcessor
	streamProcessor *processor.StreamProcessor
	registry        *server.RuleRegistry
)

func SetRuleProcessor(in *processor.RuleProcessor) {
	ruleProcessor = in
}

func SetStreamProcessor(in *processor.StreamProcessor) {
	streamProcessor = in
}

func SetRuleRegistry(in *server.RuleRegistry) {
	registry = in
}
