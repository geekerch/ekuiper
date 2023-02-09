package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

func CreateRule(name string, ruleJson string) (string, error) {

	// Validate the rule json
	r, err := ruleProcessor.GetRuleByJson(name, ruleJson)
	if err != nil {
		return "", fmt.Errorf("Invalid rule json: %v", err)
	}
	// Validate the topo
	rs, err := createRuleState(r)
	if err != nil {
		return r.Id, fmt.Errorf("Create rule topo error: %v", err)
	}
	// Store to KV
	err = ruleProcessor.ExecCreate(r.Id, ruleJson)
	if err != nil {
		// Do not store to KV so also delete the in memory shadow
		DeleteRule(r.Id)
		return r.Id, fmt.Errorf("Store the rule error: %v", err)
	}
	// Start the rule asyncly
	if r.Triggered {
		go func() {
			panicOrError := infra.SafeRun(func() error {
				//Start the rule which runs async
				return rs.Start()
			})

			if panicOrError != nil {
				fmt.Errorf("Rule %s start failed: %s", r.Id, panicOrError)
			}
		}()
	}
	return r.Id, nil

}

// Create and initialize a rule state.
// Errors are possible during plan the topo.
// If error happens return immediately without add it to the registry
func createRuleState(r *api.Rule) (*rule.RuleState, error) {
	rs, err := rule.NewRuleState(r)
	if err != nil {
		return rs, err
	}
	registry.Store(r.Id, rs)
	return rs, nil
}

func recoverRule(r *api.Rule) string {
	// Validate the topo
	rs, err := createRuleState(r)
	if err != nil { // when recovering rules, assume the rules are valid, so always add it to the registry
		conf.Log.Errorf("Create rule topo error: %v", err)
		r.Triggered = false
		registry.Store(r.Id, rs)
	}
	if !r.Triggered {
		return fmt.Sprintf("Rule %s was stopped.", r.Id)
	} else {
		panicOrError := infra.SafeRun(func() error {
			//Start the rule which runs async
			return rs.Start()
		})
		if panicOrError != nil {
			return fmt.Sprintf("Rule %s start failed: %s", r.Id, panicOrError)
		}
	}
	return fmt.Sprintf("Rule %s was started.", r.Id)
}

func UpdateRule(ruleId, ruleJson string) error {
	// Validate the rule json
	r, err := ruleProcessor.GetRuleByJson(ruleId, ruleJson)
	if err != nil {
		return fmt.Errorf("Invalid rule json: %v", err)
	}
	if rs, ok := registry.Load(r.Id); ok {
		rs.UpdateTopo(r)
		return nil
	} else {
		return fmt.Errorf("Rule %s registry not found, try to delete it and recreate", r.Id)
	}
}

func DeleteRule(name string) (result string) {
	if rs, ok := registry.Delete(name); ok {
		rs.Close()
		result = fmt.Sprintf("Rule %s was deleted.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func StartRule(name string) error {
	rs, ok := registry.Load(name)
	if !ok {
		return fmt.Errorf("Rule %s is not found in registry, please check if it is created", name)
	} else {
		err := rs.Start()
		if err != nil {
			return err
		}
		err = ruleProcessor.ExecReplaceRuleState(rs.RuleId, true)
		return err
	}
}

func StopRule(name string) (result string) {
	if rs, ok := registry.Load(name); ok {
		err := rs.Stop()
		if err != nil {
			conf.Log.Warn(err)
		}
		err = ruleProcessor.ExecReplaceRuleState(name, false)
		if err != nil {
			conf.Log.Warnf("stop rule found error: %s", err.Error())
		}
		result = fmt.Sprintf("Rule %s was stopped.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func restartRule(name string) error {
	StopRule(name)
	time.Sleep(1 * time.Millisecond)
	return StartRule(name)
}

func getRuleStatus(name string) (string, error) {
	if rs, ok := registry.Load(name); ok {
		result, err := rs.GetState()
		if err != nil {
			return "", err
		}
		if result == "Running" {
			keys, values := (*rs.Topology).GetMetrics()
			metrics := "{"
			metrics += `"status": "running",`
			for i, key := range keys {
				value := values[i]
				switch value.(type) {
				case string:
					metrics += fmt.Sprintf("\"%s\":%q,", key, value)
				default:
					metrics += fmt.Sprintf("\"%s\":%v,", key, value)
				}
			}
			metrics = metrics[:len(metrics)-1] + "}"
			dst := &bytes.Buffer{}
			if err = json.Indent(dst, []byte(metrics), "", "  "); err != nil {
				result = metrics
			} else {
				result = dst.String()
			}
		} else {
			result = fmt.Sprintf(`{"status": "stopped", "message": "%s"}`, result)
		}
		return result, nil
	} else {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}

func getAllRulesWithStatus() ([]map[string]interface{}, error) {
	ruleIds, err := ruleProcessor.GetAllRules()
	if err != nil {
		return nil, err
	}
	sort.Strings(ruleIds)
	result := make([]map[string]interface{}, len(ruleIds))
	for i, id := range ruleIds {
		ruleName := id
		rule, _ := ruleProcessor.GetRuleById(id)
		if rule != nil && rule.Name != "" {
			ruleName = rule.Name
		}
		s, err := getRuleState(id)
		if err != nil {
			s = fmt.Sprintf("error: %s", err)
		}
		result[i] = map[string]interface{}{
			"id":     id,
			"name":   ruleName,
			"status": s,
		}
	}
	return result, nil
}

func getRuleState(name string) (string, error) {
	if rs, ok := registry.Load(name); ok {
		return rs.GetState()
	} else {
		return "", fmt.Errorf("Rule %s is not found in registry", name)
	}
}

func getRuleTopo(name string) (string, error) {
	if rs, ok := registry.Load(name); ok {
		graph := rs.GetTopoGraph()
		if graph == nil {
			return "", errorx.New(fmt.Sprintf("Fail to get rule %s's topo, make sure the rule has been started before", name))
		}
		bs, err := json.Marshal(graph)
		if err != nil {
			return "", errorx.New(fmt.Sprintf("Fail to encode rule %s's topo", name))
		} else {
			return string(bs), nil
		}
	} else {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}
