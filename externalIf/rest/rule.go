package rest

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/server"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

func CreateRule(body string) error {

	r, err := ruleProcessor.ExecCreate("", body)

	if err != nil {
		fmt.Print("Create rule error")
		return err
	} else {
		fmt.Printf("Rule %s was created successfully.", r.Id)
	}

	panicOrError := infra.SafeRun(func() error {
		//Start the rule
		rs, err := createRuleState(r)
		if err != nil {
			return err
		} else {
			err = doStartRule(rs)
			return err
		}
	})

	if panicOrError != nil {
		fmt.Printf("Rule %s start failed: %s", r.Id, panicOrError)
	}

	return nil
}

func createRuleState(rule *api.Rule) (*server.RuleState, error) {
	rs := &server.RuleState{
		RuleId: rule.Id,
	}
	registry.Store(rule.Id, rs)
	if tp, err := planner.Plan(rule); err != nil {
		return rs, err
	} else {
		rs.Topology = tp
		rs.Triggered = true
		return rs, nil
	}
}

// Assume rs is started with topo instantiated
func doStartRule(rs *server.RuleState) error {
	err := ruleProcessor.ExecReplaceRuleState(rs.RuleId, true)
	if err != nil {
		return err
	}
	go func() {
		tp := rs.Topology
		err := infra.SafeRun(func() error {
			select {
			case err := <-tp.Open():
				return err
			}
		})
		if err != nil {
			tp.GetContext().SetError(err)
			fmt.Printf("closing rule %s for error: %v", rs.RuleId, err)
			tp.Cancel()
			rs.Triggered = false
		} else {
			rs.Triggered = false
			fmt.Printf("closing rule %s", rs.RuleId)
		}
	}()
	return nil
}

func restartRule(name string) error {
	stopRule(name)
	return startRule(name)
}

func startRule(name string) error {
	var rs *server.RuleState
	rs, ok := registry.Load(name)
	if !ok || (!rs.Triggered) {
		r, err := ruleProcessor.GetRuleById(name)
		if err != nil {
			return err
		}
		rs, err = createRuleState(r)
		if err != nil {
			return err
		}
		err = doStartRule(rs)
		if err != nil {
			return err
		}
	} else {
		conf.Log.Warnf("Rule %s is already started", name)
	}
	return nil
}

func stopRule(name string) (result string) {
	if rs, ok := registry.Load(name); ok && rs.Triggered {
		rs.Stop()
		err := ruleProcessor.ExecReplaceRuleState(name, false)
		if err != nil {
			conf.Log.Warnf("stop rule found error: %s", err.Error())
		}
		result = fmt.Sprintf("Rule %s was stopped.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func StartRule(name string) error {
	var rs *server.RuleState
	rs, ok := registry.Load(name)
	if !ok || (!rs.Triggered) {
		r, err := ruleProcessor.GetRuleById(name)
		if err != nil {
			return err
		}
		rs, err = createRuleState(r)
		if err != nil {
			return err
		}
		err = doStartRule(rs)
		if err != nil {
			return err
		}
	} else {
		conf.Log.Warnf("Rule %s is already started", name)
	}
	return nil
}

func StopRule(name string) (result string) {
	if rs, ok := registry.Load(name); ok && rs.Triggered {
		rs.Stop()
		err := ruleProcessor.ExecReplaceRuleState(name, false)
		if err != nil {
			conf.Log.Warnf("stop rule found error: %s", err.Error())
		}
		result = fmt.Sprintf("Rule %s was stopped.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func DeleteRule(name string) (result string) {
	if rs, ok := registry.Delete(name); ok {
		if rs.Triggered {
			(*rs.Topology).Cancel()
		}
		result = fmt.Sprintf("Rule %s was deleted.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func UpdateRule(name string, body string) error {
	_, err := ruleProcessor.GetRuleById(name)
	if err != nil {
		return err
	}

	r, err := ruleProcessor.ExecUpdate(name, string(body))

	if err != nil {
		return err
	} else {
		fmt.Printf("Rule %s was updated successfully.", r.Id)
	}

	err = restartRule(name)

	return err
}
