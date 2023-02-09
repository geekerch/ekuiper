package rest

import (
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func CreateStream(sql string, result *string) error {
	var err error
	*result, err = streamProcessor.ExecStreamSql(sql)

	return err
}

func GetStream(kind string, content *[]string) error {
	var (
		err error
	)

	st := ast.TypeStream

	if st == ast.TypeTable {
		if kind == "scan" {
			kind = ast.StreamKindScan
		} else if kind == "lookup" {
			kind = ast.StreamKindLookup
		} else {
			kind = ""
		}
	}

	if kind != "" {
		*content, err = streamProcessor.ShowTable(kind)
	} else {
		*content, err = streamProcessor.ShowStream(st)
	}

	if err != nil {
		return err
	}

	return nil
}
