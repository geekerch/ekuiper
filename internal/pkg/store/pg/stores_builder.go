package pg

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
)

func BuildStores(c definition.Config, _ string) (definition.StoreBuilder, definition.TsBuilder, error) {
	db, err := NewPgDatabase(c)

	if err != nil {
		return nil, nil, err
	}

	err = db.Connect()

	if err != nil {
		return nil, nil, err
	}

	d, ok := db.(Database)

	if !ok {
		return nil, nil, fmt.Errorf("unrecognized database type")
	}

	kvBuilder := NewStoreBuilder(d)
	tsBuilder := NewTsBuilder(d)
	return kvBuilder, tsBuilder, nil
}
