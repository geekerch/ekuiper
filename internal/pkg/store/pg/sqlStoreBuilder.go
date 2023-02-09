package pg

import "github.com/lf-edge/ekuiper/pkg/kv"

type StoreBuilder struct {
	database Database
}

func NewStoreBuilder(d Database) StoreBuilder {
	return StoreBuilder{
		database: d,
	}
}

func (b StoreBuilder) CreateStore(table string) (kv.KeyValue, error) {
	return createSqlKvStore(b.database, table)
}
