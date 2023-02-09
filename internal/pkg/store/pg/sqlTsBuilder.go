package pg

import (
	"github.com/lf-edge/ekuiper/pkg/kv"
)

type TsBuilder struct {
	database Database
}

func NewTsBuilder(d Database) TsBuilder {
	return TsBuilder{
		database: d,
	}
}

func (b TsBuilder) CreateTs(table string) (error, kv.Tskv) {
	return createSqlTs(b.database, table)
}
