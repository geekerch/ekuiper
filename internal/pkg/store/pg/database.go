// Copyright 2022-2022 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pg

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
	_ "github.com/lib/pq"
)

type Database interface {
	Apply(f func(db *sql.DB) error) error
}

type Pg struct {
	db               *sql.DB
	connectionString string
	mu               sync.Mutex
}

func NewPgDatabase(c definition.Config) (definition.Database, error) {
	return &Pg{
		db:               nil,
		connectionString: connectionString(c.Pg),
		mu:               sync.Mutex{},
	}, nil
}

func (p *Pg) Connect() error {
	db, err := sql.Open("postgres", p.connectionString)

	if err != nil {
		return err
	}

	p.db = db
	return nil
}

func (p *Pg) Disconnect() error {
	err := p.db.Close()
	return err
}

func (p *Pg) Apply(f func(db *sql.DB) error) error {
	p.mu.Lock()
	err := f(p.db)
	p.mu.Unlock()
	return err
}

func connectionString(config definition.PgConfig) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", config.Host, config.Port, config.Username, config.Password, config.Database, config.SslMode)
}
