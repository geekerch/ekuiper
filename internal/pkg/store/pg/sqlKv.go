package pg

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"strings"

	kvEncoding "github.com/lf-edge/ekuiper/internal/pkg/store/encoding"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type sqlKvStore struct {
	database Database
	table    string
}

func createSqlKvStore(database Database, table string) (*sqlKvStore, error) {
	store := &sqlKvStore{
		database: database,
		table:    table,
	}
	err := store.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" ("key" text PRIMARY KEY, "val" bytea);`, table)
		_, err := db.Exec(query)
		return err
	})
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (kv *sqlKvStore) Setnx(key string, value interface{}) error {
	return kv.database.Apply(func(db *sql.DB) error {
		err, b := kvEncoding.Encode(value)

		if nil != err {
			return err
		}

		query := fmt.Sprintf(`INSERT INTO %s (key, val) values($1, $2);`, kv.table)
		_, err = db.Exec(query, key, b)

		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				return fmt.Errorf(`item %s already exists`, key)
			}
		}
		return err
	})
}

func (kv *sqlKvStore) Set(key string, value interface{}) error {
	err, b := kvEncoding.Encode(value)

	if nil != err {
		return err
	}
	err = kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf(`update %s set val = $1 where key = $2;`, kv.table)

		_, err = db.Exec(query, b, key)

		return err
	})
	return err
}

func (kv *sqlKvStore) Get(key string, value interface{}) (bool, error) {
	result := false
	err := kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf(`SELECT val FROM %s WHERE key=$1;`, kv.table)
		row := db.QueryRow(query, key)
		var tmp []byte
		err := row.Scan(&tmp)
		if err != nil {
			result = false
			return nil
		}

		dec := gob.NewDecoder(bytes.NewBuffer(tmp))
		if err := dec.Decode(value); err != nil {
			return err
		}
		result = true
		return nil
	})
	return result, err
}

func (kv *sqlKvStore) Delete(key string) error {
	return kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf(`SELECT key FROM %s WHERE key=$1;`, kv.table)
		row := db.QueryRow(query, key)
		var tmp []byte
		err := row.Scan(&tmp)
		if nil != err || len(tmp) == 0 {
			return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("%s is not found", key))
		}
		query = fmt.Sprintf(`DELETE FROM %s WHERE key=$1;`, kv.table)
		_, err = db.Exec(query, key)
		return err
	})
}

func (kv *sqlKvStore) Keys() ([]string, error) {
	keys := make([]string, 0)
	err := kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf(`SELECT key FROM "%s"`, kv.table)
		row, err := db.Query(query)
		if nil != err {
			return err
		}
		defer row.Close()
		for row.Next() {
			var val string
			err = row.Scan(&val)
			if nil != err {
				return err
			} else {
				keys = append(keys, val)
			}
		}
		return nil
	})
	return keys, err
}

func (kv *sqlKvStore) Clean() error {
	return kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf(`DELETE FROM "%s"`, kv.table)
		_, err := db.Exec(query)
		return err
	})
}

func (kv *sqlKvStore) Drop() error {
	return kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf(`Drop table "%s";`, kv.table)
		_, err := db.Exec(query)
		return err
	})
}
