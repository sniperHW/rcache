package rcache

import (
	"context"
	dbsql "database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func queryRow(ctx context.Context, dbc *sqlx.DB, key string) (version int, value string, err error) {
	err = dbc.QueryRowContext(ctx, fmt.Sprintf("select version,value from kv where key = '%s'", key)).Scan(&version, &value)
	return version, value, err
}

func updateRowPgsql(ctx context.Context, dbc *sqlx.DB, key string, value string) (version int, err error) {
	var tx *dbsql.Tx
	tx, err = dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if nil != err {
		return version, err
	}

	var str = `insert into kv("key","value","version") values($1,$2,$3) ON conflict(key) DO UPDATE SET 
		value = $2,version = kv.version+1 where kv.key = $1;`

	_, err = tx.ExecContext(ctx, str, key, value, 1)

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return version, err
		}
		return version, err
	}

	err = tx.QueryRowContext(ctx, fmt.Sprintf("select version from kv where key = '%s'", key)).Scan(&version)

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return version, err
		}
		return version, err
	}

	err = tx.Commit()

	return version, err
}

func writebackPgsql(ctx context.Context, dbc *sqlx.DB, key string, value string, version int) (err error) {
	var str = `insert into kv("key","value","version") values($1,$2,$3) ON conflict(key) DO UPDATE SET 
	value = $2,version = $3 where kv.key = $1 and version < $3;`
	_, err = dbc.ExecContext(ctx, str, key, value, version)
	return err
}
