package rcache

import (
	"context"
	dbsql "database/sql"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/jmoiron/sqlx"
)

type dataproxy struct {
	rediscli *redis.Client
	dbc      *sqlx.DB
}

func (p *dataproxy) Set(ctx context.Context, key string, value string) (err error) {
	//尝试直接更新redis
	if err = RedisSet(p.rediscli, key, value); err != nil {
		if err.Error() == "err_not_in_redis" {
			//写入数据库
			var version int
			if version, err = updateRowPgsql(ctx, p.dbc, key, value); err == nil {
				err = RedisLoadSet(p.rediscli, key, version, value)
			}
		}
	}
	return err
}

func (p *dataproxy) Get(ctx context.Context, key string) (value string, err error) {
	if value, err = RedisGet(p.rediscli, key); err != nil {
		if err.Error() == "err_not_in_redis" {
			//从数据库加载
			var version int
			version, value, err = queryRow(ctx, p.dbc, key)
			if err == nil || err == dbsql.ErrNoRows {
				value, err = RedisLoadGet(p.rediscli, key, version, value)
				if err != nil {
					err = fmt.Errorf("error on scriptLoadGet:%s", err.Error())
				}
			}
		} else {
			err = fmt.Errorf("error on scriptGet:%s", err.Error())
		}
	}
	return value, err
}

/*func (p *dataproxy) SyncDirtyToDB() {
}*/
