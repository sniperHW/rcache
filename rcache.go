package rcache

import (
	"context"
	dbsql "database/sql"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/jmoiron/sqlx"
)

type dataproxy struct {
	rediscli *redis.Client
	scancli  *redis.Client
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
			}
		}
	}
	return value, err
}

func (p *dataproxy) SyncDirtyToDB() error {
	cursor := uint64(0)
	var keys []string
	var err error

	for {
		keys, cursor, err = p.scancli.Scan(cursor, "", 10).Result()
		if err != nil {
			return err
		}

		for _, key := range keys {
			r, err := p.rediscli.HMGet(key, "version", "value").Result()
			if err != nil {
				return err
			}
			version, _ := strconv.Atoi(r[0].(string))
			value := r[1].(string)
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
			defer cancel()

			//fmt.Println("write to db", key, value, version)

			err = writebackPgsql(ctx, p.dbc, key, value, version)

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err != nil {
					return err
				}
			}
			//清除dirty标记
			//fmt.Println("clear dirty", key, version)
			err = RedisClearDirty(p.rediscli, key, version)
			if err != nil {
				return err
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil

}
