package rcache

import (
	"context"
	dbsql "database/sql"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	redis "github.com/redis/go-redis/v9"
)

type dataproxy struct {
	redisC *redis.Client
	scanC  *redis.Client
	dbc    *sqlx.DB
}

func (p *dataproxy) Set(ctx context.Context, key string, value string, version ...int) (ver int, err error) {
	if len(version) > 0 && version[0] > 0 {
		return p.setWithVersion(ctx, key, value, version[0])
	}
	return p.set(ctx, key, value)
}

func (p *dataproxy) set(ctx context.Context, key string, value string) (ver int, err error) {
	//尝试直接更新redis
	if ver, err = RedisSet(ctx, p.redisC, key, value); err != nil {
		if err.Error() == "err_not_in_redis" {
			//写入数据库
			var dbversion int
			if dbversion, err = insertUpdateRowPgsql(ctx, p.dbc, key, value); err == nil {
				ver = dbversion
				RedisLoadSet(ctx, p.redisC, key, dbversion, value)
			}
		}
	}
	return ver, err
}

// 只有版本号一致才能更新
func (p *dataproxy) setWithVersion(ctx context.Context, key string, value string, version int) (ver int, err error) {
	if ver, err = RedisSetWithVersion(ctx, p.redisC, key, value, version); err != nil {
		if err.Error() == "err_not_in_redis" {
			//先尝试更新数据库
			var dbversion int
			if dbversion, err = updateRowPgsql(ctx, p.dbc, key, value, version); err == nil {
				ver = dbversion
				RedisLoadSet(ctx, p.redisC, key, dbversion, value)
			}
		}
	}
	return ver, err
}

func (p *dataproxy) Get(ctx context.Context, key string) (value string, ver int, err error) {
	if value, ver, err = RedisGet(ctx, p.redisC, key); err != nil {
		if err.Error() == "err_not_in_redis" {
			//从数据库加载
			var version int
			version, value, err = queryRow(ctx, p.dbc, key)
			if err == nil || err == dbsql.ErrNoRows {
				value, ver, err = RedisLoadGet(ctx, p.redisC, key, version, value)
			}
		}
	}
	return value, ver, err
}

func (p *dataproxy) SyncDirtyToDB(ctx context.Context) error {
	cursor := uint64(0)
	var keys []string
	var err error

	for {
		keys, cursor, err = p.scanC.Scan(ctx, cursor, "", 10).Result()
		if err != nil {
			return err
		}

		for _, key := range keys {
			r, err := p.redisC.HMGet(ctx, key, "version", "value").Result()
			if err != nil {
				return err
			}
			version, _ := strconv.Atoi(r[0].(string))
			value := r[1].(string)
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
			defer cancel()

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
			err = RedisClearDirty(ctx, p.redisC, key, version)
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
