package rcache

//go test -run=Get
//go tool cover -html=coverage.out

import (
	"context"
	"fmt"
	"time"

	//"sync"
	"testing"
	//"time"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestRedisGet(t *testing.T) {
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	r, err := RedisGet(cli, "hello")

	fmt.Println(r, err)

	cli.Eval("redis.call('hmset','hello','version',1,'value','hello')", []string{})

	r, err = RedisGet(cli, "hello")

	fmt.Println(r, err)

	fmt.Println(cli.TTL("hello"))

	cli.Eval("redis.call('del','hello')", []string{})

	r, err = RedisGet(cli, "hello")

	fmt.Println(r, err)
}

func TestRedisLoadGet(t *testing.T) {
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	r, err := RedisLoadGet(cli, "hello", 0, "")

	fmt.Println("loadGet", r, err)

	fmt.Println(cli.TTL("hello"))

	fmt.Println(cli.Del("hello").Result())
}

func TestRedisLoadSet(t *testing.T) {
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	err := RedisLoadSet(cli, "hello", 1, "world")

	fmt.Println(err)

	fmt.Println(cli.TTL("hello"))

	fmt.Println(cli.Del("hello").Result())
}

func TestRedisSet(t *testing.T) {
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	err := RedisSet(cli, "hello", "world")
	if err != nil {
		fmt.Println("Set error", err)
		return
	}

	var re interface{}
	re, err = cli.Eval("return redis.call('hmget','hello','version','value')", []string{}).Result()
	fmt.Println(re, err)
}

func TestPGUpdate(t *testing.T) {
	dbc, err := sqlx.Open("postgres", "host=localhost port=15432 dbname=test user=postgres password=802802 sslmode=disable")

	version, err := updateRowPgsql(context.TODO(), dbc, "hello", "world")

	fmt.Println(version, err)
}

func TestCacheGet(t *testing.T) {
	dbc, err := sqlx.Open("postgres", "host=localhost port=15432 dbname=test user=postgres password=802802 sslmode=disable")
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	proxy := dataproxy{
		rediscli: cli,
		dbc:      dbc,
	}

	fmt.Println(cli.Del("hello").Result())
	dbc.Exec("delete from kv where key = 'hello';")

	value, err := proxy.Get(context.TODO(), "hello")

	fmt.Println(value, err)

	fmt.Println(cli.HGet("hello", "version").Int())

	fmt.Println(cli.TTL("hello"))

}

func TestCacheSet(t *testing.T) {
	dbc, err := sqlx.Open("postgres", "host=localhost port=15432 dbname=test user=postgres password=802802 sslmode=disable")
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	proxy := dataproxy{
		rediscli: cli,
		dbc:      dbc,
	}

	cli.Del("hello").Result()
	dbc.Exec("delete from kv where key = 'hello';")

	err = proxy.Set(context.TODO(), "hello", "world")

	fmt.Println("set", err)

	fmt.Println(cli.HGet("hello", "version").Int())

	fmt.Println(cli.TTL("hello"))

}

func TestScan(t *testing.T) {
	dbc, _ := sqlx.Open("postgres", "host=localhost port=15432 dbname=test user=postgres password=802802 sslmode=disable")

	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	scancli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1,
	})

	proxy := dataproxy{
		rediscli: cli,
		scancli:  scancli,
		dbc:      dbc,
	}

	for i := 0; i < 100; i++ {
		str := fmt.Sprintf("key:%d", i)
		err := proxy.Set(context.TODO(), str, str)
		if err != nil {
			fmt.Println(err)
		}
	}

	err := proxy.SyncDirtyToDB()
	if err != nil {
		fmt.Println(err)
	}
}

func TestRCache(t *testing.T) {
	dbc, _ := sqlx.Open("postgres", "host=localhost port=15432 dbname=test user=postgres password=802802 sslmode=disable")

	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	scancli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1,
	})

	proxy := dataproxy{
		rediscli: cli,
		scancli:  scancli,
		dbc:      dbc,
	}

	cacheTimeout = 5

	cli.Del("testkey").Result()
	dbc.Exec("delete from kv where key = 'testkey';")

	value, err := proxy.Get(context.TODO(), "testkey")

	fmt.Println(value)

	assert.Equal(t, "err_not_exist", err.Error())

	//直接从redis获取
	version, _ := cli.HGet("testkey", "version").Result()
	assert.Equal(t, "0", version)

	//等待timeout
	fmt.Println("wait for timeout...")
	time.Sleep(time.Second * 5)
	_, err = cli.HGet("testkey", "version").Result()
	assert.Equal(t, err.Error(), "redis: nil")

	err = proxy.Set(context.TODO(), "testkey", "testvalue")

	assert.Nil(t, err)
	version, _ = cli.HGet("testkey", "version").Result()
	assert.Equal(t, "1", version)

	fmt.Println(cli.TTL("testkey"))

	err = proxy.Set(context.TODO(), "testkey", "testvalue2")

	assert.Nil(t, err)
	version, _ = cli.HGet("testkey", "version").Result()
	assert.Equal(t, "2", version)

	fmt.Println(cli.TTL("testkey"))

	value, _ = proxy.Get(context.TODO(), "testkey")
	assert.Equal(t, value, "testvalue2")
	fmt.Println(cli.TTL("testkey"))

	proxy.SyncDirtyToDB()

	fmt.Println(cli.TTL("testkey"))

	fmt.Println("wait for timeout...")
	time.Sleep(time.Second * 5)
	_, err = cli.HGet("testkey", "version").Result()
	assert.Equal(t, err.Error(), "redis: nil")

}
