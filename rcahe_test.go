package rcache

//go test -run=Get
//go tool cover -html=coverage.out

import (
	"context"
	"fmt"

	//"sync"
	"testing"
	//"time"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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
