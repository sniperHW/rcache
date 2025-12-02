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
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func initRedis() *redis.Client {
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	InitScript()
	return cli
}

func TestRedisGet(t *testing.T) {
	cli := initRedis()

	cli.FlushAll(context.TODO()).Result()

	r, ver, err := RedisGet(context.Background(), cli, "hello")

	fmt.Println(r, ver, err)

	cli.Eval(context.Background(), "redis.call('hmset','hello','version',1,'value','hello')", []string{})

	r, ver, err = RedisGet(context.Background(), cli, "hello")

	fmt.Println(r, ver, err)

	fmt.Println(cli.TTL(context.Background(), "hello"))

	cli.Eval(context.Background(), "redis.call('del','hello')", []string{})

	r, ver, err = RedisGet(context.Background(), cli, "hello")

	fmt.Println(r, ver, err)
}

func TestRedisLoadGet(t *testing.T) {
	cli := initRedis()

	cli.FlushAll(context.TODO()).Result()

	r, v, err := RedisLoadGet(context.Background(), cli, "hello", 0, "")

	fmt.Println("loadGet", r, v, err)

	fmt.Println(cli.TTL(context.Background(), "hello"))

	fmt.Println(cli.Del(context.Background(), "hello").Result())

	r, v, err = RedisLoadGet(context.Background(), cli, "hello", 1, "world")

	fmt.Println("loadGet again", r, v, err)

	fmt.Println(cli.TTL(context.Background(), "hello"))

	fmt.Println(cli.Del(context.Background(), "hello").Result())
}

func TestRedisLoadSet(t *testing.T) {
	cli := initRedis()

	cli.FlushAll(context.TODO()).Result()

	err := RedisLoadSet(context.Background(), cli, "hello", 2, "world")

	fmt.Println(err)

	fmt.Println(cli.TTL(context.Background(), "hello"))

	time.Sleep(time.Second * 2)

	err = RedisLoadSet(context.Background(), cli, "hello", 1, "world")

	fmt.Println(err)

	fmt.Println(cli.TTL(context.Background(), "hello"))

	err = RedisLoadSet(context.Background(), cli, "hello", 3, "world")

	fmt.Println(err)

	fmt.Println(cli.TTL(context.Background(), "hello"))

	fmt.Println(cli.Del(context.Background(), "hello").Result())
}

func TestRedisSet(t *testing.T) {
	cli := initRedis()

	cli.FlushAll(context.Background()).Result()

	ver, err := RedisSet(context.Background(), cli, "hello", "world")
	fmt.Println(err, ver)

	RedisLoadSet(context.Background(), cli, "hello", 1, "world")

	fmt.Println(RedisGet(context.Background(), cli, "hello"))

	ver, err = RedisSet(context.Background(), cli, "hello", "world2")
	fmt.Println(err, ver)

	fmt.Println(RedisGet(context.Background(), cli, "hello"))

	//版本不一致，更新失败
	ver, err = RedisSetWithVersion(context.Background(), cli, "hello", "world3", 1)
	fmt.Println(err, ver)

	ver, err = RedisSetWithVersion(context.Background(), cli, "hello", "world3", ver)
	fmt.Println(err, ver)

	fmt.Println(RedisGet(context.Background(), cli, "hello"))

}

func TestPGUpdate(t *testing.T) {
	dbc, err := sqlx.Open("postgres", "host=localhost port=5432 dbname=test user=postgres password=802802 sslmode=disable")

	if err != nil {
		t.Fatal(err)
	}

	dbc.ExecContext(context.TODO(), "delete from kv;")

	defer dbc.Close()

	version, err := insertUpdateRowPgsql(context.TODO(), dbc, "hello", "world")

	fmt.Println(version, err)
}

func TestCacheGet(t *testing.T) {
	dbc, err := sqlx.Open("postgres", "host=localhost port=5432 dbname=test user=postgres password=802802 sslmode=disable")

	if err != nil {
		t.Fatal(err)
	}

	cli := initRedis()

	dbc.ExecContext(context.TODO(), "delete from kv;")
	cli.FlushAll(context.TODO()).Result()

	defer dbc.Close()

	proxy := NewDataProxy(cli, dbc)

	value, _, err := proxy.Get(context.TODO(), "hello", 10)

	fmt.Println(value, err)

	fmt.Println(cli.HGet(context.TODO(), "hello", "version").Int())

	fmt.Println(cli.TTL(context.TODO(), "hello").Result())

}

func TestCacheSet(t *testing.T) {
	dbc, err := sqlx.Open("postgres", "host=localhost port=5432 dbname=test user=postgres password=802802 sslmode=disable")

	if err != nil {
		t.Fatal(err)
	}

	cli := initRedis()

	dbc.ExecContext(context.TODO(), "delete from kv;")
	cli.FlushAll(context.TODO()).Result()

	defer dbc.Close()

	proxy := NewDataProxy(cli, dbc)

	_, err = proxy.Set(context.TODO(), "hello", "world")

	fmt.Println("set", err)

	fmt.Println(cli.HGet(context.TODO(), "hello", "version").Int())

	fmt.Println(cli.TTL(context.TODO(), "hello").Result())

}

func TestRedisSetOnly(t *testing.T) {
	cli := initRedis()

	cli.FlushAll(context.TODO()).Result()

	beg := time.Now()
	for i := 0; i < 5000; i++ {
		_, err := cli.Set(context.TODO(), fmt.Sprintf("key:%d", i), fmt.Sprintf("value:%d", i), time.Second*10).Result()
		if err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println("use time:", time.Since(beg))
}

func TestScan(t *testing.T) {
	dbc, _ := sqlx.Open("postgres", "host=localhost port=5432 dbname=test user=postgres password=802802 sslmode=disable")

	//defaultCacheTimeout = 30

	cli := initRedis()

	dbc.ExecContext(context.TODO(), "delete from kv;")
	cli.FlushAll(context.TODO()).Result()

	defer dbc.Close()

	proxy := NewDataProxy(cli, dbc)

	for i := 0; i < 5000; i++ {
		str := fmt.Sprintf("key:%d", i)
		_, err := proxy.Set(context.TODO(), str, str, 30)
		if err != nil {
			fmt.Println(err)
		}
	}

	beg := time.Now()
	for i := 0; i < 5000; i++ {
		str := fmt.Sprintf("key:%d", i)
		_, err := proxy.Set(context.TODO(), str, str, 30)
		if err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println("use time:", time.Since(beg))

	err := proxy.SyncDirtyToDB(context.TODO())
	if err != nil {
		fmt.Println(err)
	}
}

func TestRCache(t *testing.T) {
	dbc, _ := sqlx.Open("postgres", "host=localhost port=5432 dbname=test user=postgres password=802802 sslmode=disable")

	defaultCacheTimeout = 5

	cli := initRedis()

	dbc.ExecContext(context.TODO(), "delete from kv;")
	cli.FlushAll(context.TODO()).Result()

	defer dbc.Close()

	proxy := NewDataProxy(cli, dbc)

	value, _, err := proxy.Get(context.TODO(), "testkey")

	fmt.Println(value)

	assert.Equal(t, "err_not_exist", err.Error())

	//直接从redis获取
	version, _ := cli.HGet(context.TODO(), "testkey", "version").Result()
	assert.Equal(t, "0", version)

	//等待timeout
	fmt.Println("wait for timeout...")
	time.Sleep(time.Second * 5)
	_, err = cli.HGet(context.TODO(), "testkey", "version").Result()
	assert.Equal(t, err.Error(), "redis: nil")

	_, err = proxy.Set(context.TODO(), "testkey", "testvalue")

	assert.Nil(t, err)
	version, _ = cli.HGet(context.TODO(), "testkey", "version").Result()
	assert.Equal(t, "1", version)

	fmt.Println(cli.TTL(context.TODO(), "testkey").Result())

	_, err = proxy.Set(context.TODO(), "testkey", "testvalue2")

	assert.Nil(t, err)
	version, _ = cli.HGet(context.TODO(), "testkey", "version").Result()
	assert.Equal(t, "2", version)

	fmt.Println(cli.TTL(context.TODO(), "testkey").Result())

	value, _, _ = proxy.Get(context.TODO(), "testkey")
	assert.Equal(t, value, "testvalue2")
	fmt.Println(cli.TTL(context.TODO(), "testkey").Result())

	proxy.SyncDirtyToDB(context.TODO())

	fmt.Println(cli.TTL(context.TODO(), "testkey").Result())

	fmt.Println("wait for timeout...")
	time.Sleep(time.Second * 5)
	_, err = cli.HGet(context.TODO(), "testkey", "version").Result()
	assert.Equal(t, err.Error(), "redis: nil")
}
