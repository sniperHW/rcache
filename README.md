# rcache

rcache 是一个两层缓存系统，通过 DataProxy 将 Redis（热数据缓存）与 PostgreSQL/MySQL（冷数据存储）组合使用。使用版本号实现乐观并发控制，防止并发更新时的丢失更新问题。

```
Client → DataProxy → Redis (Hot Cache) ↔ PostgreSQL/MySQL (Cold Storage)
```

## 安装

```bash
go get github.com/sniperHW/rcache
```

## 依赖

- Redis
- PostgreSQL 或 MySQL
- `github.com/redis/go-redis/v9`
- `github.com/jmoiron/sqlx`

## 数据库建表

```sql
CREATE TABLE kv (
    key    VARCHAR PRIMARY KEY,
    value  TEXT NOT NULL,
    version INTEGER NOT NULL
);
```

## 使用约束

1. **Lua 脚本在包级别变量声明时自动初始化**，无需手动调用。
2. **Redis 中的 key 以 Hash 结构存储**，包含 `version`、`value`、`__cache_timeout__` 三个字段，请不要在外部直接操作这些 key，以免破坏数据一致性。
3. **`__dirty__` 是系统保留的 Redis Hash key**，用于追踪已修改但尚未同步到数据库的缓存条目。
4. **默认缓存超时时间为 1800 秒（30 分钟）**，可通过 `cacheTimeout` 参数自定义。缓存过期后，下次访问将触发 cache miss 流程从数据库重新加载。
5. **`SyncDirtyToDB()` 需要由调用方定期调用**，将脏数据写回数据库。Set 操作只会更新 Redis 并标记为脏，不会立即写回数据库。
6. **`SetWithVersion()` 要求版本号匹配才能更新**，适用于需要乐观并发控制的场景。版本号不匹配时返回 `err_version_not_match` 错误。
7. **Get 一个不存在的 key 时返回 `err_not_exist` 错误**，但会在 Redis 中写入 `version=0` 的占位记录以防止缓存穿透。该占位记录同样受 TTL 控制。

## 快速开始

```go
package main

import (
    "context"
    "fmt"

    _ "github.com/lib/pq"
    "github.com/jmoiron/sqlx"
    redis "github.com/redis/go-redis/v9"
    "github.com/sniperHW/rcache"
)

func main() {
    redisC := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    dbc, _ := sqlx.Open("postgres", "host=localhost port=5432 dbname=test user=postgres password=123456 sslmode=disable")

    proxy := rcache.NewDataProxy(redisC, dbc)

    // Set
    version, err := proxy.Set(context.TODO(), "mykey", "myvalue")
    // ... sync dirty entries periodically
    proxy.SyncDirtyToDB(context.TODO())

    // Get
    value, ver, err := proxy.Get(context.TODO(), "mykey")

    // SetWithVersion (乐观锁)
    newVer, err := proxy.SetWithVersion(context.TODO(), "mykey", "newvalue", ver)
}
```

## Get 操作流程

### 缓存命中

key 已存在于 Redis 中（`version > 0`）：

```
Client          DataProxy           Redis             PostgreSQL
   |--- Get ----->|                  |                    |
   |              |---- hmget ------>|                    |
   |              |<--- value -------|                    |
   |<-- value ----|                  |                    |
```

1. DataProxy 向 Redis 发送 `hmget` 读取 `version` 和 `value`
2. 若 `version > 0` 且 value 存在，直接返回 value 和 version
3. 若 TTL 已过期但 key 尚未淘汰，会自动重新设置过期时间

### 缓存未命中

key 不在 Redis 中（不存在或已过期）：

```
Client          DataProxy           Redis             PostgreSQL
   |--- Get ----->|                  |                    |
   |              |---- hmget ------>|                    |
   |              |<--- nil ---------|                    |
   |              |                  |---- query -------->|
   |              |                  |<--- value ---------|
   |              |---- hmset ----->|                    |
   |<-- value ----|                  |                    |
```

1. DataProxy 向 Redis 发送 `hmget`，返回空
2. 从 PostgreSQL 查询该 key 的 `version` 和 `value`
3. 将查询结果通过 `hmset` 写入 Redis 并设置 TTL
4. 若 key 在数据库中也不存在，在 Redis 中写入 `version=0` 的占位记录，防止缓存穿透
5. 返回 value 和 version（key 不存在时返回 `err_not_exist`）

## Set 操作流程

### 缓存命中

key 已存在于 Redis 中：

```
Client          DataProxy           Redis             PostgreSQL
   |--- Set ----->|                  |                    |
   |              |---- hmset ------>|                    |
   |              |<--- ok ----------|                    |
   |<-- version --|                  |                    |
   |              |                  |                    |
   |              |   (异步回写)      |---- writeback ---> |
   |              |                  |<--- ok ---------- |
   |              |---- clear dirty >|                    |
```

1. DataProxy 通过 Lua 脚本原子更新 Redis 中的 `version`（+1）和 `value`
2. 清除该 key 的 TTL（设为永久），保持数据直到被同步回写
3. 在 `__dirty__` 哈希中标记该 key 为脏数据
4. 返回新 version
5. 调用方通过 `SyncDirtyToDB()` 将脏数据批量写回 PostgreSQL，写回成功后清除 dirty 标记并恢复 TTL

### 缓存未命中

key 不在 Redis 中：

```
Client          DataProxy           Redis             PostgreSQL
   |--- Set ----->|                  |                    |
   |              |---- hmset ------>|                    |
   |              |<- not in redis --|                    |
   |              |                  |--- insert/update ->|
   |              |                  |<--- ok ------------|
   |              |---- hmset ----->|                    |
   |<-- version --|                  |                    |
```

1. DataProxy 尝试更新 Redis，但 key 不存在，返回 `err_not_in_redis`
2. 直接向 PostgreSQL 执行 `INSERT ... ON CONFLICT UPDATE` 语句（upsert）
3. 获取数据库返回的 version
4. 将数据通过 `hmset` 加载到 Redis 并设置 TTL
5. 返回 version

## SetWithVersion 操作流程

与 Set 流程类似，但增加了乐观并发控制：

- **缓存命中时**：Lua 脚本在 Redis 中原子检查版本号，只有传入的 version 与当前 version 一致时才执行更新，否则返回 `err_version_not_match`
- **缓存未命中时**：通过 PostgreSQL 的 `UPDATE ... WHERE version = ?` 语句进行条件更新，同样只有版本匹配时才能成功

## Dirty 数据同步

`SyncDirtyToDB()` 负责将所有标记为脏的缓存条目写回数据库：

1. 使用 `HSCAN` 遍历 `__dirty__` 哈希，每次批量处理 100 条
2. 读取每条脏数据的 `version` 和 `value`
3. 执行 `INSERT ... ON CONFLICT UPDATE WHERE version < ?` 写回数据库（仅在版本更新时才写入）
4. 写回成功后清除 dirty 标记并恢复 key 的 TTL

## API

```go
// 创建 DataProxy 实例
func NewDataProxy(redisC *redis.Client, dbc *sqlx.DB) *DataProxy

// 获取缓存值
func (p *DataProxy) Get(ctx context.Context, key string, cacheTimeout ...int) (value string, version int, err error)

// 设置缓存值
func (p *DataProxy) Set(ctx context.Context, key string, value string, cacheTimeout ...int) (version int, err error)

// 设置缓存值（带版本检查）
func (p *DataProxy) SetWithVersion(ctx context.Context, key string, value string, version int, cacheTimeout ...int) (ver int, err error)

// 将脏数据同步到数据库
func (p *DataProxy) SyncDirtyToDB(ctx context.Context) error
```

## 错误类型

| 错误 | 含义 |
|------|------|
| `err_not_in_redis` | key 不在 Redis 缓存中（内部错误，用于触发 cache miss 流程） |
| `err_not_exist` | key 在数据库和缓存中均不存在 |
| `err_version_not_match` | 版本号不匹配，更新被拒绝（仅 `SetWithVersion`） |
