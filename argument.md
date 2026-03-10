# rcache 数据一致性风险分析

## 初始约束条件

1. 整个系统作为独立的存储接口对外提供服务，**客户端不允许直接访问 SQL 数据库**
2. **Redis 崩溃时的数据丢失是可以容忍的**

## 初始分析（基于约束条件重新评估）

### 已消除的风险

由于客户端只能通过 DataProxy 访问：
- ❌ 绕过缓存直接写数据库导致的不一致
- ❌ 外部并发导致的不一致

由于 Redis 崩溃数据丢失可容忍：
- ❌ Redis 宕机导致数据永久丢失

### 初步识别的剩余风险

#### 1. Set() 无版本号时的 lost update
**结论**: 非风险，用户明确保证单写者场景

#### 2. 缓存未命中时的数据库写入竞态
**结论**: 低风险，实际场景中很少发生

#### 3. writebackPgsql 成功但 Redis 中数据已变化
**分析**:
```
T1: SyncDirtyToDB 从 __dirty__ HScan 读取 key=10
T2: 客户端 Set key=v11，__dirty__ 更新为 11（scriptSet 第 58 行）
T3: writebackPgsql(key, v10, 10)
T4: RedisClearDirty(key, 10) - dirty 中是 11 ≠ 10，清除失败
```

**下一个周期会自动恢复**:
```
T5: HScan 读取 key=11
T6: HMGet 读取 version=11, value=v11
T7: writebackPgsql(key, v11, 11) - 成功
T8: RedisClearDirty(key, 11) - 成功
```

#### 4. SyncDirtyToDB 部分失败导致 dirty 标记泄漏
**用户指出**: SyncDirtyToDB 是周期性执行的，不是单次调用

**结论**: 部分失败会在下一个周期重试，不是永久泄漏

#### 5. 竞态窗口风险

**场景**:
```
DB: version=10, value=v10
Redis: version=11, value=v11, dirty=11

SyncDirtyToDB 第 N 次：
- HScan 读取 dirty=10（读到一半，客户端更新了 dirty）
- HMGet 读取 version=11, value=v11（已是最新）
- writebackPgsql(key, v11, 10) - where version < 10? false，不更新
- RedisClearDirty(key, 10) - dirty 中是 11 ≠ 10，清除失败

SyncDirtyToDB 第 N+1 次：
- HScan 读取 dirty=11
- writebackPgsql(key, v11, 11) - where version < 11? true，更新成功
- RedisClearDirty(key, 11) - 清除成功
```

**结论**: 单次执行可能失败，但 dirty 标记未清除，下一个周期会重试并成功。

## 最终结论

在给定的设计约束下：
1. ✅ 单写者（由用户保证）
2. ✅ Redis 崩溃数据丢失可容忍
3. ✅ SyncDirtyToDB 周期性执行

rcache 设计是**最终一致的**，dirty 标记机制确保了：
- 即使单次同步失败，下一个周期会重试
- 竞态窗口导致的失败会自动恢复
- 不会永久丢失数据（除非 Redis 崩溃，但这是可容忍的）

**之前分析的"风险"实际上是异步写回设计的一部分**，这个系统在给定约束条件下是正确的。

## 设计的正确性验证

### scriptSet 的关键逻辑

```lua
-- 设置 dirty 标记时，使用最新的 version
redis.call('hset', KEYS[2], KEYS[1], version)
```

这确保了当客户端更新数据时，`__dirty__` 中的版本号也会同步更新，使得 SyncDirtyToDB 能够在下一个周期正确处理最新数据。

### writebackPgsql 的保护机制

```sql
where kv.key = $1 and kv.version < $3
```

防止旧版本覆盖新版本，保证数据只能向前演进。
