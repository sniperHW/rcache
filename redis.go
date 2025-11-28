package rcache

import (
	"context"
	"errors"
	"fmt"
	"sync"

	redis "github.com/redis/go-redis/v9"
)

var cacheTimeout = 1800

const scriptSet string = `
	local input_version = tonumber(ARGV[2])
	local version = redis.call('hget',KEYS[1],'version')
	if not version then
		return {'err_not_in_redis'}
	else
        version = tonumber(version)
        if input_version > 0 and version ~= input_version then
			return {'err_version_not_match'}
		end	   
		version = version + 1
		redis.call('hmset',KEYS[1],'version',version,'value',ARGV[1])
		--清除ttl
		redis.call('PERSIST',KEYS[1])
		
		--设置dirty
		redis.call('hset','__dirty__', KEYS[1],version)
		return {'err_ok',version}
	end
`

var scriptSetSha string

const scriptGet string = `
	local cacheTimeout = %d
	local v = redis.call('hmget',KEYS[1],'version','value')
	local version = v[1]
	local value = v[2]
	if not version then
		return {'err_not_in_redis'}
	elseif tonumber(version) == 0 then
		return {'err_not_exist'}
	else
		local ttl = redis.call('ttl',KEYS[1])
		if tonumber(ttl) > 0 then
			redis.call('Expire',KEYS[1],cacheTimeout)
		end
		return {'err_ok',value,tonumber(version)}
	end
`

var scriptGetSha string

const scriptClearDirty string = `
	local cacheTimeout = %d
	local version = redis.call('hget','__dirty__',KEYS[1])
	if tonumber(version) == tonumber(ARGV[1]) then
		redis.call('hdel', '__dirty__', KEYS[1])
		redis.call('Expire',KEYS[1],cacheTimeout)
	end
`

var scriptClearDirtySha string

const scriptLoadGet string = `
	local cacheTimeout = %d
	local v = redis.call('hmget',KEYS[1],'version','value')
	local version = v[1]
	local value = v[2]
	if version then
		local ttl = redis.call('ttl',KEYS[1])
		if tonumber(ttl) > 0 then
			redis.call('Expire',KEYS[1],cacheTimeout)
		end
		if tonumber(version) > 0 then
			return {'err_ok',value,tonumber(version)}
		else 
			return {'err_not_exist'}
		end
	else
		if tonumber(ARGV[1]) > 0 then
			redis.call('hmset',KEYS[1],'version',ARGV[1],'value',ARGV[2])
			redis.call('Expire',KEYS[1],cacheTimeout)
			return {'err_ok',ARGV[2],tonumber(ARGV[1])}	
		else
			redis.call('hmset',KEYS[1],'version',ARGV[1])
			redis.call('Expire',KEYS[1],cacheTimeout)
			return {'err_not_exist'}				
		end
	end
`

var scriptLoadGetSha string

const scriptLoadSet string = `
	local cacheTimeout = %d
	redis.call('select',0)
	local version = redis.call('hget',KEYS[1],'version')
	if not version or tonumber(version) < tonumber(ARGV[1]) then
		redis.call('hmset',KEYS[1],'version',ARGV[1],'value',ARGV[2])
		redis.call('Expire',KEYS[1],cacheTimeout)
	end
`

var scriptLoadSetSha string

func InitScriptSha(ctx context.Context, c *redis.Client) (err error) {
	if scriptSetSha, err = c.ScriptLoad(ctx, scriptSet).Result(); err != nil {
		err = fmt.Errorf("error on init scriptSet:%s", err.Error())
		return err
	}

	if scriptGetSha, err = c.ScriptLoad(ctx, fmt.Sprintf(scriptGet, cacheTimeout)).Result(); err != nil {
		err = fmt.Errorf("error on init scriptGet:%s", err.Error())
		return err
	}

	if scriptClearDirtySha, err = c.ScriptLoad(ctx, fmt.Sprintf(scriptClearDirty, cacheTimeout)).Result(); err != nil {
		err = fmt.Errorf("error on init scriptClearDirty:%s", err.Error())
		return err
	}

	if scriptLoadGetSha, err = c.ScriptLoad(ctx, fmt.Sprintf(scriptLoadGet, cacheTimeout)).Result(); err != nil {
		err = fmt.Errorf("error on init scriptLoadGet:%s", err.Error())
		return err
	}

	if scriptLoadSetSha, err = c.ScriptLoad(ctx, fmt.Sprintf(scriptLoadSet, cacheTimeout)).Result(); err != nil {
		err = fmt.Errorf("error on init scriptLoadSet:%s", err.Error())
		return err
	}

	return err
}

var shaOnce sync.Once

func RedisGet(ctx context.Context, c *redis.Client, key string) (value string, version int, err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(ctx, c)
	})

	if err != nil {
		return value, version, err
	}

	var re interface{}
	if re, err = c.EvalSha(ctx, scriptGetSha, []string{key}).Result(); err == nil || err == redis.Nil {
		err = nil
		result := re.([]interface{})
		if len(result) == 1 {
			err = errors.New(result[0].(string))
		} else {
			value = result[1].(string)
			version = int(result[2].(int64))
		}
	}
	return value, version, err
}

func RedisSet(ctx context.Context, c *redis.Client, key string, value string) (ver int, err error) {
	return redisSet(ctx, c, key, value, 0)
}

func RedisSetWithVersion(ctx context.Context, c *redis.Client, key string, value string, version int) (ver int, err error) {
	return redisSet(ctx, c, key, value, version)
}

func redisSet(ctx context.Context, c *redis.Client, key string, value string, version int) (ver int, err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(ctx, c)
	})

	if err != nil {
		return ver, err
	}

	var re interface{}
	if re, err = c.EvalSha(ctx, scriptSetSha, []string{key}, value, version).Result(); err == nil || err == redis.Nil {
		result := re.([]interface{})
		if len(result) == 1 {
			err = errors.New(result[0].(string))
		} else {
			ver = int(result[1].(int64))
		}
	}
	return ver, err
}

func RedisLoadGet(ctx context.Context, c *redis.Client, key string, version int, v string) (value string, ver int, err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(ctx, c)
	})

	if err != nil {
		return value, version, err
	}

	var r interface{}
	if r, err = c.EvalSha(ctx, scriptLoadGetSha, []string{key}, version, v).Result(); err == nil || err == redis.Nil {
		result := r.([]interface{})
		if len(result) == 1 {
			err = errors.New(result[0].(string))
		} else {
			value = result[1].(string)
			ver = int(result[2].(int64))
		}
	}
	return value, ver, err
}

func RedisLoadSet(ctx context.Context, c *redis.Client, key string, version int, value string) (err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(ctx, c)
	})

	if err != nil {
		return err
	}

	if _, err = c.EvalSha(ctx, scriptLoadSetSha, []string{key}, version, value).Result(); err == nil || err == redis.Nil {
		err = nil
	}

	return err
}

func RedisClearDirty(ctx context.Context, c *redis.Client, key string, version int) (err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(ctx, c)
	})

	if err != nil {
		return err
	}

	if _, err = c.EvalSha(ctx, scriptClearDirtySha, []string{key}, version).Result(); err == nil || err == redis.Nil {
		err = nil
	}
	return err
}
