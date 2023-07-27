package rcache

import (
	"errors"
	"fmt"
	"sync"

	"github.com/go-redis/redis"
)

const scriptSet string = `
	redis.call('select',0)
	local version = redis.call('hget',KEYS[1],'version')
	if not version then
		return 'err_not_in_redis'
	else
		version = tonumber(version) + 1
		redis.call('hmset',KEYS[1],'version',version,'value',ARGV[1])
		--清除ttl
		redis.call('PERSIST',KEYS[1])
		--设置dirty
		redis.call('select',1)
		redis.call('hmset',KEYS[1],'version',version)
		redis.call('select',0)
		return 'err_ok'
	end
`

var scriptSetSha string

const scriptGet string = `
	redis.call('select',0)
	local v = redis.call('hmget',KEYS[1],'version','value')
	local version = v[1]
	local value = v[2]
	if not version then
		return {'err_not_in_redis'}
	elseif tonumber(version) == 0 then
		return {'err_not_exist'}
	else
		version = tonumber(version) + 1
		local ttl = redis.call('ttl',KEYS[1])
		if tonumber(ttl) > 0 then
			redis.call('Expire',KEYS[1],1800) --30分钟后超时
		end
		return {'err_ok',value}
	end
`

var scriptGetSha string

const scriptClearDirty string = `
	redis.call('select',1)
	local version = redis.call('hget',KEYS[1],'version')
	if tonumber(version) == tonumber(ARGV[1]) then
		redis.call('del',KEYS[1])
		redis.call('select',0)
		redis.call('Expire',KEYS[1],1800) --30分钟后超时
	else
		redis.call('select',0)
	end
`

var scriptClearDirtySha string

const scriptLoadGet string = `
	redis.call('select',0)
	local v = redis.call('hmget',KEYS[1],'version','value')
	local version = v[1]
	local value = v[2]
	if version then
		local ttl = redis.call('ttl',KEYS[1])
		if tonumber(ttl) > 0 then
			redis.call('Expire',KEYS[1],1800) --30分钟后超时
		end
		if tonumber(version) > 0 then
			return {'err_ok',value}
		else 
			return {'err_not_exist'}
		end
	else
		if tonumber(ARGV[1]) > 0 then
			redis.call('hmset',KEYS[1],'version',ARGV[1],'value',ARGV[2])
			redis.call('Expire',KEYS[1],1800) --30分钟后超时
			return {'err_ok',ARGV[2]}	
		else
			redis.call('hmset',KEYS[1],'version',ARGV[1])
			redis.call('Expire',KEYS[1],1800) --30分钟后超时
			return {'err_not_exist'}				
		end
	end
`

var scriptLoadGetSha string

const scriptLoadSet string = `
	redis.call('select',0)
	local version = redis.call('hget',KEYS[1],'version')
	if not version or tonumber(version) < tonumber(ARGV[1]) then
		redis.call('hmset',KEYS[1],'version',ARGV[1],'value',ARGV[2])
		redis.call('Expire',KEYS[1],1800) --30分钟后超时
	end
`

var scriptLoadSetSha string

func InitScriptSha(cli *redis.Client) (err error) {
	if scriptSetSha, err = cli.ScriptLoad(scriptSet).Result(); err != nil {
		err = fmt.Errorf("error on scriotSet:%s", err.Error())
		return err
	}

	if scriptGetSha, err = cli.ScriptLoad(scriptGet).Result(); err != nil {
		err = fmt.Errorf("error on scriptGet:%s", err.Error())
		return err
	}

	if scriptClearDirtySha, err = cli.ScriptLoad(scriptClearDirty).Result(); err != nil {
		err = fmt.Errorf("error on scriptClearDirty:%s", err.Error())
		return err
	}

	if scriptLoadGetSha, err = cli.ScriptLoad(scriptLoadGet).Result(); err != nil {
		err = fmt.Errorf("error on scriptLoadGet:%s", err.Error())
		return err
	}

	if scriptLoadSetSha, err = cli.ScriptLoad(scriptLoadSet).Result(); err != nil {
		err = fmt.Errorf("error on scriptLoadSet:%s", err.Error())
		return err
	}

	return err
}

var shaOnce sync.Once

func RedisGet(cli *redis.Client, key string) (value string, err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(cli)
	})
	if err != nil {
		return value, err
	}

	var re interface{}
	re, err = cli.EvalSha(scriptGetSha, []string{key}).Result()
	if err != nil && err.Error() == "redis: nil" {
		err = nil
	}
	if err != nil {
		return value, err
	}

	result := re.([]interface{})

	if len(result) == 1 {
		err = errors.New(result[0].(string))
	} else {
		value = result[1].(string)
	}
	return value, err
}

func RedisSet(cli *redis.Client, key string, value string) (err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(cli)
	})

	if err != nil {
		return err
	}

	var re interface{}

	re, err = cli.EvalSha(scriptSetSha, []string{key}, value).Result()

	if err != nil && err.Error() == "redis: nil" {
		err = nil
	}

	if err == nil {
		result := re.(string)
		if result != "err_ok" {
			err = errors.New(result)
		}
	}
	return err
}

func RedisLoadGet(cli *redis.Client, key string, version int, v string) (value string, err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(cli)
	})

	if err != nil {
		return value, err
	}

	var r interface{}
	r, err = cli.EvalSha(scriptLoadGetSha, []string{key}, version, v).Result()
	if err != nil && err.Error() == "redis: nil" {
		err = nil
	}

	if err == nil {
		result := r.([]interface{})
		if len(result) == 1 {
			err = errors.New(result[0].(string))
		} else {
			value = result[1].(string)
		}
	}
	return value, err
}

func RedisLoadSet(cli *redis.Client, key string, version int, value string) (err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(cli)
	})

	if err != nil {
		return err
	}

	_, err = cli.EvalSha(scriptLoadSetSha, []string{key}, version, value).Result()
	if err != nil && err.Error() == "redis: nil" {
		err = nil
	}

	return err
}

func RedisClearDirty(cli *redis.Client, key string, version int) (err error) {
	shaOnce.Do(func() {
		err = InitScriptSha(cli)
	})

	if err != nil {
		return err
	}

	_, err = cli.EvalSha(scriptClearDirtySha, []string{key}, version).Result()
	if err != nil && err.Error() == "redis: nil" {
		err = nil
	}
	return err
}
