package rcache

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	redis "github.com/redis/go-redis/v9"
)

var cacheTimeout = 1800

type script struct {
	src string
	sha string
}

func newScript(src string) *script {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	return &script{
		src: src,
		sha: hex.EncodeToString(h.Sum(nil)),
	}
}

func (s *script) eval(ctx context.Context, c *redis.Client, keys []string, args ...interface{}) (result interface{}, err error) {
	result, err = c.EvalSha(ctx, s.sha, keys, args...).Result()
	if err != nil && strings.Contains(err.Error(), "NOSCRIPT") {
		result, err = c.Eval(ctx, s.src, keys, args...).Result()
	}
	//fmt.Println(s.src, err)
	return
}

const dirtyKey = "__dirty__"

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
		redis.call('hset',KEYS[2], KEYS[1],version)
		return {'err_ok',version}
	end
`

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

const scriptClearDirty string = `
	local cacheTimeout = %d
	local dirtyKey = KEYS[1]
	local key = KEYS[2]
	local version = redis.call('hget',dirtyKey,key)
	if tonumber(version) == tonumber(ARGV[1]) then
		redis.call('hdel', dirtyKey,key)
		redis.call('Expire',key,cacheTimeout)
	end
`

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

const scriptLoadSet string = `
	local cacheTimeout = %d
	redis.call('select',0)
	local version = redis.call('hget',KEYS[1],'version')
	if not version or tonumber(version) < tonumber(ARGV[1]) then
		redis.call('hmset',KEYS[1],'version',ARGV[1],'value',ARGV[2])
		redis.call('Expire',KEYS[1],cacheTimeout)
	end
`

var (
	set        *script
	get        *script
	loadset    *script
	loadget    *script
	cleardirty *script
)

func InitScript() {
	set = newScript(scriptSet)

	get = newScript(fmt.Sprintf(scriptGet, cacheTimeout))

	loadset = newScript(fmt.Sprintf(scriptLoadSet, cacheTimeout))

	loadget = newScript(fmt.Sprintf(scriptLoadGet, cacheTimeout))

	cleardirty = newScript(fmt.Sprintf(scriptClearDirty, cacheTimeout))
}

func RedisGet(ctx context.Context, c *redis.Client, key string) (value string, version int, err error) {
	var re interface{}
	if re, err = get.eval(ctx, c, []string{key}); err == nil {
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
	var re interface{}
	if re, err = set.eval(ctx, c, []string{key, dirtyKey}, value, version); err == nil {
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
	var r interface{}
	if r, err = loadget.eval(ctx, c, []string{key}, version, v); err == nil {
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
	if _, err = loadset.eval(ctx, c, []string{key}, version, value); err == redis.Nil {
		err = nil
	}
	return err
}

func RedisClearDirty(ctx context.Context, c *redis.Client, key string, version int) (err error) {
	if _, err = cleardirty.eval(ctx, c, []string{dirtyKey, key}, version); err == redis.Nil {
		err = nil
	}
	return err
}
