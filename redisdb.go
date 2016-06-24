package dhwdb

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

const (
	//短信验证码
	SmsCodeDb = 1
	//用户访问Token
	UserTokenDb = 2
	//序列库
	SeqDb = 3
	//
	// WxToken
	WxDBToken = 4
)

var RedisClient *redisTool

type redisTool struct {
	pool *redis.Pool
}

//初始化redis连接池
func init() {
	redisServer := "127.0.0.1:6379"
	//redisPassword := ""
	//创建Redis连接池
	RedisClient = &redisTool{
		pool: &redis.Pool{
			//最大空闲连接
			MaxIdle: 100,
			//最大活动连接
			MaxActive: 1000,
			//空闲连接过期时间
			IdleTimeout: 600 * time.Second,

			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", redisServer)
				if err != nil {
					return nil, err
				}
				//if _, err := c.Do("AUTH", redisPassword); err != nil {
				//	c.Close()
				//	return nil, err
				//}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}

}

//设置默认数据库
func (p *redisTool) SetDefaultKeyValue(key string, value interface{}) (err error) {

	con := p.pool.Get()
	defer con.Close()
	_, err = con.Do("SELECT", 0)
	if err != nil {
		log.Fatal(err)
		return err
	} else {
		if _, err := con.Do("SET", key, value); err == nil {
			return nil
		} else {
			log.Fatal(err)
			return err
		}
	}
}

//通过Key获取值
func (p *redisTool) GetDefaultValue(key string) (res string, err error) {
	con := p.pool.Get()
	defer con.Close()
	_, err = con.Do("SELECT", 0)
	if err != nil {
		log.Fatal(err)
		return "", err
	} else {
		if res, err := redis.String(con.Do("GET", key)); err == nil {
			return res, nil
		} else {
			log.Fatal(err)
			return "", err
		}
	}
}

func (p *redisTool) GetValue(db int, key string) (res string) {
	con := p.pool.Get()
	defer con.Close()
	_, err := con.Do("SELECT", db)
	if err != nil {
		log.Fatal(err)
		return ""
	} else {
		reply, err := con.Do("GET", key)
		if value, err := redis.String(reply, err); err == nil {
			return value
		} else {
			return ""
		}

	}
}

func (p *redisTool) SetKeyValue(db int, key string, value interface{}) (err error) {
	con := p.pool.Get()
	defer con.Close()
	_, err = con.Do("SELECT", db)
	if err != nil {
		log.Fatal(err)
		return err
	} else {
		if _, err := con.Do("SET", key, value); err == nil {
			return nil
		} else {
			log.Fatal(err)
			return err
		}
	}
}

//存储哈希数据
func (p *redisTool)SetHashValue(db int, key string, field string, value interface{}) (err error) {
	con := p.pool.Get()
	defer con.Close()
	_, err = con.Do("SELECT", db)
	if err != nil {
		log.Fatal(err)
		return err
	} else {
		if _, err := con.Do("HSET", key, field, value); err == nil {
			return nil
		} else {
			log.Fatal(err)
			return err
		}
	}
}

//获取哈希数据
func (p *redisTool)GetHashValue(db int, key string, field string) (value interface{}) {
	con := p.pool.Get()
	defer con.Close()
	_, err := con.Do("SELECT", db)
	if err != nil {
		log.Fatal(err)
		return nil
	} else {
		reply, err := con.Do("HGET", key, field)
		if value, err := redis.String(reply, err); err == nil {
			return value
		} else {
			return nil
		}
	}
}


//删除键值数据
func (p *redisTool)DeleteKeyValue(db int, key string) (err error) {
	con := p.pool.Get()
	defer con.Close()
	_, err = con.Do("SELECT", db)
	if err != nil {
		log.Fatal(err)
		return err
	} else {
		if _, err := con.Do("DEL", key); err == nil {
			return nil
		} else {
			log.Fatal(err)
			return err
		}
	}
}