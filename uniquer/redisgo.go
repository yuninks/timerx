package uniquer

import (
	"errors"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

type uniqueRedisgo struct {
	Redis *redis.Pool
}

var exporeSecond int64 = 15

func NewUniqueRedisGo(redis *redis.Pool) *uniqueRedisgo {
	return &uniqueRedisgo{redis}
}

func (u *uniqueRedisgo) SetLimit(key, value string) error {
	conn := u.Redis.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		fmt.Println("get redis connect fail, err: ", err)
		return err
	}

	response, err := conn.Do("SET", key, value, "NX", "EX", exporeSecond)

	// fmt.Println("SetLimit:", response, err)

	if err != nil {
		fmt.Println("redis setex fail, err: ", err)
		return err
	}

	if response != "OK" {
		return errors.New("response not ok")
	}

	return nil

}

func (u *uniqueRedisgo) DeleteLimit(key, value string) error {
	conn := u.Redis.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		fmt.Println("get redis connect fail, err: ", err)
		return err
	}

	_, err := conn.Do("WATCH", key)
	if err != nil {
		return err
	}
	defer conn.Do("UNWATCH")

	val, err := redis.String(conn.Do("GET", key))
	// fmt.Println("DeleteLimit Do:", val, err)
	if err != nil {
		return err
	}
	if val != value {
		return errors.New("值不一致")
	}
	// 处理
	err = conn.Send("MULTI")
	if err != nil {
		return err
	}
	err = conn.Send("DEL", key)
	if err != nil {
		return err
	}
	reply, err := conn.Do("EXEC")
	// fmt.Printf("DeleteLimit EXEC:%T val:%+v err:%+v\n", reply, reply, err)
	if err != nil {
		return err
	}

	if reply == nil {
		return errors.New("删除失败")
	}

	return nil
}

func (u *uniqueRedisgo) RefreshLimit(key, value string) error {
	conn := u.Redis.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		fmt.Println("get redis connect fail, err: ", err)
		return err
	}

	_, err := conn.Do("WATCH", key)
	if err != nil {
		return err
	}
	defer conn.Do("UNWATCH")

	val, err := redis.String(conn.Do("GET", key))

	// fmt.Println("RefreshLimit GET:", val, err, value)
	if err != nil {
		return err
	}
	if val != value {
		return errors.New("值不一致")
	}

	// time.Sleep(time.Second * 5)

	// 处理
	err = conn.Send("MULTI")
	// fmt.Println("RefreshLimit MULTI:", err)
	if err != nil {
		return err
	}

	err = conn.Send("EXPIRE", key, exporeSecond)
	// fmt.Println("RefreshLimit EXPIRE:", err)
	if err != nil {
		return err
	}

	// 管道
	// reply 执行失败将会返回nil 执行成功就是返回的值
	reply, err := conn.Do("EXEC")

	// fmt.Printf("RefreshLimit EXEC:%T val:%+v err:%+v\n", reply, reply, err)
	// i, ok := reply.([]interface{})
	// if ok {
	// 	fmt.Printf("reply i %T\n", i[0])
	// 	uu, ok := i[0].([]uint8)
	// 	if ok {
	// 		fmt.Printf("reply %+v\n", string(uu))
	// 	}
	// }

	if err != nil {
		return err
	}

	if reply == nil {
		return errors.New("更新失败")
	}

	return nil
}
