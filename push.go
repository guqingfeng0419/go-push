package main

import (
	"fmt"

	"encoding/json"
	"sync"

	"github.com/go-redis/redis"
)

var wg sync.WaitGroup
var listKey = "chinese:home:user:visit:fake"

func main() {

	client := redis.NewClient(&redis.Options{
		Addr:        "r-bp1fzqk7he7rt7ot1m.redis.rds.aliyuncs.com:6379",
		Password:    "Sinyee4Redis", // no password set
		DB:          3,              // use default DB
		DialTimeout: 0,
		MaxRetries:  5,
	})

	var redisData map[string]string
	redisData = make(map[string]string)
	redisData["visit_id"] = "5484"
	redisData["rand"] = "5"

	data, err := json.Marshal(redisData)
	if err != nil {
		fmt.Println(err)
	}
	go func() {
		for i := 0; i < 10000; i++ {
			res, err := client.RPush(listKey, string(data)).Result()
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(res)
		}
	}()
	go func() {
		for i := 0; i < 10000; i++ {
			res, err := client.RPush(listKey, string(data)).Result()
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(res)
		}
	}()
	select {}
}
