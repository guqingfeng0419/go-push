package main

import (
	"fmt"

	"github.com/go-redis/redis"

	"time"

	"os"

	"sync"

	"math/rand"
)

var wg sync.WaitGroup

func main() {

	client := redis.NewClient(&redis.Options{
		Addr:     "10.1.14.187:6938",
		Password: "sinyee4redis", // no password set
		DB:       0,              // use default DB
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)

	now := time.Now()
	nowTime := now.Format("2006-01-02 15:04:05")
	fmt.Println(nowTime)

	for i := 0; i < 100; i++ {

		wg.Add(100)

		go func() {

			for j := 0; j < 100; j++ {

				num := rand.Intn(99999999)
				client.LPush("wx:world:list:push", num).Result()

			}
			wg.Done()
		}()
	}

	wg.Wait()

	end := time.Now()
	endTime := end.Format("2006-01-02 15:04:05")
	fmt.Println(nowTime)
	fmt.Println(endTime)

	os.Exit(1)

	return
}
