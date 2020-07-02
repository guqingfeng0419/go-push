package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"gopkg.in/mgo.v2"
)

var listKey = "chinese:home:user:visit:fake"
var redisKey = "chinese:all:userId:set"
var wg sync.WaitGroup

func main() {

	//mongodb
	GlobalMgoSession, err := mgo.Dial("172.18.2.59:27017?maxPoolSize=20")
	if err != nil {
		panic(err)
	}
	defer GlobalMgoSession.Close()

	GlobalMgoSession.SetMode(mgo.Monotonic, true)
	//redis
	client := redis.NewClient(&redis.Options{
		Addr:        "r-bp1fzqk7he7rt7ot1m.redis.rds.aliyuncs.com:6379",
		Password:    "Sinyee4Redis", // no password set
		DB:          3,              // use default DB
		DialTimeout: 0,
		MaxRetries:  5,
	})
	for {
		num, err := client.LLen(listKey).Result()

		if err != nil {
			fmt.Println(err)
		}

		if num <= 0 {
			fmt.Println("wait")
			time.Sleep(1 * time.Second)
			continue
		}

		for i := 0; i < 20; i++ {
			if num == 0 {
				break
			}
			wg.Add(1)
			num = num - 1
			go func() {

				var redisData string
				var redisDataMap map[string]string

				redisData, err = client.LPop(listKey).Result()

				if err != nil {
					return
				}

				if redisData == "" {
					return
				}

				err = json.Unmarshal([]byte(redisData), &redisDataMap)

				if err != nil {
					return
				}

				if len(redisDataMap) == 0 {
					return
				}

				var randNum int64
				randNum, err := strconv.ParseInt(redisDataMap["rand"], 10, 64)
				if err != nil {
					return
				}
				visitUserId, err := client.SRandMemberN(redisKey, randNum).Result()
				if err != nil {
					return
				}

				//复用mongo连接
				session := GlobalMgoSession.Clone()
				defer session.Close()

				var mongoMap map[string]interface{}
				mongodbClient := session.DB("chinese").C("home_visit_log")
				for _, visitId := range visitUserId {

					if visitId == redisDataMap["user_id"] {
						continue
					}
					mongoMap = make(map[string]interface{})
					mongoMap["user_id"] = redisDataMap["user_id"]
					mongoMap["visit_id"] = visitId
					mongoMap["type"] = 2
					mongoMap["create_time"] = time.Now().Unix()
					err = mongodbClient.Insert(mongoMap)
					if err != nil {
						fmt.Println(err)

					}
					fmt.Println(visitId)
				}
				defer wg.Done()
			}()
		}
	}
}
