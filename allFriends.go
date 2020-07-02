package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var redisKey = "chinese:user:all:friends:set:"

type Person struct {
	Player_id string
	Friend_id []string
}

func main() {

	//mongodb
	GlobalMgoSession, err := mgo.Dial("172.18.2.59:27017?maxPoolSize=20")
	if err != nil {
		panic(err)
	}
	defer GlobalMgoSession.Close()

	GlobalMgoSession.SetMode(mgo.Monotonic, true)

	client := redis.NewClient(&redis.Options{
		Addr:        "r-bp1fzqk7he7rt7ot1m.redis.rds.aliyuncs.com:6379",
		Password:    "Sinyee4Redis", // no password set
		DB:          3,              // use default DB
		DialTimeout: 0,
		MaxRetries:  5,
	})

	//复用mongo连接
	session := GlobalMgoSession.Clone()
	defer session.Close()

	mongodbClient := session.DB("chinese").C("pk_player")
	//计算总数
	num, MgoError := mongodbClient.Find(nil).Count()
	if MgoError != nil {
		fmt.Println(MgoError.Error())
	} else {
		fmt.Println(num)
	}

	forever := make(chan bool)
	var list []Person

	go func() {
		for i := 0; i < (num/1000)+1; i++ {
			err = mongodbClient.Find(nil).Select(bson.M{"player_id": "", "friend_id": ""}).Skip(i * 1000).Limit(1000).All(&list)
			if err != nil {
				fmt.Println(err)
			}
			var key string

			for _, value := range list {
				if value.Friend_id == nil {
					continue
				}
				key = redisKey + value.Player_id

				for _, friend := range value.Friend_id {

					fmt.Println(key)
					_, err := client.SAdd(key, friend).Result()
					if err != nil {
						fmt.Println(err)
					}
				}

			}

		}
		forever <- true
	}()
	<-forever
}
