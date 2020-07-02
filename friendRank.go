package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"strconv"
	"sync"
	"time"
)

var redisKey = "chinese:user:all:friends:set:"
var rankRedisKey = "chinese:home:user:friends:rank:set:"
var spriteNumRedisKey = "chinese:home:user:sprite:num:"

var wg sync.WaitGroup

type Player struct {
	Player_id string
	Friend_id []string
}

type Sprites struct {
	Hp       float32 `bson:"Hp"`
	SpriteID string  `bson:"SpriteID"`
	Blood    float32 `bson:"Blood"`
}

type SaveData struct {
	Baby_id    int
	GotSprites []Sprites `bson:"GotSprites"`
}

func main() {

	now := time.Now()
	nowTime := now.Format("2006-01-02 15:04:05")
	fmt.Println(nowTime)

	//mongodb
	GlobalMgoSession, err := mgo.Dial("172.18.2.59:27017?maxPoolSize=20")
	if err != nil {
		panic(err)
	}
	defer GlobalMgoSession.Close()

	GlobalMgoSession.SetMode(mgo.Monotonic, true)

	//复用mongo连接
	session := GlobalMgoSession.Clone()
	defer session.Close()

	mongodbClient := session.DB("chinese").C("pk_player")
	saveDataClient := session.DB("chinese").C("save_data")
	//redis
	client := redis.NewClient(&redis.Options{
		Addr:        "r-bp1fzqk7he7rt7ot1m.redis.rds.aliyuncs.com:6379",
		Password:    "Sinyee4Redis", // no password set
		DB:          3,              // use default DB
		DialTimeout: 0,
		MaxRetries:  5,
	})

	//计算总数
	num, MgoError := mongodbClient.Find(nil).Count()
	if MgoError != nil {
		fmt.Println(MgoError.Error())
	} else {
		fmt.Println(num)
	}

	var list []Player
	var spriteInfo SaveData
	var realNum int

	forever := make(chan bool)

	go func() {
		for i := 0; i < (num/1000)+1; i++ {

			err = mongodbClient.Find(nil).Select(bson.M{"player_id": "", "friend_id": ""}).Skip(i * 1000).Limit(1000).All(&list)
			if err != nil {
				fmt.Println(err)
			}

			var key string
			var rankKey string
			var spriteNumKey string

			for _, value := range list {
				if len(value.Player_id) >= 12 {
					continue
				}
				//实际非机器人人数
				realNum = realNum + 1

				key = redisKey + value.Player_id
				rankKey = rankRedisKey + value.Player_id

				value.Friend_id = append(value.Friend_id, value.Player_id)

				for _, friend := range value.Friend_id {

					fmt.Println("当前user_id:" + value.Player_id + " 好友：" + friend)

					//自身不进入好友集合
					if friend != value.Player_id {
						_, err := client.SAdd(key, friend).Result()
						if err != nil {
							fmt.Println(err)
						}
					}

					friendId, err := strconv.Atoi(friend)
					if err != nil {
						fmt.Println(err)
					}
					err = saveDataClient.Find(bson.M{"baby_id": friendId}).Select(bson.M{"baby_id": "", "GotSprites": 1}).One(&spriteInfo)
					if err != nil {
						fmt.Println(err)
					}
					fmt.Println("精灵数目:", len(spriteInfo.GotSprites))

					spriteNum := len(spriteInfo.GotSprites)
					var floatNum float64
					floatNum = float64(spriteNum)

					spriteNumKey = spriteNumRedisKey + friend

					_, err = client.Set(spriteNumKey, spriteNum, 96400*10000000000).Result()
					if err != nil {
						fmt.Println(err)
					}

					result, err := client.ZAdd(rankKey, redis.Z{
						Score:  floatNum,
						Member: friend,
					}).Result()
					if err != nil {
						fmt.Println(err)
					}
					fmt.Println(result)

				}
			}
		}
		forever <- true
	}()

	<-forever
	end := time.Now()
	endTime := end.Format("2006-01-02 15:04:05")
	fmt.Println("开始时间：", nowTime)
	fmt.Println("结束时间", endTime)
	fmt.Println("目标计算总数人数", num)
	fmt.Println("除去机器人的总人数", realNum)
}
