package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup

var mutex sync.Mutex

var secret = "4b0cfd398888c2788ce5e9cd9f65c332"
var appid = "wxd327b8c133817472"
var gameSecret = "b33149a2cc5b42d7262709862699f793"
var gameAppid = "wx4faefcaf55bd09aa"

var listKey = "wx:world:list:push"

func main() {

	var client *redis.Client
	var err error

	client = redis.NewClient(&redis.Options{
		Addr:        "r-bp1fzqk7he7rt7ot1m.redis.rds.aliyuncs.com:6379",
		Password:    "Sinyee4Redis", // no password set
		DB:          0,
		DialTimeout: 0,
		MaxRetries:  5,
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)

	now := time.Now()
	nowTime := now.Format("2006-01-02 15:04:05")
	fmt.Println(nowTime)

	for {
		num, err := client.Exists(listKey).Result()

		if err != nil {
			fmt.Println(err)
		}

		if num <= 0 {
			end := time.Now()
			endTime := end.Format("2006-01-02 15:04:05")
			time.Sleep(2 * time.Second)
			fmt.Println(nowTime)
			fmt.Println(endTime)
			continue
		}

		for i := 0; i < 1000; i++ {
			if num == 0 {
				break
			}
			wg.Add(1)
			num = num - 1
			go func() {

				mutex.Lock()
				var redisData string
				var redisDataMap map[string]interface{}
				var accessToken string
				redisData, err = client.LPop(listKey).Result()
				mutex.Unlock()

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

				accessToken = getAccessToken(appid, secret, client)

				if accessToken == "" || accessToken == "null" {
					return
				}

				if redisDataMap["type"] == 3 {
					accessToken = getAccessToken(gameAppid, gameSecret, client)
				}

				sendMessage(accessToken, redisDataMap["templateId"], redisDataMap["openId"], redisDataMap["message"])

				defer wg.Done()
			}()
		}
	}
}

func getAccessToken(appid string, secret string, client *redis.Client) string {

	var wxAccessToken string

	wxAccessToken, err := client.Get(appid).Result()

	if err != nil {
		fmt.Println(err)

		url := "https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=" + appid + "&secret=" + secret

		resp, err := http.Post(url,
			"application/x-www-form-urlencoded",
			strings.NewReader("name=cjb"))
		if err != nil {
			fmt.Println(err)
			return "null"
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "null"
		}

		var dataMap map[string]string

		json.Unmarshal([]byte(body), &dataMap)

		client.Set(appid, dataMap["access_token"], time.Hour).Result()

		wxAccessToken = dataMap["access_token"]
	}

	return wxAccessToken
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

func sendMessage(accessToken string, templateId interface{}, openId interface{}, message interface{}) {

	postUrl := "https://api.weixin.qq.com/cgi-bin/message/subscribe/send?access_token=" + accessToken

	data := make(map[string]interface{})
	data["touser"] = openId
	data["template_id"] = templateId

	data["data"] = message

	// 超时时间：5秒
	client := &http.Client{Timeout: 10 * time.Second}
	jsonStr, _ := json.Marshal(data)
	resp, err := client.Post(postUrl, "text/html", bytes.NewBuffer(jsonStr))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	result, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(result))
}
