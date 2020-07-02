package main

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql" //
)

var redisKey = "chinese:all:userId:set"

func main() {

	db, err := sql.Open("mysql", "babybus:0BPIMBoOqNejxFRY@tcp(rm-bp1m270lb22902f47.mysql.rds.aliyuncs.com:3306)/chinese?charset=utf8")
	if err != nil {
		fmt.Println(err)
	}

	// 最大连接周期
	db.SetConnMaxLifetime(100 * time.Second)

	defer db.Close()

	client := redis.NewClient(&redis.Options{
		Addr:        "r-bp1fzqk7he7rt7ot1m.redis.rds.aliyuncs.com:6379",
		Password:    "Sinyee4Redis", // no password set
		DB:          3,              // use default DB
		DialTimeout: 0,
		MaxRetries:  5,
	})

	rows := db.QueryRow("select count(*) as total from chinese_baby limit ?", 1)
	total := 0
	rows.Scan(&total)
	fmt.Println(total)

	forever := make(chan bool)

	userChan := make(chan string)

	go func() {
		for i := 0; i < (total/1000)+1; i++ {

			rows, err := db.Query("select id from chinese_baby limit ?,?", i*1000, 1000)

			if err != nil {
				fmt.Println(1)
				continue
			}

			userId := ""

			for rows.Next() {
				rows.Scan(&userId)

				userChan <- userId

			}
		}
		forever <- true
	}()

	go func() {

		for userId := range userChan {

			fmt.Println(userId)
			_, err := client.SAdd(redisKey, userId).Result()
			if err != nil {
				fmt.Println(err)
			}
		}

	}()
	<-forever
}
