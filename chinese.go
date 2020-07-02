package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2"
)

var wg sync.WaitGroup

func main() {

	var connect string
	var QUEUENAME string
	//var PRODUCERCNT int
	//rabbitmq
	connect = "amqp://babyrbq:zpB9ciwmA9R3pNMk@172.18.14.105:5672/"
	QUEUENAME = "chinese_user_visit_log_queue"
	//PRODUCERCNT = 20
	conn, err := amqp.Dial(connect)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	//mongodb
	GlobalMgoSession, err := mgo.Dial("172.18.2.59:27017?maxPoolSize=20")
	if err != nil {
		panic(err)
	}
	defer GlobalMgoSession.Close()

	GlobalMgoSession.SetMode(mgo.Monotonic, true)

	forever := make(chan bool)

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	_, err = ch.QueueDeclare(
		QUEUENAME,
		false, //durable
		false,
		false,
		false,
		nil,
	)

	failOnError(err, "Failed to declare a queue")

	ch, err = conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	failOnError(err, "Failed to Qos")

	q, err := ch.QueueDeclare(
		QUEUENAME,
		true, //durable
		false,
		false,
		false,
		nil,
	)

	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name,
		"",
		false, //Auto Ack
		false,
		false,
		false,
		nil,
	)

	fmt.Println(1)
	if err != nil {
		fmt.Println(err)
	}

	//复用mongo连接
	session := GlobalMgoSession.Clone()
	defer session.Close()

	mongodbClient := session.DB("chinese").C("home_visit_log")

	go func() {
		for msg := range msgs {
			var redisDataMap map[string]interface{}
			err = json.Unmarshal([]byte(msg.Body), &redisDataMap)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("In %d consume a message: %s\n", 0, msg.Body)
			msg.Ack(false) //Ack
			err = mongodbClient.Insert(redisDataMap)
			if err != nil {
				fmt.Println(err)
				continue
			}

		}
		session.Close()

		fmt.Println(22)
	}()

	<-forever
}
func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s\n", msg, err)
	}
}
