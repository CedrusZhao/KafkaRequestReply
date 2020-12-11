package example

import (
	"encoding/json"
	"github.com/CedrusZhao/KafkaRequestReply/Response"
	"github.com/Shopify/sarama"
	"log"
)

func TestResponse() {
	Response.InitResponse("192.168.43.56:9092") //初始化响应者
	master, err := sarama.NewConsumer([]string{"192.168.43.56:9092"}, nil)
	if err != nil {
		log.Panic(err)
	}

	consumer, err := master.ConsumePartition("my_topic", 0, -1) //监听自己感兴趣的主题
	for {
		select {
		case message := <-consumer.Messages():
			log.Println("Topic:" + message.Topic)
			log.Println("Key:" + string(message.Key))
			log.Println("Value:" + string(message.Value))
			reData := Response.RequestReplyData{}
			err1 := json.Unmarshal(message.Value, &reData)
			if err1 != nil {
				log.Fatal(err1)
			}
			reData.Response("mysql") //回应请求者
		case err := <-consumer.Errors():
			log.Fatal(err)
		}
	}
}
