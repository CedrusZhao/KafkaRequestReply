package example

import (
	"github.com/CedrusZhao/KafkaRequestReply/Request"
	"github.com/Shopify/sarama"
	"log"
)

func TestRequest() {
	producer, err := sarama.NewSyncProducer([]string{"192.168.43.56:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	Request.SetRequest(producer, 0, "192.168.43.56:9092")   //使用前先设置，需要给一个生产者实例，以及kafka broker地址
	msg, err := Request.Request("my_topic", "hello", 10000) //发送请求
	println(msg)
}
