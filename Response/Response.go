package Response

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"log"
	"strconv"
)

const replyTopic = "replyTopic"

var ResPro sarama.SyncProducer //响应者专用生产者
type RequestReplyData struct {
	ReplyID string
	Data    interface{}
}

func (d RequestReplyData) Response(data interface{}) error {
	datajson, err := json.Marshal(data)

	if err != nil {
		log.Fatal(err)
	}
	partionnum, err1 := strconv.Atoi(d.ReplyID[:2])

	if err1 != nil {
		log.Fatal(err1)
	}
	msg := &sarama.ProducerMessage{Topic: replyTopic, Partition: int32(partionnum), Key: sarama.StringEncoder(d.ReplyID), Value: sarama.ByteEncoder(datajson)}
	_, _, err = ResPro.SendMessage(msg)
	return err

}

func InitResponse(addr string) { //实例化一个生产者
	var err error
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner
	ResPro, err = sarama.NewSyncProducer([]string{addr}, config)
	if err != nil {
		log.Fatal(err)
	}
}
