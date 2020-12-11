package Request

import (
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
	"log"
	"strconv"
	"time"
)

const replyTopic = "replyTopic"

var resqmap map[string]chan string = make(map[string]chan string)

type RequestReplyData struct {
	ReplyID string
	Data    interface{}
}

func newRequestReplyData(data interface{}) RequestReplyData {
	var partionstr string
	if request_partion_num < 10 {
		partionstr = "0" + strconv.Itoa(request_partion_num)
	}
	redata := RequestReplyData{Data: data, ReplyID: partionstr + uuid.NewV1().String()}
	return redata
}

func Request(topic string, data interface{}, timeout uint16) (string, error) {
	requestdata := newRequestReplyData(data)
	datajson, err := json.Marshal(requestdata)
	if err != nil {
		log.Fatal(err)
	}
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(datajson)}
	_, _, err = request_productor.SendMessage(msg)

	if err != nil {
		log.Fatal(err)
	}
	boxchan := make(chan string)
	resqmap[requestdata.ReplyID] = boxchan
	select {
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		log.Fatal("超时")
		return "", errors.New("超时")
	case msg := <-boxchan:
		println("接收到消息")
		close(boxchan)                       //关闭channel
		delete(resqmap, requestdata.ReplyID) //清理map
		return msg, nil
	}

}

var request_productor sarama.SyncProducer
var request_partion_num int

func SetRequest(pro sarama.SyncProducer, partionNum int, addr string) {
	request_productor = pro
	if partionNum > 99 {
		log.Panic("分区序号超出两位数")
		return
	}
	request_partion_num = partionNum

	//实例化一个全局消费者
	master, err := sarama.NewConsumer([]string{addr}, nil)
	if err != nil {
		log.Panic(err)
	}
	go consumerHandle(master)

}
func consumerHandle(c sarama.Consumer) {
	consumer, err := c.ConsumePartition(replyTopic, int32(request_partion_num), -1)
	if err != nil {
		log.Panic(err)
	}
	for {
		select {
		case message := <-consumer.Messages():
			//log.Println("Topic:" + message.Topic)
			replyid := string(message.Key)
			//log.Println("Key:" + replyid)
			//log.Println("Value:" + string(message.Value))

			ch := resqmap[replyid]
			ch <- string(message.Value)
		case err := <-consumer.Errors():
			log.Fatal(err)
		}
	}
}
