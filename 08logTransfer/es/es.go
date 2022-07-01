package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"strings"
	"time"
)

// 初始化es，准备接受Kafka那边发来的数据

var (
	client *elastic.Client
	ch     chan *LogData
)

type LogData struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

func Init(address string, maxSize int) (err error) {
	if !strings.HasPrefix(address, "https://") {
		address = "https://" + address
	}
	client, err = elastic.NewClient(elastic.SetURL(address))
	if err != nil {
		panic(err)
	}
	ch = make(chan *LogData, maxSize)
	go sendToES()
	return
}

func SendToESChan(msg *LogData) {
	ch <- msg
}

// SendToES 发送数据到es
func sendToES() {
	for {
		select {
		case msg := <-ch:
			// BodyJson(msg)传参必须是一个go里面的结构体类型，可以被json序列化
			put1, err := client.Index().Index(msg.Topic).OpType("ip").BodyJson(msg).Do(context.Background())
			if err != nil {
				// Handle error
				panic(err)
			}
			fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
		default:
			time.Sleep(time.Second)
		}
	}

}
