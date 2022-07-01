package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

// 专门往Kafka里面写日志

type logData struct {
	topic string
	data  string
}

var (
	client      sarama.SyncProducer // 声明一个全局的连接Kafka的生产者client
	logDataChan chan *logData
)

// Init 初始化client
func Init(addrs []string, maxSize int) (err error) {
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follower都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回

	// 连接Kafka
	client, err = sarama.NewSyncProducer(addrs, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	// 初始化全局的chan
	logDataChan = make(chan *logData, maxSize)
	// 开启后台的goroutine从通道中取数据发往Kafka
	go sendToKafka()
	return
}

// SendToChan 【将消息放入到kafka的通道里面】给外部暴露的一个函数，该函数只把日志数据发送到一个内部的channel中
func SendToChan(topic, data string) {
	msg := &logData{
		topic: topic,
		data:  data,
	}
	logDataChan <- msg
}

// 【真正干活的func】真正往Kafka中发送日志的函数
func sendToKafka() {
	for {
		select {
		case ld := <-logDataChan:
			msg := &sarama.ProducerMessage{} // 必须造一个msg的结构体
			msg.Topic = ld.topic
			msg.Value = sarama.StringEncoder(ld.data)
			// 发送到Kafka
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("send msg failed, err: ", err)
				return
			}
			fmt.Printf("partition id: %d, offset: %d\n", pid, offset)
		default:
			time.Sleep(time.Millisecond * 50)
		}
	}
}
