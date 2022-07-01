package kafka

import (
	"08logTransfer/es"
	"fmt"
	"github.com/Shopify/sarama"
)

var (
	client      sarama.SyncProducer // 声明一个全局的连接Kafka的生产者client
	logDataChan chan *es.LogData
)

// 初始化Kafka，从Kafka中取数据发给es

// Init 初始化client
func Init(addrs []string, topic string) (err error) {
	consumer, err := sarama.NewConsumer(addrs, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	fmt.Println(partitionList)
	for partition := range partitionList { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return
		}
		//defer pc.AsyncClose()
		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v\n", msg.Partition, msg.Offset, msg.Key, string(msg.Value))
				// 直接发给es
				ld := es.LogData{
					Data:  string(msg.Value),
					Topic: topic,
				}
				es.SendToESChan(&ld)
				//es.SendToES() // 函数调用函数
				// 优化一下：直接放到一个chan中
			}
		}(pc)
	}
	return
}
