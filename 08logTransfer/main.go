package main

import (
	"08logTransfer/conf"
	"08logTransfer/es"
	"08logTransfer/kafka"
	"fmt"
	"gopkg.in/ini.v1"
)

// logTransfer
// 将日志数据从Kafka中取出来发往es

func main() {
	// 0 加载配置文件
	var cfg conf.LogTransferCfg
	err := ini.MapTo(&cfg, "./conf/cfg.ini")
	if err != nil {
		fmt.Printf("init config, err:%v\n", err)
		return
	}
	fmt.Printf("cfg:%v\n", cfg)
	// 1 初始化【因为Kafka的Init中需要启动goroutine往es中写数据，需要es先初始化，所以先初始化es，后初始化Kafka】
	// 1.1 初始化es
	// 1.1.1 初始化一个es连接的client
	// 1.1.2 对外提供一个往es写数据的一个函数
	err = es.Init(cfg.ESCfg.Address, cfg.ChanSize)
	if err != nil {
		fmt.Printf("init es client failed, err:%v\n", err)
		return
	}
	// 1.2 初始化Kafka
	// 1.2.1 连接Kafka，创建分区的消费者
	// 1.2.2 每个分区的消费者分别取出数据通过SendToES发往es
	err = kafka.Init([]string{cfg.KafkaCfg.Address}, cfg.KafkaCfg.Topic)
	if err != nil {
		fmt.Printf("init kafka consumer failed, err:%v\n", err)
		return
	}

	// 2 从Kafka取日志数据

	// 3 发往es
}
