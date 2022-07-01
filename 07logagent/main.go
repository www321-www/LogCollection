package main

import (
	"07logagent/conf"
	"07logagent/etcd"
	"07logagent/kafka"
	"07logagent/taillog"
	"07logagent/utils"
	"fmt"
	"gopkg.in/ini.v1"
	"sync"
	"time"
)

// logagent入口程序

var (
	cfg = new(conf.AppConf)
)

func main() {
	// ---------------------------------------服务器编写逻辑第零步：读取配置文件---------------------------------------------
	// 0. 加载配置文件
	//cfg, err := ini.Load("./conf/conf.ini")
	//println(cfg.Section("kafka").Key("address").String())
	//println(cfg.Section("kafka").Key("topic").String())
	//println(cfg.Section("taillog").Key("path").String())
	err := ini.MapTo(cfg, "./conf/conf.ini") // 这个路径是以main.go文件作为入口的
	if err != nil {
		fmt.Printf("load ini failed, err: %v\n", err)
	}
	// ---------------------------------------服务器编写逻辑第一步：通过读取到的配置文件内容，进行各种初始化----------------------
	// 1. 初始化Kafka连接
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.ChanMaxSize)
	if err != nil {
		fmt.Printf("init kafka failed, err: %v\n", err)
		return
	}
	println("init kafka success")
	// 2. 初始化etcd
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Printf("init etcd failed, err: %v\n", err)
		return
	}
	println("init etcd success")
	// ---------------------------------------服务器编写逻辑第二步：根据服务器的主要功能编写代码-------------------------------
	// 为了实现每个logAgent都拉取自己独有的配置，统一要以自己的IP地址作为区分
	ipStr, err := utils.GetOutBoundIP()
	if err != nil {
		panic(err)
	}
	etcdConfKey := fmt.Sprintf(cfg.EtcdConf.Key, ipStr)
	// 2.1 从etcd中获取日志收集项的配置信息
	logEntryConf, err := etcd.GetConf(etcdConfKey)
	if err != nil {
		fmt.Printf("etcd get conf failed, err: %v\n", err)
		return
	}
	fmt.Printf("etcd get conf success, %v\n", logEntryConf)
	// ？
	// 2.2 派一个哨兵去监视日志收集项的变化(有变化及时通知logAgent)
	//for index, value := range logEntryConf {
	//	fmt.Printf("index:%v, value: %v\n", index, value)
	//}
	taillog.Init(logEntryConf)           // 因为NewConfChan访问了tskMgr的newConfChan，这个chan是在taillog.Init(logEntryConf)之后才能获得的
	NewConfChan := taillog.NewConfChan() // 从taillog中获取tskMgr对外暴露的通道
	// 3. 收集日志发往Kafka

	var wg sync.WaitGroup
	wg.Add(1)
	// line55 第一次拉取配置之后，应该用etcd的watch去派一个哨兵观察配置文件的变化
	go etcd.WatchConf(etcdConfKey, NewConfChan) // 只要哨兵发现最新的配置信息会将更新的配置信息放到NewConfChan里面
	wg.Wait()
	// 当etcd的watch哨兵观察到配置项发生变化的时候，应该动态的去调整tailTask的任务
	// 1. 原来有3个配置项，现在有4个配置项。原来没有现在有了，新增一个tailTask
	// 2. 原来有配置项，现在没有了。找到对应的tailTask停掉
	// 3. 修改了配置项，有删除的配置项有新增的配置项
	// 【思路】连上etcd读取了配置信息之后，放置一个哨兵观察配置项的变化，及时返回消息
	//       有了最新的配置之后，在etcd里面收到这个消息。需要通知tailLog模块的tskMgr，在tskMgr的Init中开启了go tskMgr.run()
	//       只要chan中有更新的配置，就通过map更改

	// kafka把数据取出来写入到es里面去，es做的事情就是将文本数据对应分词进行索引。到时候可以将直接搜某个词

}
