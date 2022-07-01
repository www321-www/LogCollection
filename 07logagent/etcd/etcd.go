package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

// 初始化ETCD的函数

var (
	cli *clientv3.Client
)

// LogEntry 需要收集的日志的配置信息
type LogEntry struct {
	Path  string `json:"path"`  // 日志存放的路径
	Topic string `json:"topic"` // 日志要发往Kafka中的哪个topic
}

func Init(addr string, timeout time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 5 * timeout,
	})
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd failed, err: %v\n", err)
		return
	}
	return
}

// GetConf 从 ETCD 中根据key后去配置项。返回slice是因为可能同一台机器上可能有多个配置项
func GetConf(key string) (logEntryConf []*LogEntry, err error) {
	// get
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err: %v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		//fmt.Printf("%s: %s\n", ev.Key, ev.Value)
		// slice是无法直接存入etcd的，所以用json转换成字符串格式
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			fmt.Printf("unmarshal etcd value failed, err: %v\n", err)
			return
		}
	}
	return
}

func WatchConf(key string, newConfCh chan<- []*LogEntry) {
	rch := cli.Watch(context.Background(), key) // type WatchChan <-chan WatchResponse
	defer cli.Close()
	// 从通道尝试取值
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("Type: %s, key: %s, value: %s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			// 通知tailLog.tskMgr
			// 1. 先判断操作的类型，如果是删除的操作，应该传一个空的slice
			var newConf []*LogEntry
			if ev.Type != clientv3.EventTypeDelete {
				// 如果是删除操作，手动传递一个空的配置项
				err := json.Unmarshal(ev.Kv.Value, &newConf)
				if err != nil {
					fmt.Printf("unmarshal failed, err:%v\n", err)
					continue
				}
			}
			fmt.Printf("get new conf:%v\n", newConf)
			newConfCh <- newConf
		}
	}
}
