package taillog

import (
	"07logagent/etcd"
	"fmt"
	"time"
)

var tskMgr *tailLogMgr

// tailTask 管理者。统一管理所有的tailObj，每一个tailObj是一个配置文件的路径
type tailLogMgr struct {
	logEntry    []*etcd.LogEntry      // 收集日志的logEntry，如果来了新的logEntry要进行比较
	tskMap      map[string]*TailTask  // 有多少个TailTask要记下来，方便停止
	newConfChan chan []*etcd.LogEntry // 等待最新的通知来的通道
}

// Init 定义了一个全局的管理者，这个管理者把当前日志收集项管理信息存储起来了
func Init(logEntryConf []*etcd.LogEntry) {
	tskMgr = &tailLogMgr{
		logEntry:    logEntryConf, // 把当前的日志收集项配置信息保存起来
		tskMap:      make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry), // 无缓冲区通道
	}

	// 遍历配置项，有一个配置，就起一个NewTailTask去完成
	for _, logEntry := range logEntryConf {
		// conf: *etcd.LogEntry
		// logEntry.Path: 要收集的日志文件的路径
		// 初始化的时候起了多少个tailTask都要记下来，为了方便后续判断
		tailObj := NewTailTask(logEntry.Path, logEntry.Topic)
		// 同一台机器上不同的收集项哪里不一样？？？path不一样，topic可以一样。map中用path和topic拼接的结果来进行唯一性的区分
		mk := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		tskMgr.tskMap[mk] = tailObj
	}
	go tskMgr.run()
}

// 监听自己的 newConfChan ，有了新的配置过来之后就做对应的处理
// 1.配置新增
// 2.配置删除
// 3.配置更改
func (t *tailLogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			// 需要遍历chan中的每一个新配置，因为etcd中设置的配置永远设置全量更新，需要遍历列表中的每一个具体配置项检查
			for _, conf := range newConf {
				mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				// 1 检查是不是新增的
				_, ok := t.tskMap[mk]
				if ok {
					// 1.1 原来就有，无需操作
					continue
				} else {
					// 1.2 原来没有，需要新增
					tailObj := NewTailTask(conf.Path, conf.Topic)
					t.tskMap[mk] = tailObj
				}
			}
			// 找出原来t.logEntry有，但是newConf中没有的，要删掉
			for _, c1 := range t.logEntry { // 从原来的配置中依次拿出配置项
				isDelete := true
				for _, c2 := range newConf { // 去新的配置中逐一进行比较
					if c1.Path == c2.Path && c1.Topic == c2.Topic {
						isDelete = false // 如果有一次匹配到了，说明在新配置中已经存在，不需要删除
						break
					}
				}
				if isDelete {
					// 把c1对应的这个tailObj给停掉
					mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					//t.tskMap[mk] --> tailObj
					t.tskMap[mk].cancel()
				}
			}
			// 2 配置删除

			// 3 配置变更
			fmt.Println("新的配置来了：", newConf)
		default:
			time.Sleep(time.Second)
		}
	}
}

// NewConfChan 一个函数，向外暴露tsgMgr的newConfChan
func NewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
