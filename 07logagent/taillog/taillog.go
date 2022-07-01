package taillog

import (
	"07logagent/kafka"
	"fmt"
	"github.com/hpcloud/tail"
	"golang.org/x/net/context"
)

// TailTask 一个日志收集的任务
type TailTask struct {
	path     string     // 收集的哪个路径下的日志
	topic    string     // 收集的日志放在哪个topic下面
	instance *tail.Tail // 具体的tailf对象的实例
	// 为了能够实现退出line 58的t.run()异步调用采集日志的功能
	ctx    context.Context
	cancel context.CancelFunc
}

// NewTailTask TailTask的构造函数
func NewTailTask(path, topic string) (tailObj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailObj = &TailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
		// 还差instance没有实例，instance通过init方法进行初始化
	}
	tailObj.init() // 根据路径去打开对应的日志
	return
}

// 通过init对tail的文件进行初始化
func (t TailTask) init() {
	config := tail.Config{
		ReOpen:    true,                                 // 重新打开，切日志功能
		Follow:    true,                                 // 是否跟随之前未读取完的文件继续读
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件的哪个位置断点续读
		MustExist: false,                                // 如果文件不存在，也不报错
		Poll:      true,                                 //
	}
	var err error
	// TailFile 是通过上面定义的Config来打开一个文件来记录日志
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail file failed, err: ", err)
		return
	}
	// 当goroutine执行的函数退出的时候，goroutine就结束了
	go t.run() // 直接去采集日志发送到Kafka
}

func (t *TailTask) run() {
	for {
		select {
		// 如果读到了ctx的关闭监视当前日志文件的通知，就关闭当前goroutine
		case <-t.ctx.Done(): // 在t中嵌套了一个ctx，只要调用run的goroutine调用了cancel，t.ctx.Done()通道中就有值，goroutine就可以结束
			fmt.Printf("tail task:%s结束了...", t.path+t.topic)
			return
		// 如果从tail.Lines中读到了日志更新的内容，就把更新的内容写到Kafka里面去
		case line := <-t.instance.Lines: // 从tailObj的通道中一行一行的读取日志数据
			// 3.2 发往Kafka
			//kafka.SendToKafka(t.topic, line.Text) // 函数调函数，函数每次等Kafka发送成功后
			// 如何变成异步？
			kafka.SendToChan(t.topic, line.Text)
			// 先把日志数据放到通道中，在Kafka那个包中有单独的goroutine去取日志发送到Kafka
		}
	}
}
