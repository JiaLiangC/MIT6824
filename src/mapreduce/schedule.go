package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task.a
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce

	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	//这里的 w := <-registerChan ,  registerChan 是一个无缓冲的管道, 负责传递已经存在的RPC地址和新注册的RPC调用地址
	//master.go中的 forwardRegistrations  方法会启用多个线程把查到的地址通过管道传过来
	//所以一个个读取效率不高，可以开启多线程读取
	//当task数量大于worker时候，如何判断一个worker已经完成工作了，然后把task 派发给它
	// 根据call的返回值判断任务的成功和失败与否
	//doTask执行时会把数据从拿走
	//forwardRegistrations中 关键代码如下
	//i：=0
	//for i<len(workers)
	//w := mr.workers[i]
	//go func() { ch <- w }() // send without holding the lock.
	//i = i + 1
	// 启动一个线程开始读数据，每当有一个worker地址被读走就默认该worker被使用和消费了，
	//当所有地址都读完后默认worker都消费完了，就会wait直到新的worker被加进到workers中去。
	//所以考虑两种情况，当消费者成功执行完毕时需要放回地址到workers中去，以便后续的消费者使用
	//当消费者消费失败时也需要把该地址放回去，以便与后续的消费使用。
	//对goroutine 的生产者和消费者不够熟悉，对chanel个 waitgroup 不熟悉

	// 此处该实现一个生产者消费者模型, 生产者负责分发任务，消费者负责消费任务
	// 生产者分发任务ID ，任务ID都存在队列里（此处用channel 当消息队列）
	// 消费者负责拿到任务信息后启动一个worker 去完成任务。
	// 生产者等到队列为空，且所有worker都完成再退出
	//

	var p DoTaskArgs
	p.JobName = jobName
	p.Phase = phase
	p.NumOtherPhase = n_other

	var workG sync.WaitGroup
	var taskChan = make(chan int)

	//生产者线程
	go func() {

		for taskIndex := 0; taskIndex < ntasks; taskIndex++ {
			workG.Add(1)
			taskChan <- taskIndex
		}

		//等到所有任务都完成为止
		workG.Wait()
		close(taskChan)
	}()

	//消费者线程
	for taksIdx := range taskChan {
		worker := <-registerChan

		p.TaskNumber = taksIdx
		if phase == mapPhase {
			p.File = mapFiles[taksIdx]
		}

		go func(worker string, p DoTaskArgs) {
			//等待一个worker完成后给它另一个task.  sync.WaitGroup
			ok := call(worker, "Worker.DoTask", &p, nil)

			if ok {
				workG.Done()
				//成功的任务把 worker地址 重新加回 可用worker 队列
				registerChan <- worker
			} else {
				//任务失败返回时把worker 重新加回任务队列
				fmt.Printf("worker execute error", worker)
				taskChan <- p.TaskNumber
			}
		}(worker, p)

	}

}
