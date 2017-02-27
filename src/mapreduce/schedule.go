package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int  // number of tasks to deal with
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

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	waitListener := new(sync.WaitGroup)
	waitListener.Add(ntasks)

	taskChan := make(chan int, ntasks)
	// put all tasks into channel, and worker can get tasks from this channel.
	for i := 0; i < ntasks; i++ {
		taskChan <- i
	}

	// This channel is used to indicate all tasks is done.
	doneChan := make(chan bool, 1)

	go func() {
		waitListener.Wait()
		doneChan <- true
	}()

	for {
		select {
		case worker := <-registerChan:
			taskNum := <-taskChan
			file := ""
			if phase == mapPhase {
				file = mapFiles[taskNum]
			}
			args := DoTaskArgs{jobName, file, phase, taskNum, n_other}
			go func() {
				var err error
				call(worker, "Worker.DoTask", args, &err)
				if err != nil {
					fmt.Println("LALALA: func err" + err.Error())
					waitListener.Done()
					// dealing with error
				} else {
					// it means a task is done by a worker, so this worker can be reused.
					fmt.Println("LOLOLO: i have done!")
					waitListener.Done()
					registerChan <- worker
				}
			}()
		case <-doneChan:
			// all task is done. Function return.
			break
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
