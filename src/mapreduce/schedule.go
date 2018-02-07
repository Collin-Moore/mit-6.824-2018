package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
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
	var wg sync.WaitGroup
	wg.Add(ntasks)

	tasksQueue := make(chan DoTaskArgs, 5)

	// add task to queue
	go func() {
		for i := 0; i < ntasks; i++ {
			var args DoTaskArgs
			args.JobName = jobName
			args.TaskNumber = i
			args.NumOtherPhase = n_other
			switch phase {
			case mapPhase:
				args.File = mapFiles[i]
				args.Phase = mapPhase
			case reducePhase:
				args.Phase = reducePhase
			}
			tasksQueue <- args
		}
	}()

	// wait workers
	go func() {
		for {
			workerName := <-registerChan
			debug("get worker %s\n", workerName)
			// schedule tasks to workers
			go func(workerName string) {
				for {
					args := <-tasksQueue
					if !call(workerName, "Worker.DoTask", args, nil) {
						fmt.Printf("call %s(%v) failed\n", workerName, args)
						tasksQueue <- args
						time.Sleep(500 * time.Millisecond)
						continue
					}
					wg.Done()
				}
			}(workerName)
		}
	}()

	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
