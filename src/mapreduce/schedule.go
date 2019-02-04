package mapreduce

import (
	"fmt"
	"sync"
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
	// Part III Notes
	// Rpc's need to be sent in parrallel, so I think I'll start a goroutine
	// for each worker I get off of the registerChan, so maybe check for new
	// workers in the main loop and only spawn new goroutines if a new one appears
	// may also need to lock the mapFiles or ntasks or other shared variables
	// New thought: maybe use a single buffered channel to that each worker can pull from?
	// Another new thought: I'm overthinking the concurrency part, they just want me to
	// use `go` for calling the rpc, so I don't wait on it forever, I think...
	// okay, I can read from a single channel in multiple go routines, thats good

	processChan := make(chan int)
	var wg sync.WaitGroup

	handleWorker := func(rpcaddr string) {
		defer wg.Done()
		for j := range processChan {
			args := DoTaskArgs{jobName, mapFiles[j], phase, j, n_other}
			result := call(rpcaddr, "Worker.DoTask", args, nil)
			if !result {
				break
			}
		}
	}

	for i := 0; i < ntasks; i++ {
		taskChannel <- i // could cause a bug if sharing same `i`
	}
	for r := range registerChan {
		wg.Add(1)
		go handleWorker(r)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
