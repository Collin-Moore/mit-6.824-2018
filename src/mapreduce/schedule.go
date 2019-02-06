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
	//
	// Part IV Notes
	// My first thought is to rework my loop to populate my processChan
	// I could add a new channel to each of the feeders that they can send
	// info back to the master on, which can be checked in that loop and added
	// to the processChan again, probably want to kill that worker feeder, too.
	// Need to figure out how to check a chan for stuff and continue if empty
	// I think the right move is to use a select statement

	processChan := make(chan int)
	workerFailChan := make(chan int)
	workerSuccessChan := make(chan int, 4) // going to try this as a buffered, so its not blocking the workers
	var wg sync.WaitGroup                  // for the handleWorker goroutines

	handleWorker := func(rpcaddr string, wg *sync.WaitGroup) {
		fmt.Println("Handler created for: ", rpcaddr)
		defer wg.Done()
		for j := range processChan {
			fmt.Printf("Starting %sJob %d on worker %s\n", phase, j, rpcaddr)
			args := DoTaskArgs{jobName, mapFiles[j], phase, j, n_other}
			result := call(rpcaddr, "Worker.DoTask", args, nil)
			if !result {
				fmt.Printf("%sJob %d failed on worker %s\n", phase, j, rpcaddr)
				workerFailChan <- j // sending failed task number
			} else {
				fmt.Printf("%sJob %d finished on worker %s\n", phase, j, rpcaddr)
			}
		}
		fmt.Println("Handler finished for: ", rpcaddr)
		return
	}

	// goroutine to add workers as they register
	go func() {
		wg.Add(1)
		defer wg.Done()
		for r := range registerChan {
			wg.Add(1)
			go handleWorker(r, &wg)
		}
		return
	}()

	// determine finished
	go func() {
		wg.Add(1)
		defer wg.Done()
		completion := make(map[int]bool)
		count := 0
		for count < ntasks {
			fmt.Println("starting success job")
			job := <-workerSuccessChan
			fmt.Println("received a success")
			_, ok := completion[job]
			if !ok {
				fmt.Printf("Job %d is complete\n", job)
				completion[job] = true
				count++
			}
		}
		fmt.Println("Closing all the channels")
		close(workerSuccessChan)
		close(processChan)
		close(workerFailChan)
		return
	}()

	// intial shedule handling
	go func() {
		wg.Add(1)
		defer wg.Done()
		for i := 0; i < ntasks; i++ {
			i := i
			fmt.Println("loading initial job: ", i)
			processChan <- i
		}
		fmt.Println("finished loading processChan")
		return
	}()

	// rescheduling failures
	go func() {
		wg.Add(1)
		defer wg.Done()
		for fail := range workerFailChan {
			fmt.Printf("job %d failed, rescheduling\n", fail)
			processChan <- fail
		}
		return
	}()

	// fmt.Printf("Loading up processChan with %d tasks\n", ntasks)

	// for i := 0; i < ntasks; i++ {
	// i := i
	// processChan <- i // could cause a bug if sharing same `i`
	// }

	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
