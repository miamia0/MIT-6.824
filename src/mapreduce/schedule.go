package mapreduce

import(
	"fmt"
	"sync"
	"time"

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
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan 	chan string) {
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
	//avaiChan := make(chan string)
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var wg sync.WaitGroup

	for i := 0;i<ntasks;i++{
	
		var file string 
		if phase == mapPhase{
			file = mapFiles[i]
		}
		wg.Add(1)
		go func(file string,taskIndex int,nReduce int){
			taskArgs :=  DoTaskArgs{jobName,file,phase,taskIndex,n_other}
			result := false			
			for result == false{
				//fmt.Println(taskIndex)
				srv := <- registerChan
				result = call(srv, "Worker.DoTask",taskArgs,nil)
				if result {
					wg.Done()
					registerChan<-srv
					break
				}else{
					time.After(time.Second)
				}
			}
		}(file,i,nReduce)
		
	}
	wg.Wait()
	



	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
