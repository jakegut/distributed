package mapreduce

import (
	"container/list"
	"fmt"
	"sync"
)

type WorkerInfo struct {
  address string
  available bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // for{
  //   args := &DoJobArgs{}
  //   select{
  //   case worker := <-mr.registerChannel:
  //     mr.Workers[worker] = &WorkerInfo{address:worker, available: true}
  //     mr.nAvailableWorkers += 1
  //     go func(){mr.StartWorker <- mr.Workers[worker]}()
  //   case workerStart := <-mr.StartWorker:
  //     if mr.CurrentNMap < mr.nMap || mr.CurrentNReduce < mr.nReduce {
  //       if mr.CurrentNMap < mr.nMap {
  //         args = &DoJobArgs{
  //           File: mr.file,
  //           Operation: Map,
  //           JobNumber: mr.CurrentNMap,
  //           NumOtherPhase: mr.nReduce,
  //         }
  //         mr.CurrentNMap = mr.CurrentNMap + 1
  //       } else if mr.CurrentNReduce < mr.nReduce {
  //         args = &DoJobArgs{
  //           File: mr.file,
  //           Operation: Reduce,
  //           JobNumber: mr.CurrentNReduce,
  //           NumOtherPhase: mr.nMap,
  //         }
  //         mr.CurrentNReduce = mr.CurrentNReduce + 1
  //       }
  //     } else {
  //       isDone := true
  //       for _, worker := range mr.Workers {
  //         if !worker.available{
  //           isDone = false
  //           break;
  //         }
  //       }
  //       if isDone {
  //         return mr.KillWorkers()
  //       }
  //     }
  //   case failedWorker := <-mr.FailedWorker:
  //     args = failedWorker
  //   }
  //   go func(worker *WorkerInfo, args *DoJobArgs){
  //     if args == nil {
  //       return
  //     }
  //     var reply DoJobReply
  //     worker.available = false
  //     ok := call(worker.address, "Worker.DoJob", args, &reply)
  //     worker.available = true
  //     if reply.OK && ok {
  //       mr.StartWorker <- worker
  //     }
  //   }(workerStart, args)
  // }

  var wg sync.WaitGroup

  go func(){
    for worker := range mr.registerChannel {
      mr.Workers[worker] = &WorkerInfo{address:worker, available: true}
      mr.nAvailableWorkers += 1
      mr.StartWorker <- mr.Workers[worker]
    }
  }()

  go func(){
    for worker := range mr.StartWorker {
      jobArgs := <-mr.SendArgs
      go func(w *WorkerInfo, args *DoJobArgs){
        if args == nil {
          return
        }
        var reply DoJobReply
        w.available = false
        ok := call(w.address, "Worker.DoJob", args, &reply)
        w.available = true
        if reply.OK && ok {
          wg.Done()
          mr.StartWorker <- w
        } else {
          mr.SendArgs <- args
        }
      }(worker, jobArgs)
    }
  }()
  
  wg.Add(mr.nMap + mr.nReduce)

  for currentMap := 0; currentMap < mr.nMap; currentMap += 1 {
    args := &DoJobArgs{
      File: mr.file,
      Operation: Map,
      JobNumber: currentMap,
      NumOtherPhase: mr.nReduce,
    }
    mr.SendArgs <- args
  }

  for currentReduce := 0; currentReduce < mr.nReduce; currentReduce += 1 {
    args := &DoJobArgs{
      File: mr.file,
      Operation: Reduce,
      JobNumber: currentReduce,
      NumOtherPhase: mr.nMap,
    }
    mr.SendArgs <- args
  }

  // for{
  //   isDone := true
  //   for _, worker := range mr.Workers {
  //     if !worker.available {
  //       isDone = false
  //       break
  //     }
  //   }
  //   if isDone {
  //     break
  //   }
  // }
  
  wg.Wait()
  return mr.KillWorkers()
}
