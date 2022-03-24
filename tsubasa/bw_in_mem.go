package tsubasa

import (
  "fmt"
  "time"
  "runtime"
  "github.com/cheggaaa/pb/v3"
)

var pairWindowsList [][]BasicWindowResult

/* DoPart for TSUBASA sketch */
func doPartBWSketchInMem(sem_1 chan int, taskNum int, listOfPairs *([][]Pair), granularity int) {

  pairs := (*listOfPairs)[taskNum]
  lengthOfPairs := len(pairs)
  pairWindowsList[taskNum] = make([]BasicWindowResult, lengthOfPairs)
  for i := 0; i < lengthOfPairs; i += 1 {
    pair := pairs[i]
    var bwr BasicWindowResult
    getBasicWindowResult(&dataMap, granularity, &pair, &bwr, nil, false, 0)
    pairWindowsList[taskNum][i] = bwr  
  }
  // Signal that the part is done
  sem_1 <- 1
}

/* DoAll for TSUBASA sketch */
func doAllBWSketchInMem(NCPU int, granularity int) {
	partitionsNum := NCPU
  sem_1 := make(chan int, partitionsNum)
  listOfPairs := make([][]Pair, partitionsNum)
  partitionData(partitionsNum, &dataMap, &listOfPairs)
  // doPart
  fmt.Println("Starting to sketch: ")
	bar := pb.StartNew(partitionsNum)
  for i := 0; i < partitionsNum; i += 1 {
    go doPartBWSketchInMem(sem_1, i, &listOfPairs, granularity)
  }

  // Waiting for tasks to be finished
  for i := 0; i < partitionsNum; i += 1 {
  	bar.Increment()
    <-sem_1
  }
  bar.Finish()
  fmt.Println("Sketch FINISHED.")
}

/* DoPart for TSUBASA query */
func doPartBWQueryInMem(sem chan int, taskNum int, listOfPairs *([][]Pair), queryStart int, queryEnd int, thres float64) {
	pairs := (*listOfPairs)[taskNum]
  lengthOfPairs := len(pairs)
  var slicesOfMeanX []float64 = make([]float64, queryEnd - queryStart)
  var slicesOfMeanY []float64 = make([]float64, queryEnd - queryStart)
  var slicesOfSigmaX []float64 = make([]float64, queryEnd - queryStart)
  var slicesOfSigmaY []float64 = make([]float64, queryEnd - queryStart)
  var slicesOfCXY []float64 = make([]float64, queryEnd - queryStart)
  for i := 0; i < lengthOfPairs; i += 1 {
    //pair := pairs[i]
    bwr := (pairWindowsList[taskNum][i])
    slicesOfMeanX = (*(bwr.slicesOfMeanX))[queryStart:queryEnd]
    slicesOfMeanY = (*(bwr.slicesOfMeanY))[queryStart:queryEnd]
    slicesOfSigmaX = (*(bwr.slicesOfSigmaX))[queryStart:queryEnd]
    slicesOfSigmaY = (*(bwr.slicesOfSigmaY))[queryStart:queryEnd]
    slicesOfCXY = (*bwr.slicesOfCXY)[queryStart:queryEnd]
    updateMatrix(&matrix, thres, &(bwr.pair), &slicesOfMeanX, &slicesOfMeanY, &slicesOfSigmaX, &slicesOfSigmaY, &slicesOfCXY, nil, false, nil)
  }
  // Signal that the part is done
  sem <-1
}

/* DoAll for TSUBASA query */
func doAllBWQueryInMem(NCPU int, queryStart int, queryEnd int, thres float64) {
	t0 := time.Now()
  sem := make(chan int, NCPU)
  listOfPairs := make([][]Pair, NCPU)
  partitionData(NCPU, &dataMap, &listOfPairs)

  // doPart
  for i := 0; i < NCPU; i += 1 {
    go doPartBWQueryInMem(sem, i, &listOfPairs, queryStart, queryEnd, thres)
  }

  // Waiting for NCPU tasks to be finished
  for i := 0; i < NCPU; i += 1 {
    <-sem
  }
  elapsed := time.Since(t0)
  fmt.Println("Query FINISHED. Time:", elapsed)
}

func networkConstructionBWParallelSketchInMem(granularity int) string {

	InitMatrix()
	NCPU := getNumCPU()
  //fmt.Println("CPU Num: ", NCPU)
  runtime.GOMAXPROCS(NCPU)

  pairWindowsList = make([][]BasicWindowResult, NCPU)
  t0 := time.Now()
  doAllBWSketchInMem(NCPU, granularity)
  elapsed := time.Since(t0)
  return fmt.Sprintf("%v", elapsed)
}

func networkConstructionBWParallelQueryInMem(queryStart int, queryEnd int, thres float64) {
	NCPU := getNumCPU()
  //fmt.Println("CPU Num: ", NCPU)
  runtime.GOMAXPROCS(NCPU)
  doAllBWQueryInMem(NCPU, queryStart, queryEnd, thres)
}


