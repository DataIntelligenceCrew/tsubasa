package tsubasa

import (
  "fmt"
  "math"
  "runtime"
)

/* Direct calculation network construction */
func networkConstructionNaive(dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  sumOfConnectedPairs := 0
  var i, j int
  for i = 0; i < locationsNum; i += 1 {
    for j = i + 1; j < locationsNum; j += 1 {
      var leftLocation int = locations[i]
      var rightLocation int = locations[j]
      leftPointsSlices := (*dataMap)[leftLocation]
      rightPointsSlices := (*dataMap)[rightLocation]
      var count float64 = 0
      var sumOfX float64 = 0
      var sumOfY float64 = 0
      var sumSquaredX float64 = 0
      var sumSquaredY float64 = 0
      var sumOfXY float64 = 0
      var k int
      for k = 0; k < len(leftPointsSlices); k += 1 {
        count += 1
        sumOfX += leftPointsSlices[k].temperature
        sumOfY += rightPointsSlices[k].temperature
        sumSquaredX += leftPointsSlices[k].temperature * leftPointsSlices[k].temperature
        sumSquaredY += rightPointsSlices[k].temperature * rightPointsSlices[k].temperature
        sumOfXY += leftPointsSlices[k].temperature * rightPointsSlices[k].temperature
      }
      std := ((sumOfXY/count) - (sumOfX*sumOfY)/(count*count))/
      (math.Sqrt((sumSquaredX/count) - ((sumOfX*sumOfX)/(count*count)))*
        math.Sqrt((sumSquaredY/count) - ((sumOfY*sumOfY)/(count*count))))
      if math.Abs(std) >= thres {
        (*matrix)[i][j] = 1
        (*matrix)[j][i] = 1
        sumOfConnectedPairs += 1
      }
      realMatrix[i][j] = math.Abs(std)
      realMatrix[j][i] = math.Abs(std)
    }
  }
  fmt.Println(sumOfConnectedPairs)
}

/* DoPart for naive implementation */
func doPartNaive(sem chan int, taskNum int, listOfPairs *([][]Pair), dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  for i := 0; i < len((*listOfPairs)[taskNum]); i += 1 {
    pair := (*listOfPairs)[taskNum][i]
    leftPointsSlices := (*dataMap)[pair.leftLocation]
    rightPointsSlices := (*dataMap)[pair.rightLocation]
    var count float64 = 0
    var sumOfX float64 = 0
    var sumOfY float64 = 0
    var sumSquaredX float64 = 0
    var sumSquaredY float64 = 0
    var sumOfXY float64 = 0
    var k int
    for k = 0; k < len(leftPointsSlices); k += 1 {
      count += 1
      sumOfX += leftPointsSlices[k].temperature
      sumOfY += rightPointsSlices[k].temperature
      sumSquaredX += leftPointsSlices[k].temperature * leftPointsSlices[k].temperature
      sumSquaredY += rightPointsSlices[k].temperature * rightPointsSlices[k].temperature
      sumOfXY += leftPointsSlices[k].temperature * rightPointsSlices[k].temperature
    }
    std := ((sumOfXY/count) - (sumOfX*sumOfY)/(count*count))/
    (math.Sqrt((sumSquaredX/count) - ((sumOfX*sumOfX)/(count*count)))*
      math.Sqrt((sumSquaredY/count) - ((sumOfY*sumOfY)/(count*count))))
    if math.Abs(std) >= thres {
      (*matrix)[pair.indexOfRow][pair.indexOfCol] = 1
      (*matrix)[pair.indexOfCol][pair.indexOfRow] = 1
    }
    realMatrix[pair.indexOfRow][pair.indexOfCol] = math.Abs(std)
    realMatrix[pair.indexOfCol][pair.indexOfRow] = math.Abs(std)
  }
  // Signal that the part is done
  sem <-1
}

/* DoAll for naive implementation */
func doAllNaive(NCPU int, dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  sem := make(chan int, NCPU)

  // Separate the data map by NCPU
  // The pairs of locations locations are assigned to the list evenly
  listOfPairs := make([][]Pair, NCPU)
  partitionData(NCPU, dataMap, &listOfPairs)

  // doPart
  for i := 0; i < NCPU; i += 1 {
    go doPartNaive(sem, i, &listOfPairs, dataMap, matrix, thres)
  }

  // Waiting for NCPU tasks to be finished
  for i := 0; i < NCPU; i += 1 {
    <-sem
  }
  fmt.Println("All tasks are finished.")
}

/* Construct network for naive implemetation with parallel computing */
func networkConstructionNaiveParallel(dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  NCPU := getNumCPU()
  fmt.Println("CPU Num: ", NCPU)
  runtime.GOMAXPROCS(NCPU)
  doAllNaive(NCPU, dataMap, matrix, thres)
}