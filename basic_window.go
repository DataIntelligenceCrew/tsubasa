package tsubasa

import (
  "fmt"
  "time"
  "runtime"
  "strings"
)

/* DoPart for TSUBASA sketch */
func doPartBWSketch(endChan chan int, dataChan chan DataOfChannel, taskNum int, listOfPairs *([][]Pair), dataMap *(map[int][]Point), 
  granularity int, writeBlockSize int, header string, isDFT bool, ratio float64, durations *([]string)) {
  t0 := time.Now()

  // Open db
  dbName := fmt.Sprintf("%s", dbname)
  db := openDB(&dbName) // Open and get the database

  var accumulate int = 0
  var tableName string
  if !isDFT {
    tableName = fmt.Sprintf("%s_%d", tablename, taskNum)
  } else {
    tableName = fmt.Sprintf("%s_%d", tablenamedft, taskNum)
  }
  blockInsertionSQLStarter := fmt.Sprintf("INSERT INTO %s %s VALUES ", tableName, header)
  var statementSB strings.Builder
  statementSB.WriteString(blockInsertionSQLStarter)

  pairs := (*listOfPairs)[taskNum]
  lengthOfPairs := len(pairs)

  for i := 0; i < lengthOfPairs; i += 1 {
    pair := pairs[i]
    var bwr BasicWindowResult
    var bwrdft BasicWindowDFTResult
    //getBasicWindowResult(dataMap, granularity, &pair, &bwr)
    if !isDFT {
      getBasicWindowResult(dataMap, granularity, &pair, &bwr, nil, false, 0)
    } else {
      getBasicWindowResult(dataMap, granularity, &pair, nil, &bwrdft, true, ratio)
    }

    if writeBlockSize <= 0 {
      if !isDFT {
        insertRowBWR(db, &bwr, i, tableName) // i is id
      } else {
        insertRowBWRDFT(db, &bwrdft, i, tableName) // i is id
      }
    } else {
      // Accumulate
      if accumulate > 0 {
        statementSB.WriteString(",")
      }
      if !isDFT {
          appendRowBWR(&statementSB, &bwr, i) // i is id
        } else {
          appendRowBWRDFT(&statementSB, &bwrdft, i) // i is id
        }
      accumulate += 1
      if accumulate == writeBlockSize {
        // Insert rows
        statementSB.WriteString(";")
        // Not do insertion, but add string to channel
        dataChan <- DataOfChannel{statementSB.String()}
        //insertRowsBWR(db, &statementSB)

        // Reset values
        accumulate = 0
        statementSB.Reset()
        statementSB.WriteString(blockInsertionSQLStarter)
      }
    }
  }
  if writeBlockSize > 0 && accumulate > 0 {
    // Insert remained rows
    statementSB.WriteString(";")
    // Not do insertion, but add string to channel
    dataChan <- DataOfChannel{statementSB.String()}
    //insertRowsBWR(db, &statementSB)
  }

  // close db
  closeDB(db)

  elapsed := time.Since(t0)
  (*durations)[taskNum] = fmt.Sprintf("%v", elapsed)

  //fmt.Println("Time: ", elapsed)

  // Signal that the part is done
  endChan <-1
}

/* writer worker */
func writeDBFromChan(partitionsNum int, dataChan chan DataOfChannel, sem_2 chan int, batchesNum int) {
  dbName := fmt.Sprintf("%s", dbname)
  db := openDB(&dbName) // Open and get the database

  t0 := time.Now()
  for i := 0; i < batchesNum; i += 1 {
    data := <- dataChan
    insertRowsBWRString(db, &(data.statement))
  }
  elapsed := time.Since(t0)
  fmt.Println("Time for writing data: ", elapsed)

  closeDB(db)

  sem_2 <- 1
}

/* DoAll for TSUBASA sketch */
func doAllBWSketch(partitionsNum int, dataMap *(map[int][]Point), listOfPairs *([][]Pair),
  granularity int, writeBlockSize int, header string, isDFT bool, ratio float64, durations *([]string)) {

  sem_1 := make(chan int, partitionsNum) // To signal parts are finsihed
  sem_2 := make(chan int, 1)             // To signal writing is finished

  // Compute the number of data batches
  batchesNum := getBatchesNum(partitionsNum, listOfPairs, writeBlockSize)

  dataChan := make(chan DataOfChannel, batchesNum)

  // doPart
  for i := 0; i < partitionsNum; i += 1 {
    go doPartBWSketch(sem_1, dataChan, i, listOfPairs, dataMap, granularity, writeBlockSize, header, isDFT, ratio, durations)
  }

  // writer worker
  go writeDBFromChan(partitionsNum, dataChan, sem_2, batchesNum)

  // Waiting for tasks to be finished
  for i := 0; i < partitionsNum; i += 1 {
    <-sem_1
  }

  // Waiting for writing to be finished
  for i := 0; i < 1; i += 1 {
    <-sem_2
  }

  fmt.Println("All tasks for sketching are finished.")
}

/* DoPart for TSUBASA query */
func doPartBWQuery(sem chan int, taskNum int, listOfPairs *([][]Pair),
  matrix *([][]int), thres float64, tableName string, readBlockSize int, numberOfBasicwindows int, isDFT bool, 
  queryStart int, queryEnd int, durations *([]string), readsTime *([]float64)) {
  t0 := time.Now()

  // Open db
  dbName := fmt.Sprintf("%s", dbname)
  db := openDB(&dbName) // Open and get the database

  var totalCnt int = len((*listOfPairs)[taskNum])
  var readTime float64 = 0
  // Read by blocks
  startID := 0
  endID := 0
  for startID < totalCnt {
    if startID + readBlockSize > totalCnt {
      endID = totalCnt
    } else {
      endID = startID + readBlockSize
    }
    readTimeStr := queryRowsDB(db, tableName, startID, endID, matrix, thres, numberOfBasicwindows, isDFT, queryStart, queryEnd)
    //fmt.Println("read: ", readTimeStr)
    readTime += stringToSeconds(readTimeStr)
    startID = endID
  }

  // close db
  closeDB(db)

  elapsed := time.Since(t0)
  (*durations)[taskNum] = fmt.Sprintf("%v", elapsed)
  (*readsTime)[taskNum] = readTime

  // Signal that the part is done
  sem <-1
}

/* DoAll for TSUBASA query */
func doAllBWQuery(NCPU int, dataMap *(map[int][]Point), listOfPairs *([][]Pair),
  matrix *([][]int), thres float64, readBlockSize int, numberOfBasicwindows int, isDFT bool, 
  queryStart int, queryEnd int, durations *([]string), readsTime *([]float64)) {
  sem := make(chan int, NCPU)
  // doPart
  for i := 0; i < NCPU; i += 1 {
    tableName := fmt.Sprintf("%s_%d", tablename, i)
    if isDFT {
      tableName = fmt.Sprintf("%s_%d", tablenamedft, i)
    }
    go doPartBWQuery(sem, i, listOfPairs, matrix, thres, tableName, readBlockSize, numberOfBasicwindows, isDFT, queryStart, queryEnd, durations, readsTime)
  }
  // Waiting for NCPU tasks to be finished
  for i := 0; i < NCPU; i += 1 {
    <-sem
  }
  fmt.Println("All tasks for querying are finished.")
}

/* Construct network for TSUBASA with parallel computing */
func networkConstructionBWParallel(dataMap *(map[int][]Point), matrix *([][]int), thres float64, granularity int, 
  writeBlockSize int, readBlockSize int, isDFT bool, ratio float64, 
  queryStart int, queryEnd int, sketchDurations *([]string), queryDurations *([]string), queryReadTime *([]float64)) {
  NCPU := getNumCPU()
  fmt.Println("CPU Num: ", NCPU)
  partitionsNum := NCPU - 1
  fmt.Println("Partions Num: ", partitionsNum)
  runtime.GOMAXPROCS(NCPU)

  // Create a new database
  dbName := fmt.Sprintf("%s", dbname)
  createNewDB(dbName)
  db := openDB(&dbName) // Open and get the database

  // Create partitionsNum tables
  for i := 0; i < partitionsNum; i += 1 {
    if !isDFT {
      tableName := fmt.Sprintf("%s_%d", tablename, i)
      createTable(db, tableName, pairsbwrschema) // Create a new table for mapping pairs to basic window statistics
    } else {
      tableName := fmt.Sprintf("%s_%d", tablenamedft, i)
      createTable(db, tableName, pairsbwrdftschema) // Create a new table for mapping pairs to basic window statistics
    }
  }

  // Close db before parallel
  closeDB(db)

  //sizeBeforeSketch := getSizeOfDB(dbName)

  var numberOfBasicwindows int = getNumberOfBasicwindows(dataMap, granularity)
  listOfPairs := make([][]Pair, partitionsNum)
  partitionData(partitionsNum, dataMap, &listOfPairs)

  t0 := time.Now()
  header := pairsbwrheader
  if isDFT {
    header = pairsbwrdftheader
  }
  doAllBWSketch(partitionsNum, dataMap, &listOfPairs, granularity, writeBlockSize, header, isDFT, ratio, sketchDurations)
  elapsed := time.Since(t0)
  fmt.Println("Sketch time: ", elapsed)

  // Check queryStart and queryEnd
  if queryEnd >= 0 {
      if queryEnd - queryStart > numberOfBasicwindows {
      panic("ERROR: queryEnd - queryStart > numberOfBasicwindows")
    }
  }

  //sizeAfterSketch := getSizeOfDB(dbName)
  //fmt.Println(fmt.Sprintf("sizeBeforeSketch: %d bytes, sizeAfterSketch: %d bytes, size: %d bytes", sizeBeforeSketch, sizeAfterSketch, sizeAfterSketch - sizeBeforeSketch))

  t1 := time.Now()
  doAllBWQuery(partitionsNum, dataMap, &listOfPairs, matrix, thres, readBlockSize, numberOfBasicwindows, isDFT, queryStart, queryEnd, queryDurations, queryReadTime)
  elapsed = time.Since(t1)
  fmt.Println("Query time: ", elapsed)

  db = openDB(&dbName) 
  // Delete tables
  for i := 0; i < partitionsNum; i += 1 {
    if !isDFT {
      tableName := fmt.Sprintf("%s_%d", tablename, i)
      deleteTable(db, tableName)
    } else {
      tableName := fmt.Sprintf("%s_%d", tablenamedft, i)
      deleteTable(db, tableName)
    }
  }

  closeDB(db) // Close the database
  deleteDB(dbName) // Delete the database
}

func networkConstructionBWParallelSketch(dataMap *(map[int][]Point), granularity int, 
  writeBlockSize int, isDFT bool, ratio float64, sketchDurations *([]string)) {

  NCPU := getNumCPU()
  //fmt.Println("CPU Num: ", NCPU)
  partitionsNum := NCPU - 1
  //fmt.Println("Partions Num: ", partitionsNum)
  runtime.GOMAXPROCS(NCPU)

  // Create a new database
  dbName := fmt.Sprintf("%s", dbname)
  createNewDB(dbName)
  db := openDB(&dbName) // Open and get the database

  // Create partitionsNum tables
  for i := 0; i < partitionsNum; i += 1 {
    if !isDFT {
      tableName := fmt.Sprintf("%s_%d", tablename, i)
      createTable(db, tableName, pairsbwrschema) // Create a new table for mapping pairs to basic window statistics
    } else {
      tableName := fmt.Sprintf("%s_%d", tablenamedft, i)
      createTable(db, tableName, pairsbwrdftschema) // Create a new table for mapping pairs to basic window statistics
    }
  }

  // Close db before parallel
  closeDB(db)

  listOfPairs := make([][]Pair, partitionsNum)
  partitionData(partitionsNum, dataMap, &listOfPairs)

  t0 := time.Now()
  header := pairsbwrheader
  if isDFT {
    header = pairsbwrdftheader
  }
  doAllBWSketch(partitionsNum, dataMap, &listOfPairs, granularity, writeBlockSize, header, isDFT, ratio, sketchDurations)
  elapsed := time.Since(t0)
  fmt.Println("Sketch time: ", elapsed)
}

func networkConstructionBWParallelQuery(dataMap *(map[int][]Point), matrix *([][]int), thres float64, granularity int, 
	readBlockSize int, isDFT bool, queryStart int, queryEnd int, queryDurations *([]string), queryReadTime *([]float64)) {

  NCPU := getNumCPU()
  //fmt.Println("CPU Num: ", NCPU)
  partitionsNum := NCPU - 1
  //fmt.Println("Partions Num: ", partitionsNum)
  runtime.GOMAXPROCS(NCPU)

  var numberOfBasicwindows int = getNumberOfBasicwindows(dataMap, granularity)
  listOfPairs := make([][]Pair, partitionsNum)
  partitionData(partitionsNum, dataMap, &listOfPairs)

  // Check queryStart and queryEnd
  if queryEnd >= 0 {
      if queryEnd - queryStart > numberOfBasicwindows {
      panic("ERROR: queryEnd - queryStart > numberOfBasicwindows")
    }
  }

  t1 := time.Now()
  doAllBWQuery(partitionsNum, dataMap, &listOfPairs, matrix, thres, readBlockSize, numberOfBasicwindows, isDFT, queryStart, queryEnd, queryDurations, queryReadTime)
  elapsed := time.Since(t1)
  fmt.Println("Query time: ", elapsed)
}

func DeleteSkecth(isDFT bool) {
	NCPU := getNumCPU()
  //fmt.Println("CPU Num: ", NCPU)
  partitionsNum := NCPU - 1
  //fmt.Println("Partions Num: ", partitionsNum)
  runtime.GOMAXPROCS(NCPU)

	dbName := fmt.Sprintf("%s", dbname)
	db := openDB(&dbName) 
  // Delete tables
  for i := 0; i < partitionsNum; i += 1 {
    if !isDFT {
      tableName := fmt.Sprintf("%s_%d", tablename, i)
      deleteTable(db, tableName)
    } else {
      tableName := fmt.Sprintf("%s_%d", tablenamedft, i)
      deleteTable(db, tableName)
    }
  }

  closeDB(db) // Close the database
  deleteDB(dbName) // Delete the database
}
