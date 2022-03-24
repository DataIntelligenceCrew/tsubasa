package tsubasa

import (
  "runtime"
  "fmt"
  "time"
  "strings"
  "strconv"
  "math"
  "math/cmplx"
  "database/sql"
  _ "github.com/lib/pq"
)

// Global variables
var user string = ""
var password string = ""
var dataMap map[int][]Point
var matrix [][]int
var realMatrix [][]float64
var basicWindowSize int

const (
  // const vars for db
  host              = "127.0.0.1"
  port              = 5432
  dbname            = "climatedb"
  tablename         = "pairsbwr"
  tablenamedft      = "pairsbwrdft"
  pairsbwrschema    = "id INT UNIQUE NOT NULL, pair VARCHAR(30) UNIQUE NOT NULL, meanx VARCHAR(10000), meany VARCHAR(10000), sigmax VARCHAR(10000), sigmay VARCHAR(10000), cxy VARCHAR(10000)"
  pairsbwrheader    = "(id, pair, meanx, meany, sigmax, sigmay, cxy)"
  pairsbwrdftschema = "id INT UNIQUE NOT NULL, pair VARCHAR(30) UNIQUE NOT NULL, meanx VARCHAR(10000), meany VARCHAR(10000), sigmax VARCHAR(10000), sigmay VARCHAR(10000), dxy VARCHAR(10000)"
  pairsbwrdftheader = "(id, pair, meanx, meany, sigmax, sigmay, dxy)"
)

type Pair struct {
  leftLocation int    // location of left stream
  rightLocation int   // location of right stream
  indexOfRow int      // row index in matrix
  indexOfCol int      // column index in matrix
}

/* Struct for data point */
type Point struct {
  timestamp float64
  latitude float32
  longitude float32
  location int
  temperature float64
}

/* Struct to store basic window statistics */
type BasicWindowResult struct {
  pair Pair
  slicesOfMeanX *([]float64)
  slicesOfMeanY *([]float64)
  slicesOfSigmaX *([]float64)
  slicesOfSigmaY *([]float64)
  slicesOfCXY *([]float64)
}

/* Struct to store basic window dft statistics */
type BasicWindowDFTResult struct {
  pair Pair
  slicesOfMeanX *([]float64)
  slicesOfMeanY *([]float64)
  slicesOfSigmaX *([]float64)
  slicesOfSigmaY *([]float64)
  slicesOfDXY *([]float64)
  // For updates
  slicesOfSumSquaredX *([]float64)
  slicesOfSumSquaredY *([]float64)
}

/* Struct for insertion to db, unique to each other */
type SerializedPair struct {
  value string
}

/* Serialized BasicWindowResult */
type RowBWR struct {
  pair SerializedPair // leftLocation,rightLocation,indexOfRow,indexOfCol
  meanX string        // mean_x_1,mean_x_2,mean_x_3...
  meanY string        // mean_y_1,mean_y_2,mean_y_3...
  sigmaX string       // sigma_x_1,sigma_x_2,sigma_x_3...
  sigmaY string       // sigma_y_1,sigma_y_2,sigma_y_3...
  cXY string          // cxy_1,cxy_2,cxy_3...
}

/* Serialized BasicWindowDFTResult */
type RowBWRDFT struct {
  pair SerializedPair // leftLocation,rightLocation,indexOfRow,indexOfCol
  meanX string        // mean_x_1,mean_x_2,mean_x_3...
  meanY string        // mean_y_1,mean_y_2,mean_y_3...
  sigmaX string       // sigma_x_1,sigma_x_2,sigma_x_3...
  sigmaY string       // sigma_y_1,sigma_y_2,sigma_y_3...
  dXY string          // dxy_1,dxy_2,dxy_3...
}

/* Data stored in channel */
type DataOfChannel struct {
  statement string
}

/* --- Functions related to database operations --- */
/* Exec handler */
func execDB(db *sql.DB, sqlStatementPtr *string) {
  _, err := db.Exec(*sqlStatementPtr)
  if err != nil {
    panic(err)
  }
}

/* Open db */
func openDB(dbNamePtr *string) *sql.DB {
  var psqlInfo string
  if (dbNamePtr == nil) {
    psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s sslmode=disable",
    host, port, user, password)
  } else {
    psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s dbname=%s sslmode=disable",
    host, port, user, password, *dbNamePtr)
  }
  // Open a connection, 1st arg: server name, 2nd arg: connection string
  db, err := sql.Open("postgres", psqlInfo)
  if err != nil {
    panic(err)
  }
  // Check whether or not the connection string was 100% correct
  err = db.Ping()
  if err != nil {
    panic(err)
  }
  //fmt.Println("Successfully connected!")
  return db
}

/* Close db */
func closeDB(db *sql.DB) {
  db.Close()
}

/* Get size of db */
func getSizeOfDB(dbName string) int {
  // Get db size
  db := openDB(&dbName)
  st := fmt.Sprintf("select pg_database_size('%s');", dbName)
  rows, err := db.Query(st)
  if err != nil {
    panic(err)
  }
  var sizeStr string
  for rows.Next() {
    rows.Scan(&sizeStr)
    fmt.Println("db size: ", sizeStr)
  }
  closeDB(db) // Close the database
  intVal, _ := strconv.Atoi(sizeStr)
  return intVal
}

/* Create a new database in postgreSQL */
func createNewDB(dbName string) {
  db := openDB(nil)
  // Create a new table
  sqlStatement := "CREATE DATABASE " + dbName + ";"
  execDB(db, &sqlStatement)
  //fmt.Println("DATABASE CREATED: ", dbName)
  closeDB(db)
  //fmt.Println("Closed!")
}

/* Delete the database (dbname) when it is closed */
func deleteDB(dbName string) {
  db := openDB(nil)
  defer closeDB(db)
  // Delete the table
  sqlStatement := "DROP DATABASE " + dbName + ";"
  execDB(db, &sqlStatement)
  //fmt.Println("DATABASE DELETED: ", dbName)
  //fmt.Println("Successfully deleted " + dbName + "!")
}

/* Create a table with schema in the specific database */
func createTable(db *sql.DB, tableName string, schema string) {
  sqlStatement := fmt.Sprintf("CREATE TABLE %s (%s);", tableName, schema)
  execDB(db, &sqlStatement)
  //fmt.Println("TABLE CREATED: ", tableName)
}

/* Delete a table in the database */
func deleteTable(db *sql.DB, tableName string) {
  sqlStatement := "DROP TABLE " + tableName + ";"
  execDB(db, &sqlStatement)
  //fmt.Println("TABLE DELETED: ", tableName)
}

/* Insert one row (basic window result) to db */
func insertRowBWR(db *sql.DB, bwr *BasicWindowResult, id int, tableName string) {
  rowBWR := RowBWR{SerializedPair{""}, "", "", "", "", ""}
  serializeBWR(bwr, &rowBWR)
  sqlStatement := fmt.Sprintf("INSERT INTO %s %s VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s');", 
  tableName, pairsbwrheader, id, rowBWR.pair.value, rowBWR.meanX, rowBWR.meanY, rowBWR.sigmaX, rowBWR.sigmaY, rowBWR.cXY)
  execDB(db, &sqlStatement)
}

/* Insert one row (basic window result + dft) to db */
func insertRowBWRDFT(db *sql.DB, bwrdft *BasicWindowDFTResult, id int, tableName string) {
  rowBWRDFT := RowBWRDFT{SerializedPair{""}, "", "", "", "", ""}
  serializeBWRDFT(bwrdft, &rowBWRDFT)
  sqlStatement := fmt.Sprintf("INSERT INTO %s %s VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s');", 
  tableName, pairsbwrdftheader, id, rowBWRDFT.pair.value, rowBWRDFT.meanX, rowBWRDFT.meanY, rowBWRDFT.sigmaX, rowBWRDFT.sigmaY, rowBWRDFT.dXY)
  execDB(db, &sqlStatement)
}

/* Append row statistics to rows statement */
func appendRowBWR(statement *strings.Builder, bwr *BasicWindowResult, id int) {
  rowBWR := RowBWR{SerializedPair{""}, "", "", "", "", ""}
  serializeBWR(bwr, &rowBWR)
  (*statement).WriteString(fmt.Sprintf(" (%d, '%s', '%s', '%s', '%s', '%s', '%s')",
  id, rowBWR.pair.value, rowBWR.meanX, rowBWR.meanY, rowBWR.sigmaX, rowBWR.sigmaY, rowBWR.cXY))
}

/* Append row statistics to rows statement (with dft) */
func appendRowBWRDFT(statement *strings.Builder, bwrdft *BasicWindowDFTResult, id int) {
  rowBWRDFT := RowBWRDFT{SerializedPair{""}, "", "", "", "", ""}
  serializeBWRDFT(bwrdft, &rowBWRDFT)
  (*statement).WriteString(fmt.Sprintf(" (%d, '%s', '%s', '%s', '%s', '%s', '%s')",
  id, rowBWRDFT.pair.value, rowBWRDFT.meanX, rowBWRDFT.meanY, rowBWRDFT.sigmaX, rowBWRDFT.sigmaY, rowBWRDFT.dXY))
}

/* Insert rows to db in strings.Builder */
func insertRowsBWR(db *sql.DB, statement *strings.Builder) {
  str := (*statement).String()
  execDB(db, &str)
}

/* Insert rows to db in string */
func insertRowsBWRString(db *sql.DB, s *string) {
  execDB(db, s)
}

/* Helper function: transfer slices of float64 to a row of string */
func slicesToString(slices *([]float64), row *string) {
  var sb strings.Builder
  for i := 0; i < len(*slices); i += 1 {
    sb.WriteString(fmt.Sprintf("%.5f", (*slices)[i]))
    if (i != len(*slices) - 1) {
      sb.WriteString(",")
    }
  }
  *row = sb.String()
  if len(*row) > 10000 {
    panic("Size of string is too large")
  }
}

/* Serialize BasicWindowResult to RowBWR in case for insertion */
func serializeBWR(bwr *BasicWindowResult, rowBWR *RowBWR) {
  // Serialize Pair
  serializedPairString := fmt.Sprintf("%d,%d,%d,%d", 
    bwr.pair.leftLocation, bwr.pair.rightLocation, bwr.pair.indexOfRow, bwr.pair.indexOfCol)
  if len(serializedPairString) > 30 {
    fmt.Println("len(serializedPairString): ", len(serializedPairString))
  }
  serializedPair := SerializedPair{serializedPairString}
  rowBWR.pair = serializedPair
  slicesToString(bwr.slicesOfMeanX, &rowBWR.meanX)
  slicesToString(bwr.slicesOfMeanY, &rowBWR.meanY)
  slicesToString(bwr.slicesOfSigmaX, &rowBWR.sigmaX)
  slicesToString(bwr.slicesOfSigmaY, &rowBWR.sigmaY)
  slicesToString(bwr.slicesOfCXY, &rowBWR.cXY)
}

/* Serialize BasicWindowDFTResult to RowBWRDFT in case for insertion */
func serializeBWRDFT(bwrdft *BasicWindowDFTResult, rowBWRDFT *RowBWRDFT) {
  // Serialize Pair
  serializedPairString := fmt.Sprintf("%d,%d,%d,%d", 
    bwrdft.pair.leftLocation, bwrdft.pair.rightLocation, bwrdft.pair.indexOfRow, bwrdft.pair.indexOfCol)
  if len(serializedPairString) > 30 {
    fmt.Println("len(serializedPairString): ", len(serializedPairString))
  }
  serializedPair := SerializedPair{serializedPairString}
  rowBWRDFT.pair = serializedPair
  slicesToString(bwrdft.slicesOfMeanX, &rowBWRDFT.meanX)
  slicesToString(bwrdft.slicesOfMeanY, &rowBWRDFT.meanY)
  slicesToString(bwrdft.slicesOfSigmaX, &rowBWRDFT.sigmaX)
  slicesToString(bwrdft.slicesOfSigmaY, &rowBWRDFT.sigmaY)
  slicesToString(bwrdft.slicesOfDXY, &rowBWRDFT.dXY)
}

/* Helper function: transfer a row of string to slices of float64 (index is from start to end - 1) */
func stringToSlices(row *string, slices *([]float64), start int, end int) {
  strSlices := strings.Split(*row, ",")
  for index, str := range strSlices {
    if index < start {
      continue
    }
    if index >= end {
      break
    }
    floatVal, err := strconv.ParseFloat(str, 64)
    if err != nil {
      panic(err)
    }
    (*slices)[index - start] = floatVal
  }
}

/* Serialize RowBWR to BasicWindowResult */
func deserializRowBWR(rowBWR *RowBWR, bwr *BasicWindowResult, start int, end int) {
  _, err := fmt.Sscanf(rowBWR.pair.value, "%d,%d,%d,%d", &bwr.pair.leftLocation, &bwr.pair.rightLocation, &bwr.pair.indexOfRow, &bwr.pair.indexOfCol)
  if err != nil {
    panic(err)
  }
  stringToSlices(&rowBWR.meanX, bwr.slicesOfMeanX, start, end)
  stringToSlices(&rowBWR.meanY, bwr.slicesOfMeanY, start, end)
  stringToSlices(&rowBWR.sigmaX, bwr.slicesOfSigmaX, start, end)
  stringToSlices(&rowBWR.sigmaY, bwr.slicesOfSigmaY, start, end)
  stringToSlices(&rowBWR.cXY, bwr.slicesOfCXY, start, end)
}

/* Serialize RowBWRDFT to BasicWindowDFTResult */
func deserializRowBWRDFT(rowBWRDFT *RowBWRDFT, bwrdft *BasicWindowDFTResult, start int, end int) {
  _, err := fmt.Sscanf(rowBWRDFT.pair.value, "%d,%d,%d,%d", &bwrdft.pair.leftLocation, &bwrdft.pair.rightLocation, &bwrdft.pair.indexOfRow, &bwrdft.pair.indexOfCol)
  if err != nil {
    panic(err)
  }
  stringToSlices(&rowBWRDFT.meanX, bwrdft.slicesOfMeanX, start, end)
  stringToSlices(&rowBWRDFT.meanY, bwrdft.slicesOfMeanY, start, end)
  stringToSlices(&rowBWRDFT.sigmaX, bwrdft.slicesOfSigmaX, start, end)
  stringToSlices(&rowBWRDFT.sigmaY, bwrdft.slicesOfSigmaY, start, end)
  stringToSlices(&rowBWRDFT.dXY, bwrdft.slicesOfDXY, start, end)
}

/* Helper function: update matrix */
func updateMatrix(matrix *([][]int), thres float64, pair *Pair, slicesOfMeanX *([]float64), slicesOfMeanY *([]float64), 
  slicesOfSigmaX *([]float64), slicesOfSigmaY *([]float64), slicesOfCXY *([]float64), slicesOfDXY *([]float64), isDFT bool, accurateMatrix *([][]float64)) {
  var corr float64 = 0
  var numerator float64 = 0
  var demoninator1 float64 = 0
  var demoninator2 float64 = 0
  meanXValue := getAvg(slicesOfMeanX)
  meanYValue := getAvg(slicesOfMeanY)
  slicesOfDeltaX := make([]float64, len(*slicesOfMeanX))
  slicesOfDeltaY := make([]float64, len(*slicesOfMeanY))
  size := len(slicesOfDeltaX)
  for i := 0; i < size; i += 1 {
    slicesOfDeltaX[i] = (*slicesOfMeanX)[i] - meanXValue
    slicesOfDeltaY[i] = (*slicesOfMeanY)[i] - meanYValue
  }
  for i := 0; i < size; i += 1 {
    if !isDFT {
      numerator += (*slicesOfSigmaX)[i] * (*slicesOfSigmaY)[i] * (*slicesOfCXY)[i] + slicesOfDeltaX[i] * slicesOfDeltaY[i]
    } else {
      numerator += (*slicesOfSigmaX)[i] * (*slicesOfSigmaY)[i] * (*slicesOfDXY)[i] * (*slicesOfDXY)[i] - 2 * (*slicesOfSigmaX)[i] * (*slicesOfSigmaY)[i] - 2 * slicesOfDeltaX[i] * slicesOfDeltaY[i]
    }
    demoninator1 += (*slicesOfSigmaX)[i] * (*slicesOfSigmaX)[i] + slicesOfDeltaX[i] * slicesOfDeltaX[i]
    demoninator2 += (*slicesOfSigmaY)[i] * (*slicesOfSigmaY)[i] + slicesOfDeltaY[i] * slicesOfDeltaY[i]
  }
  if !isDFT {
    corr = numerator/(math.Sqrt(demoninator1) * math.Sqrt(demoninator2))
  } else {
    var dSquare float64 = 2 + numerator / (math.Sqrt(demoninator1) * math.Sqrt(demoninator2))
    corr = 1 - 0.5 * dSquare
  }
  if accurateMatrix != nil {
    (*accurateMatrix)[pair.indexOfRow][pair.indexOfCol] = corr
    (*accurateMatrix)[pair.indexOfCol][pair.indexOfRow] = corr
  }
  if math.Abs(corr) >= thres {
    (*matrix)[pair.indexOfRow][pair.indexOfCol] = 1
    (*matrix)[pair.indexOfCol][pair.indexOfRow] = 1
  }
  realMatrix[pair.indexOfRow][pair.indexOfCol] = math.Abs(corr)
  realMatrix[pair.indexOfCol][pair.indexOfRow] = math.Abs(corr)
}

/* Query by the range of ids, updates matrix meanwhile */
func queryRowsDB(db *sql.DB, tableName string, 
  startID int, endID int, matrix *([][]int), thres float64, numberOfBasicwindows int, isDFT bool, 
  queryStart int, queryEnd int) string {
  sqlStatement := fmt.Sprintf("SELECT * FROM %s WHERE id >= %d AND id < %d",
    tableName, startID, endID)
  t0 := time.Now()
  rows, err := db.Query(sqlStatement)
  elapsed := time.Since(t0)
  if err != nil {
    panic(err)
  }
  defer rows.Close()
  lengthOfSlices := queryEnd - queryStart
  if queryEnd < 0 {
    lengthOfSlices = numberOfBasicwindows
    queryStart = 0
    queryEnd = numberOfBasicwindows
  }
  var rowBWR RowBWR
  var rowBWRDFT RowBWRDFT
  for rows.Next() {
    var id int
    var pair string
    var meanX string
    var meanY string
    var sigmaX string
    var sigmaY string
    var cXY string
    var dXY string
    if !isDFT {
      err = rows.Scan(&id, &pair, &meanX, &meanY, &sigmaX, &sigmaY, &cXY)
    } else {
      err = rows.Scan(&id, &pair, &meanX, &meanY, &sigmaX, &sigmaY, &dXY)
    }
    if err != nil {
      panic(err)
    }
    rowBWR = RowBWR{SerializedPair{pair}, meanX, meanY, sigmaX, sigmaY, cXY}
    if isDFT {
      rowBWRDFT = RowBWRDFT{SerializedPair{pair}, meanX, meanY, sigmaX, sigmaY, dXY}
    }
    slicesOfMeanX := make([]float64, lengthOfSlices)
    slicesOfMeanY := make([]float64, lengthOfSlices)
    slicesOfSigmaX := make([]float64, lengthOfSlices)
    slicesOfSigmaY := make([]float64, lengthOfSlices)
    slicesOfCXY := make([]float64, lengthOfSlices)
    slicesOfDXY := make([]float64, lengthOfSlices)
    var bwr BasicWindowResult = BasicWindowResult{Pair{0, 0, 0, 0}, &slicesOfMeanX, &slicesOfMeanY, &slicesOfSigmaX, &slicesOfSigmaY, &slicesOfCXY}
    var bwrdft BasicWindowDFTResult = BasicWindowDFTResult{Pair{0, 0, 0, 0}, &slicesOfMeanX, &slicesOfMeanY, &slicesOfSigmaX, &slicesOfSigmaY, &slicesOfDXY, nil, nil}
    if !isDFT {
      deserializRowBWR(&rowBWR, &bwr, queryStart, queryEnd)
      // Update matrix
      updateMatrix(matrix, thres, &(bwr.pair), bwr.slicesOfMeanX, bwr.slicesOfMeanY, bwr.slicesOfSigmaX, bwr.slicesOfSigmaY, bwr.slicesOfCXY, nil, false, nil)
    } else {
      deserializRowBWRDFT(&rowBWRDFT, &bwrdft, queryStart, queryEnd)
      // Update matrix
      updateMatrix(matrix, thres, &(bwrdft.pair), bwrdft.slicesOfMeanX, bwrdft.slicesOfMeanY, bwrdft.slicesOfSigmaX, bwrdft.slicesOfSigmaY, nil, bwrdft.slicesOfDXY, true, nil)
    }
  }
  return fmt.Sprintf("%v", elapsed)
}

/* -------------------------------------------------------------------- */

/* String representation of Point */
func displayPoint(dataPoint Point) {
  fmt.Println(fmt.Sprintf("%#v", dataPoint))
}

/* Get average value of a variable-length array */
func getAvg(arr *([]float64)) float64 {
  var sum float64 = 0
  for i := 0; i < len(*arr); i += 1 {
    sum += (*arr)[i]
  }
  return sum/float64(len(*arr))
}

/* Get max value of a variable-length array */
func getMax(arr *([]float64)) float64 {
  var max float64 = (*arr)[0]
  for i := 1; i < len(*arr); i += 1 {
    if (*arr)[i] > max {
      max = (*arr)[i]
    }
  }
  return max
}

/* Get sum of a variable-length array */
func getSum(arr *([]float64)) float64 {
  var sum float64 = 0
  for i := 0; i < len(*arr); i += 1 {
    sum += (*arr)[i]
  }
  return sum
}

/* -------------------------------------------------------------------- */

/* Transfer an array of string in time format to an array of float64 */
func stringToFloatInSlices(arr []string) ([]float64) {
  res := make([]float64, len(arr))
  for i := 0; i < len(arr); i += 1 {
    res[i] = stringToSeconds(arr[i])
  }
  return res
}

/* Transfer time-formatted string to seconds */
func stringToSeconds(time string) float64 {
  var index int = 0
  var res float64 = 0
  for index < len(time) {
    res += stringToSecondsHelper(time, &index)
  }
  return res
}

/* Helper function for Transfering time-formatted string to seconds */
func stringToSecondsHelper(time string, indexPtr *int) float64 {
  var sb strings.Builder
  var floatVal float64
  for i := *indexPtr; i < len(time); i += 1 {
    s := string(time[i])
    _, err := strconv.Atoi(s)
    if err == nil {
      sb.WriteString(s)
    } else {
      if s == "h" {
        numberStr := sb.String()
        floatVal, _ = strconv.ParseFloat(numberStr, 64)
        *indexPtr = i + 1
        return floatVal * 3600
      }
      if s == "m" {
        if i + 1 >= len(time) {
          floatVal, _ = strconv.ParseFloat(sb.String(), 64)
          *indexPtr = i + 1
          return floatVal * 60
        }
        nextLetter := string(time[i + 1])
        if nextLetter == "s" {
          floatVal, _ = strconv.ParseFloat(sb.String(), 64)
          *indexPtr = i + 2
          return floatVal * 0.001
        }
        floatVal, _ = strconv.ParseFloat(sb.String(), 64)
        *indexPtr = i + 1
        return floatVal * 60
      }
      if s == "µ" {
        floatVal, _ = strconv.ParseFloat(sb.String(), 64)
        *indexPtr = i + 2
        return floatVal * 0.000001
      }
      if s == "s" {
        floatVal, _ = strconv.ParseFloat(sb.String(), 64)
        *indexPtr = i + 1
        return floatVal
      }
      if s == "." {
        sb.WriteString(s)
      }
    }
  }
  return 0
}

/* -------------------------------------------------------------------- */

/* Get the latitudes */
func GetLatitudes() []float64 {
  return realLat
}

/* Get the longitudes */
func GetLongitudes() []float64 {
  return realLon
}

/* Get the latitudes */
func GetLatitudesIdx() []int {
  return realLatIdx
}

/* Get the longitudes */
func GetLongitudesIdx() []int {
  return realLonIdx
}

/* Get the length of dataMap */
func GetTimeSeriesNum() int {
  return len(dataMap)
}

/* Get length of time series */
func GetTimeSeriesLength() int {
  prevSize := -1
  size := 0
  for key := range dataMap {
    size = len(dataMap[key])
    if prevSize < 0 {
      prevSize = size
    } else {
      if prevSize != size {
        fmt.Println("WARNING: time series is unaligned.")
        break
      } else {
        prevSize = size
      }
    }
  }
  return size
}

/* Set Basic Window Size */
func SetBasicWindowSize(size int) {
  basicWindowSize = size
}

/* Get Basic Window Size */
func GetBasicWindowSize() int {
  return basicWindowSize
}

/* Get the information of dataMap */
func GetDataMapInfo() int {
  return getMapInfo(&dataMap)
}

/* Get the information of dataMap_ */
func getMapInfo(dataMap_ *(map[int][]Point)) int {
  length := len(*dataMap_)
  prevSize := -1
  size := 0
  for key := range *dataMap_ {
    size = len((*dataMap_)[key])
    if prevSize < 0 {
      prevSize = size
    } else {
      if prevSize != size {
        fmt.Println("WARNING: time series is unaligned.")
        break
      } else {
        prevSize = size
      }
    }
  }
  fmt.Println(fmt.Sprintf("Num of locations: %d, length of time series: %d", length, size))
  return size
}

/* Get a slice of dataMap */
func CutDataMap(newDataMap *(map[int][]Point), start int, end int) {
  size := GetDataMapInfo()
  if start < 0 || end > size {
    fmt.Println("WARNING: start or end is not correct!")
    return
  }
  for key := range dataMap {
    (*newDataMap)[key] = dataMap[key][start:end]
  }
}

/* Clear dataMap */
func ClearDataMap() {
  dataMap = make(map[int][]Point)
}

/* Set all items in the mastrix as 0 */
func clearMatrix(matrix *([][]int)) {
  for i := 0; i < len(*matrix); i += 1 {
    for j := 0; j < len((*matrix)[0]); j += 1 {
      (*matrix)[i][j] = 0
    }
  }
}

/* Set all items in the slices as 0 */
func clearSliceOfString(slice *([]string)) {
  for i := 0; i < len(*slice); i += 1 {
    (*slice)[i] = ""
  }
}

/* Get the number of edges with given graph */
func checkMatrix(matrix *([][]int)) int {
  sumOfConnectedPairs := 0
  for i := 0; i < len(*matrix); i += 1 {
    for j := i + 1; j < len((*matrix)[0]); j += 1 {
      if (*matrix)[i][j] == 1 {
        sumOfConnectedPairs += 1
      }
    }
  }
  fmt.Println(sumOfConnectedPairs)
  return sumOfConnectedPairs
}

/* Get locations from given dataMap */
func getLocations(dataMap *(map[int][]Point), locations *([]int)) {
  i := 0
  for key := range *dataMap {
    (*locations)[i] = key
    i += 1
  }
}

/* Get locations */
func GetLocations() []int {
  i := 0
  locations := make([]int, len(dataMap))
  for key := range dataMap {
    locations[i] = key
    i += 1
  }
  return locations
}

// N <= w
func getDFTResult(sigma float64, avg float64, w int, N int, 
    xs *([]float64), result *([]complex128)) {
  for f := 0; f < N; f += 1 {
    var sum complex128 = 0
    for i := 0; i < w; i += 1 {
      xi := ((*xs)[i] - avg) / sigma
      sum += cmplx.Rect(xi, 2 * math.Pi * float64(f * i) / float64(w))
    }
    Xf := complex(1 / math.Sqrt(float64(w)), 0) * sum
    (*result)[f] = Xf
  }
}

/* Get euclidean distance */
func getEuclideanDistance(left *([]complex128), right *([]complex128)) float64 {
  var res float64 = 0
  for i := 0; i < len(*left); i += 1 {
    diff := cmplx.Abs((*left)[i] - (*right)[i])
    res += diff * diff
  }
  return math.Sqrt(res)
}

/* Get the number of basic windows */
func getNumberOfBasicwindows(dataMap *(map[int][]Point), granularity int) int {
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  return len((*dataMap)[locations[0]])/granularity
}

/* Get the number of basic windows */
func GetNumberOfBW(granularity int) int {
  locationsNum := len(dataMap)
  locations := make([]int, locationsNum)
  getLocations(&dataMap, &locations)
  res := len(dataMap[locations[0]])/granularity
  fmt.Println("Total number of basic windows:", res)
  return res
}

/* Helper function: get bwr from a specific pair, also get number of basic windows and store the value to the reference */
func getBasicWindowResult(dataMap *(map[int][]Point), granularity int,
  pair *Pair, bwr *BasicWindowResult, bwrdft *BasicWindowDFTResult, isDFT bool, ratio float64) {
  // Pair{leftLocation, rightLocation, i, j}
  leftPointsSlices := (*dataMap)[pair.leftLocation]
  rightPointsSlices := (*dataMap)[pair.rightLocation]
  numberOfBasicwindows := len(leftPointsSlices)/granularity
  var basicWindowIndex int = 0
  // Statistics for basic windows
  var count float64 = 0
  var sumOfX float64 = 0
  var sumOfY float64 = 0
  var sumSquaredX float64 = 0
  var sumSquaredY float64 = 0
  var sumOfXY float64 = 0
  var countOfRemained float64 = 0
  var sumOfXRemained float64 = 0
  var sumOfYRemained float64 = 0
  var sumSquaredXRemained float64 = 0
  var sumSquaredYRemained float64 = 0
  var sumOfXYRemained float64 = 0
  slicesOfMeanX := make([]float64, numberOfBasicwindows)
  slicesOfMeanY := make([]float64, numberOfBasicwindows)
  slicesOfSigmaX := make([]float64, numberOfBasicwindows)
  slicesOfSigmaY := make([]float64, numberOfBasicwindows)
  slicesOfCXY := make([]float64, numberOfBasicwindows)
  slicesOfDXY := make([]float64, numberOfBasicwindows)
  // Slices for DFT
  slicesOfRemainedX := make([]float64, granularity)
  slicesOfRemainedY := make([]float64, granularity)
  slicesOfSumSquaredX := make([]float64, numberOfBasicwindows)
  slicesOfSumSquaredY := make([]float64, numberOfBasicwindows)
  // Compute basic window statistics
  for k := 0; k < len(leftPointsSlices); k += 1 {
    if isDFT {
      slicesOfRemainedX[int(countOfRemained)] = leftPointsSlices[k].temperature
      slicesOfRemainedY[int(countOfRemained)] = rightPointsSlices[k].temperature
    }
    countOfRemained += 1
    sumOfXRemained += leftPointsSlices[k].temperature
    sumOfYRemained += rightPointsSlices[k].temperature
    sumSquaredXRemained += leftPointsSlices[k].temperature * leftPointsSlices[k].temperature
    sumSquaredYRemained += rightPointsSlices[k].temperature * rightPointsSlices[k].temperature
    sumOfXYRemained += leftPointsSlices[k].temperature * rightPointsSlices[k].temperature
    if int(countOfRemained) == granularity {
      var sigmaX float64 = math.Sqrt((sumSquaredXRemained/countOfRemained) - (sumOfXRemained*sumOfXRemained)/(countOfRemained*countOfRemained))
      var sigmaY float64 = math.Sqrt((sumSquaredYRemained/countOfRemained) - (sumOfYRemained*sumOfYRemained)/(countOfRemained*countOfRemained))
      var cXY float64 = (countOfRemained*sumOfXYRemained - sumOfXRemained*sumOfYRemained)/
                        (math.Sqrt(countOfRemained*sumSquaredXRemained - sumOfXRemained*sumOfXRemained)*
                        math.Sqrt(countOfRemained*sumSquaredYRemained - sumOfYRemained*sumOfYRemained))
      if (countOfRemained*sumOfXYRemained - sumOfXRemained*sumOfYRemained) == 0 {
        cXY = 0
      }
      // Update statistics
      count += countOfRemained
      sumOfX += sumOfXRemained
      sumOfY += sumOfYRemained
      sumSquaredX += sumSquaredXRemained
      sumSquaredY += sumSquaredYRemained
      sumOfXY += sumOfXYRemained
      slicesOfMeanX[basicWindowIndex] = sumOfXRemained/countOfRemained
      slicesOfMeanY[basicWindowIndex] = sumOfYRemained/countOfRemained
      slicesOfSigmaX[basicWindowIndex] = sigmaX
      slicesOfSigmaY[basicWindowIndex] = sigmaY
      slicesOfCXY[basicWindowIndex] = cXY
      if isDFT {
        N := int(float64(granularity)*ratio)
        slicesDFTX := make([]complex128, N)
        slicesDFTY := make([]complex128, N)
        getDFTResult(sigmaX, sumOfXRemained/countOfRemained, granularity, N, &slicesOfRemainedX, &slicesDFTX)
        getDFTResult(sigmaY, sumOfYRemained/countOfRemained, granularity, N, &slicesOfRemainedY, &slicesDFTY)
        d := getEuclideanDistance(&slicesDFTX, &slicesDFTY)
        slicesOfDXY[basicWindowIndex] = d
        // For DFT updates
        slicesOfSumSquaredX[basicWindowIndex] = sumSquaredXRemained
        slicesOfSumSquaredY[basicWindowIndex] = sumSquaredYRemained
      }
      // Reset remained values
      countOfRemained = 0
      sumOfXRemained = 0
      sumOfYRemained = 0
      sumSquaredXRemained = 0
      sumSquaredYRemained = 0
      sumOfXYRemained = 0
      // Basic Window Index increment
      basicWindowIndex += 1
    }
  }
  if !isDFT {
    bwr.pair = *pair
    bwr.slicesOfMeanX = &slicesOfMeanX
    bwr.slicesOfMeanY = &slicesOfMeanY
    bwr.slicesOfSigmaX = &slicesOfSigmaX
    bwr.slicesOfSigmaY = &slicesOfSigmaY
    bwr.slicesOfCXY = &slicesOfCXY
  } else {
    bwrdft.pair = *pair
    bwrdft.slicesOfMeanX = &slicesOfMeanX
    bwrdft.slicesOfMeanY = &slicesOfMeanY
    bwrdft.slicesOfSigmaX = &slicesOfSigmaX
    bwrdft.slicesOfSigmaY = &slicesOfSigmaY
    bwrdft.slicesOfDXY = &slicesOfDXY
    bwrdft.slicesOfSumSquaredX = &slicesOfSumSquaredX
    bwrdft.slicesOfSumSquaredY = &slicesOfSumSquaredY
  }
}

/* Sketching part for TSUBASA */
func getBasicWindows(dataMap *(map[int][]Point), granularity int, 
  db *sql.DB, id *int, blockSize int, tableName string, header string, isDFT bool, ratio float64) {
  // Get locations
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  // Nested loops
  var i, j int
  *id = 0 // Set *id to 0
  var accumulate int = 0
  blockInsertionSQLStarter := fmt.Sprintf("INSERT INTO %s %s VALUES ", tableName, header)
  var statementSB strings.Builder
  statementSB.WriteString(blockInsertionSQLStarter)
  for i = 0; i < locationsNum; i += 1 {
    for j = i + 1; j < locationsNum; j += 1 {
      var leftLocation int = locations[i]
      var rightLocation int = locations[j]
      var pair Pair = Pair{leftLocation, rightLocation, i, j}
      var bwr BasicWindowResult
      var bwrdft BasicWindowDFTResult
      if !isDFT {
        getBasicWindowResult(dataMap, granularity, &pair, &bwr, nil, isDFT, ratio)
      } else {
        getBasicWindowResult(dataMap, granularity, &pair, nil, &bwrdft, isDFT, ratio)
      }
      if blockSize <= 0 {
        if !isDFT {
          insertRowBWR(db, &bwr, *id, tableName)
        } else {
          insertRowBWRDFT(db, &bwrdft, *id, tableName)
        }
      } else {
        // Accumulate
        if accumulate > 0 {
          statementSB.WriteString(",")
        }
        if !isDFT {
          appendRowBWR(&statementSB, &bwr, *id)
        } else {
          appendRowBWRDFT(&statementSB, &bwrdft, *id)
        }
        accumulate += 1
        if accumulate == blockSize {
          // Insert rows
          statementSB.WriteString(";")
          insertRowsBWR(db, &statementSB)
          // Reset values
          accumulate = 0
          statementSB.Reset()
          statementSB.WriteString(blockInsertionSQLStarter)
        }
      }
      (*id) += 1 // id increment
    }
  }
  if blockSize > 0 && accumulate > 0 {
    // Insert remained rows
    statementSB.WriteString(";")
    insertRowsBWR(db, &statementSB)
  }
}

/* ---------------------------------------------------------------------- */

/* Get the number of CPUs */
func getNumCPU() int {
  return runtime.NumCPU()
}

/* Partition data to NCPU lists */
func partitionData(NCPU int, dataMap *(map[int][]Point), listOfPairs *([][]Pair)) {
  // Separate the data map by NCPU
  // The pairs of locations locations are assigned to the list evenly
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  numOfPairs := (locationsNum * (locationsNum - 1)) / 2
  quotient := numOfPairs / NCPU
  remained := numOfPairs % NCPU
  for i := 0; i < NCPU; i += 1 {
    if (i < remained) {
      (*listOfPairs)[i] = make([]Pair, quotient + 1)
    } else {
      (*listOfPairs)[i] = make([]Pair, quotient)
    }
  }
  indexOfRow := 0
  indexOfCol := 1
  for i := 0; i < NCPU; i += 1 {
    for j := 0; j < len((*listOfPairs)[i]); j += 1 {
      (*listOfPairs)[i][j] = Pair{locations[indexOfRow], locations[indexOfCol], indexOfRow, indexOfCol}
      indexOfCol += 1
      if indexOfCol == locationsNum {
        indexOfRow += 1
        indexOfCol = indexOfRow + 1
      }
    }
  }
  //fmt.Println(indexOfRow)
  //fmt.Println(indexOfCol)
  //fmt.Println("Assigned locations: PARTITION FINISHED")
}

func getBatchesNum(partitionsNum int, listOfPairs *([][]Pair), blockSize int) int {
  var res int = 0
  for i := 0; i < partitionsNum; i += 1 {
    var length int = len((*listOfPairs)[i])
    if length % blockSize == 0 {
      res += length / blockSize
    } else {
      res += length / blockSize + 1
    }
  }
  return res
}