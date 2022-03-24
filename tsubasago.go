package tsubasa

import (
	"math"
	"time"
	"fmt"
	"sort"
)

func DirectCompute(thres float64, start int, end int) []int {
	// Matrix initiation
  InitMatrix()
  newDataMap := make(map[int][]Point)
  t0 := time.Now()
  CutDataMap(&newDataMap, start, end)
	networkConstructionNaiveParallel(&newDataMap, &matrix, thres)
	elapsed := time.Since(t0)
	fmt.Println("Time:", elapsed)
	checkMatrix(&matrix)

	return GetMatrix()
}

func SketchInDB() {
	granularity := basicWindowSize
	var sketchDurations []string = make([]string, getNumCPU()-1)
  networkConstructionBWParallelSketch(&dataMap, granularity, 1000, false, 1.0, &sketchDurations)
}

func QueryInDB(thres float64, start int, end int, granularity int) []int {
	InitMatrix()
	var queryDurations []string = make([]string, getNumCPU()-1)
  var queryReadTime []float64 = make([]float64, getNumCPU()-1)
  networkConstructionBWParallelQuery(&dataMap, &matrix, thres, granularity, 1000, false, start, end, &queryDurations, &queryReadTime)
  checkMatrix(&matrix)

  return GetMatrix()
}

func GetMatrix() []int {
	arr := make([]int, len(matrix) * len(matrix))
  index := 0
  for i := 0; i < len(matrix); i += 1 {
  	for j := 0; j < len(matrix); j += 1 {
  		arr[index] = matrix[i][j]
  		index += 1
  	}
  }
  return arr
}

func GetRealMatrix() []float64 {
	realArr := make([]float64, len(realMatrix) * len(realMatrix))
  index := 0
  for i := 0; i < len(realMatrix); i += 1 {
  	for j := 0; j < len(realMatrix); j += 1 {
  		if math.IsNaN(realMatrix[i][j]) {
  			realArr[index] = float64(0.0)
  		} else {
  			realArr[index] = float64(realMatrix[i][j])
  		}
  		index += 1
  	}
  }
  return realArr
}

func ResetSketch() {
	DeleteSkecth(false)
}

func Sketch() string {
	return networkConstructionBWParallelSketchInMem(basicWindowSize)
}

func Query(thres float64, queryStart int, queryEnd int) []int {
	networkConstructionBWParallelQueryInMem(queryStart, queryEnd, thres)
	checkMatrix(&matrix)
  return GetMatrix()
}

/* Get realMatrix */
func GetCorrelationMatrix(queryStart int, length int) []float64 {
	networkConstructionBWParallelQueryInMem(queryStart, queryStart + length, 0.7)
	return GetRealMatrix()
}

/* Get matrix */
func GetNetworkUnweighted(queryStart int, length int, thres float64) []int {
	networkConstructionBWParallelQueryInMem(queryStart, queryStart + length, thres)
	return GetMatrix()
}

func GetNetworkWeightedRatio(queryStart int, length int, rho float64) []float64 {
	list := GetCorrelationMatrix(queryStart, length)
	list_sorted := make([]float64, len(list))
	copy(list_sorted, list)
	sort.Float64s(list_sorted)
	var thres float64 = list_sorted[int(float64(len(list)) * (1 - rho))]
	for j := 0; j < len(list); j += 1 {
		if list[j] < thres {
			list[j] = 0.0
		}
	}
	return list
}
