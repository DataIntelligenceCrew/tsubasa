package tsubasa

import (
  "fmt"
  "math"
  "sort"
  "gonum.org/v1/gonum/mat"
  "time"
  "github.com/cheggaaa/pb/v3"
)

func getIndex(arr *([]int), val int) int {
	for k, v := range *arr {
		if v == val {
			return k
		}
	}
	return -1
}

func matPrint(X mat.Matrix) {
  fa := mat.Formatted(X, mat.Prefix(""), mat.Squeeze())
  fmt.Printf("%v\n", fa)
}

func Slide(start int, cnt int, qsize int, bwsize int, offset int, rho float64) []float64 {
	arr := make([]mat.Matrix, cnt)
	var end int

	t0 := time.Now()

	for i := 0; i < cnt; i += 1 {
		end = start + int(qsize/bwsize)
		Query(0.7, start, end)
		start += int(offset/bwsize)
		list := GetRealMatrix()
		list_sorted := make([]float64, len(list))
		copy(list_sorted, list)
		sort.Float64s(list_sorted)
		var thres float64 = list_sorted[int(float64(len(list)) * (1 - rho))]
		for j := 0; j < len(list); j += 1 {
			if list[j] < thres {
				list[j] = 0.0
			}
		}
		rowNum := int(math.Sqrt(float64(len(list))))
		M := mat.NewDense(rowNum, rowNum, list)
		arr[i] = M
	}

	res := make([]float64, cnt)

	fmt.Println("Starting to compute transitivity: ")
	bar := pb.StartNew(cnt)
	for i := 0; i < cnt; i += 1 {
		res[i] = getTrans(arr[i])
		bar.Increment()
	}
	bar.Finish()

	elapsed := time.Since(t0)
	fmt.Println("Slide time:", elapsed)
	fmt.Println(res)
	return res
}

func getw(locations *([]int), i int) float64 {
	loc := (*locations)[i]
	lat := int(loc / 1000)
	latList := GetLatitudes()
	latIdxList := GetLatitudesIdx()
	latVal := latList[getIndex(&latIdxList, lat)]
	return math.Cos((latVal * math.Pi)/180.0)
}

func getws(locations *([]int), ws *([]float64)) {
	length := len(*locations)
	for i := 0; i < length; i += 1 {
		(*ws)[i] = getw(locations, i)
	}
}

func GetTransitivity(arr []float64) float64 {
	rowNum := int(math.Sqrt(float64(len(arr))))
	X := mat.NewDense(rowNum, rowNum, arr)
	return getTrans(X)
}

func getTrans(X mat.Matrix) float64 {
	numerator := 0.0
	denominator := 1.0
	locations := GetLocations()
	ws := make([]float64, len(locations))
	getws(&locations, &ws)
	lenws := len(ws)
	w_matrix_lst := make([]float64, lenws*lenws)
	for i := 0; i < lenws; i += 1 {
		for j := 0; j < lenws; j += 1 {
			w_matrix_lst[i*lenws + j] = ws[i]
		}
	}
	W := mat.NewDense(lenws, lenws, w_matrix_lst)
	TMP := mat.NewDense(lenws, lenws, nil)
	TMP.MulElem(W, X)
	A := mat.NewDense(lenws, lenws, nil)
	A.Product(TMP, TMP)
	A.MulElem(A, TMP.T())
	numerator = mat.Sum(A)
	B := mat.NewDense(lenws, lenws, nil)
	B.MulElem(W.T(), TMP)
	B.Product(TMP, B)
	denominator = mat.Sum(B)
	return numerator / denominator
}