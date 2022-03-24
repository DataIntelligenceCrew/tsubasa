package tsubasa

import (
	"fmt"
	"math"
	"github.com/fhs/go-netcdf/netcdf"
	"os"
	"log"
	"strings"
	"strconv"
	"sort"
)

var realLon []float64
var realLat []float64
var realLonIdx []int
var realLatIdx []int

func initSlices(slice *([]float64)) {
	*slice = make([]float64, 0)
}

func initSlicesInt(slice *([]int)) {
	*slice = make([]int, 0)
}

func getAttrs(ds netcdf.Dataset, nattrs int) {
	for i := 0; i < nattrs; i += 1 {
		attr, _ := ds.AttrN(i)
		attrname := attr.Name()
		fmt.Println(fmt.Sprintf("attr %d: %s", i, attrname))
	}
}

func AddDataFromFile(filename string, locationRangeFile string) error {
	// Open example file in read-only mode. The dataset is returned.
	ds, err := netcdf.OpenFile(filename, netcdf.NOWRITE)
	if err != nil {
		return err
	}
	defer ds.Close()

	var minLon float64 = -1
	var maxLon float64 = -2
	var minLat float64 = -1
	var maxLat float64 = -2
	if len(locationRangeFile) > 0 {
		content, err := os.ReadFile(locationRangeFile)
	  if err != nil {
	      log.Fatal(err)
	  }
	  //fmt.Println(string(content))
	  s := strings.Split(string(content), ",")
		minLon, _ = strconv.ParseFloat(s[0], 64)
		maxLon, _ = strconv.ParseFloat(s[1], 64)
		minLat, _ = strconv.ParseFloat(s[2], 64)
		maxLat, _ = strconv.ParseFloat(s[3], 64)
	}

	if minLon <= maxLon {
		initSlices(&realLon)
		initSlicesInt(&realLonIdx)
	}
	if minLat <= maxLat {
		initSlices(&realLat)
		initSlicesInt(&realLatIdx)
	}

	//nvars, _ := ds.NVars()
	//nattrs, _ := ds.NAttrs()

	//fmt.Println(fmt.Sprintf("NVars: %d", nvars))
	//fmt.Println(fmt.Sprintf("NAttrs: %d", nattrs))

	/*for i := 0; i < nvars; i += 1 {
		varname, _ := ds.VarN(i).Name()
		vartype, _ := ds.VarN(i).Type()
		fmt.Println(fmt.Sprintf("var %d: %s, type: %s", i, varname, vartype))
	}*/

	lat, _ := netcdf.GetFloat32s(ds.VarN(0))
	dimsLat, _ := ds.VarN(0).LenDims()
	lon, _ := netcdf.GetFloat32s(ds.VarN(1))
	dimsLon, _ := ds.VarN(1).LenDims()
	time, _ := netcdf.GetFloat64s(ds.VarN(2))
	dimsTime, _ := ds.VarN(2).LenDims()
	air, _ := netcdf.GetFloat32s(ds.VarN(3))
	dimsAir, _ := ds.VarN(3).LenDims()

	slicesLat := make([]float32, dimsLat[0])
	slicesLon := make([]float32, dimsLon[0])
	slicesTime := make([]float64, dimsTime[0])

	for i := 0; i < int(dimsLat[0]); i += 1 {
		slicesLat[i] = lat[i]
		if slicesLat[i] >= float32(minLat) && slicesLat[i] <= float32(maxLat) {
			realLat = append(realLat, float64(slicesLat[i]))
			realLatIdx = append(realLatIdx, i)
		}
	}
	for i := 0; i < int(dimsLon[0]); i += 1 {
		slicesLon[i] = lon[i]
		if slicesLon[i] >= float32(minLon) && slicesLon[i] <= float32(maxLon) {
			realLon = append(realLon, float64(slicesLon[i]))
			realLonIdx = append(realLonIdx, i)
		}
	}
	for i := 0; i < int(dimsTime[0]); i += 1 {
		slicesTime[i] = time[i]
	}
	sort.Float64s(realLon)
	sort.Float64s(realLat)
	sort.Ints(realLonIdx)
	sort.Ints(realLatIdx)
	//fmt.Println(slicesLat)
	//fmt.Println(slicesLon)
	//fmt.Println(slicesTime)

	//fmt.Println(dimsAir[0], dimsAir[1], dimsAir[2])
	index := 0
	for i := 0; i < int(dimsAir[0]); i += 1 {
		for j := 0; j < int(dimsAir[1]); j += 1 {
			for k := 0; k < int(dimsAir[2]); k += 1 {
				location := j * 1000 + k // lat * 1000 + lon
				if float64(air[index]) == math.NaN() {
					fmt.Println(air[index])
				}
				dataPoint := Point{slicesTime[i], slicesLat[j], slicesLon[k], location, float64(air[index])}
				_, ok := (dataMap)[location]
				if minLon < 0 {
					if !ok {
						var points []Point
	        	points = append(points, dataPoint)
	        	(dataMap)[location] = points
					} else {
						(dataMap)[location] = append((dataMap)[location], dataPoint)
					}
				} else {
					if slicesLat[j] <= float32(maxLat) && slicesLat[j] >= float32(minLat) && slicesLon[k] <= float32(maxLon) && slicesLon[k] >= float32(minLon) {
						if !ok {
							var points []Point
		        	points = append(points, dataPoint)
		        	(dataMap)[location] = points
						} else {
							(dataMap)[location] = append((dataMap)[location], dataPoint)
						}
					}
				}
				index += 1
			}
		}
	}
	//fmt.Println(index)

	//getAttrs(ds, nattrs)

	return nil
}