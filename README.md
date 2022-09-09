# tsubasa
## Overview
This package is used for climate network construction. More details can be found from [this paper](https://dl.acm.org/doi/abs/10.1145/3514221.3526177).
## Install
Download this package from the GitHub repository and then import it locally.

*Prerequisite*: [NetCDF C Library](https://downloads.unidata.ucar.edu/netcdf/) must be installed firstly.
## Documentation
### Initialization
The init functions are in `init.go`.

1. `Init(void) void`
	> Initiate all required global variables and data structures. All data will be stored in and loaded from the memory. It must be called before any other operations.


2. `InitDB(username string, password string) void`
	> Initiate all required global variables and data structures for disk storage. Data will be stored onto disk. The statistics computed from the originial data could be retrieved and reused from PostgreSQL database on the machine. It also must be called before any other operations.


3. `InitMatrix(void) void`
	> Initiate or reset all values of matrices to zero.


### Data Loading
It can load data directly from NetCDF files (ended with *.nc*). The corresponding functions are in `readfiles.go`.

1. `ReadFile(fileName string) void`
	> Read the given NetCDF file.


2. `ReadFiles(directoryName string) error`
	> Read all NetCDF files from the specific directory.


3. `ReadFileByLocation(fileName string, locationRangeFile string) void`
	> Read the given NetCDF file. The second argument should be a text file to limit the geographical ranges, which contains one line with 4 integers separated by ",". For instance, `100,150,-10,10` denotes minimum longitude, maximum longitude, minimum latitude, and maximum latitude, respectively.


4. `ReadFilesByLocation(directoryName string, locationRangeFile string) error`
	> Read all NetCDF files from the specific directory. The second argument should be a text file to limit the geographical ranges, which contains one line with 4 integers separated by ",". For instance, `100,150,-10,10` denotes minimum longitude, maximum longitude, minimum latitude, and maximum latitude, respectively.

### Information Acquisition
These functions are from `utils.go` and `tsubasago.go`.

1. `GetTimeSeriesNum(void) int`
	> Get the total number of time series


2. `GetTimeSeriesLength(void) int`
	> Get the length of time series. Assume all time series has the same length.


3. `GetBasicWindowSize(void) int`
	> Get the size of basic window.


4. `GetMatrix(void) []int`
	> Get the weighted correlation matrix. The return value is an integer array, which transferred from the N * N matrix to a 1 * (N^2) vector.


5. `GetRealMatrix(void) []float64`
	> Get the unweighted correlation matrix. The return value is a float array, which transferred from the N * N matrix to a 1 * (N^2) vector.


6. `GetDataMapInfo(void) int`
	> Check if each time series has the same length. Meanwhile, return the length of time series.

### Parameters Setting
These functions are from `utils.go`.

1.  `SetBasicWindowSize(size int) void`
	> Set the size of basic window.

### Computation
These functions are used to compute the intermediate statistics and the final correlation matrices. They are from `tsubasago.go`.

*Note*: The package does not provide exposed methods for DFT methods.

1. `DirectCompute(thres float64, start int, end int) []int`
	> Direct in-memory computation with parallel computing. The number of Goroutines depends on the number of CPU obtained from `runtime.NumCPU()`. `thred` usually is set to the value between 0.6 and 0.95. `start` and `end` defines the length of time series.


2. `Sketch(void) string`
	> In-memory sketch with parallel computing. The number of Goroutines depends on the number of CPU obtained from `runtime.NumCPU()`. It returns the total time represented by a string with a time unit.


3. `Query(thres float64, queryStart int, queryEnd int) []int`
	> In-memory Query with parallel computing. The number of Goroutines depends on the number of CPU obtained from `runtime.NumCPU()`. `queryStart` and `queryEnd` are the start and end index of basic window, which makes up to the query window. It returns the weighted correlation matrix. It is an integer array, which transferred from the N * N matrix to a 1 * (N^2) vector. *(N is the number of time series)*


4. `SketchInDB(writersNum int) void`
	> In-DB sketch with parallel computing. The number of Goroutines depends on the number of CPU obtained from `runtime.NumCPU()`. The `writersNum` should be at least 1 and smaller than the half of the total Goroutines (*NCPU*).


5. `QueryInDB(thres float64, start int, end int, granularity int, writersNum int) []int`
	> Query with parallel computing. It reads the statistics from PostgreSQL database. The number of Goroutines depends on the number of CPU obtained from `runtime.NumCPU()`. `granularity` is the basic window size. `writersNum` should be the same as the value in `SketchInDB(writersNum int) void`. It returns the weighted correlation matrix. It is an integer array, which transferred from the N * N matrix to a 1 * (N^2) vector. *(N is the number of time series)*


6. `ResetSketch(writersNum int) void`
	> Reset to the initial state, which means cleanning all results after calling `SketchInDB(writersNum int) void`.


## Example
A simple use case is provided below,
```go
func main() {
	// Initialization
	tsubasa.Init()

	// Read data from a NetCDF file
	tsubasa.ReadFileByLocation("../data.nc", "range.txt")

	// Get time series length
	length := tsubasa.GetTimeSeriesLength()

	// Set basic window size
	tsubasa.SetBasicWindowSize(30)

	tsubasa.Sketch()
	tsubada.Query(0.75, 0, int(length/30) - 1)
}
```
