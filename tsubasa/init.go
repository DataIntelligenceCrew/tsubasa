package tsubasa

func Init() {
	dataMap = make(map[int][]Point)
	InitMatrix()
	NCPU := getNumCPU()
	pairWindowsList = make([][]BasicWindowResult, NCPU)
	SetBasicWindowSize(-1)
}

func InitDB(username string, password_ string) {
	user = username
	password = password_
	Init()
}

func InitMatrix() {
	matrix = make([][]int, len(dataMap))
  for i := range matrix {
    matrix[i] = make([]int, len(dataMap))
  }

  realMatrix = make([][]float64, len(dataMap))
  for i := range realMatrix {
    realMatrix[i] = make([]float64, len(dataMap))
  }
}

