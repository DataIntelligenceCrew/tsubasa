package tsubasa

import (
	"fmt"
	"os"
	"log"
	"sort"
	"github.com/cheggaaa/pb/v3"
)

func getFilesInDir(dirname string, filesLst *([]string)) {
	f, err := os.Open(dirname)
  if err != nil {
      log.Fatal(err)
  }
  files, err := f.Readdir(-1)
  f.Close()
  if err != nil {
    log.Fatal(err)
  }
  for _, file := range files {
    //fmt.Println(file.Name())
    (*filesLst) = append((*filesLst), file.Name())
  }
}

func ReadFiles(dirname string) error {
	var files []string
	getFilesInDir(dirname, &files)
	fmt.Println("Reading files ... ")
	bar := pb.StartNew(len(files))
	for _, fn := range files {
		AddDataFromFile(dirname + "/" + fn, "")
		bar.Increment()
	}
	bar.Finish()

	for location := range dataMap {
		sort.Slice((dataMap)[location][:], func(i, j int) bool {
			return (dataMap)[location][i].timestamp < (dataMap)[location][j].timestamp
		})
	}
	return nil
}

func ReadFile(filename string) {
	AddDataFromFile(filename, "")

	for location := range dataMap {
		sort.Slice((dataMap)[location][:], func(i, j int) bool {
			return (dataMap)[location][i].timestamp < (dataMap)[location][j].timestamp
		})
	}
}

func ReadFileByLocation(filename string, locationRangeFile string) {
	AddDataFromFile(filename, locationRangeFile)

	for location := range dataMap {
		sort.Slice((dataMap)[location][:], func(i, j int) bool {
			return (dataMap)[location][i].timestamp < (dataMap)[location][j].timestamp
		})
	}
}

func ReadFilesByLocation(dirname string, locationRangeFile string) error {
	var files []string
	getFilesInDir(dirname, &files)
	fmt.Println("Reading files ... ")
	bar := pb.StartNew(len(files))
	for _, fn := range files {
		AddDataFromFile(dirname + "/" + fn, locationRangeFile)
		bar.Increment()
	}
	bar.Finish()

	for location := range dataMap {
		sort.Slice((dataMap)[location][:], func(i, j int) bool {
			return (dataMap)[location][i].timestamp < (dataMap)[location][j].timestamp
		})
	}
	return nil
}