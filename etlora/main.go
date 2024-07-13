package main

import (
	"etlo2m/common"
	"etlo2m/etldata"
	"io/ioutil"
	"log"
	_ "net/http/pprof"
	"runtime"
	"sync"
)

func ora2mysql() {
	var splitInfo [][][2]string
	var err error

	if common.Parallel > 1 {
		splitInfo, _ = etldata.SplitTab()
	} else {
		splitInfo = nil
	}

	// echo task do split unit
	tasknum := len(splitInfo) / common.Parallel

	dataChannel := make(chan []interface{}, common.Parallel+1)

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	if splitInfo == nil {
		wg1.Add(1)
		go func(processid int) {
			// fmt.Println("Read process %d start !", processid)
			defer wg1.Done()
			err := etldata.FetchDataForMySQL(nil, dataChannel)
			if err != nil {
				log.Fatal("Read process %d failed : %w", processid, err)
			}
			// fmt.Println("Read process %d end !", processid)
		}(1)

		wg2.Add(1)
		go func(processid int) {
			// fmt.Println("Write process %d start !", processid)
			defer wg2.Done()
			err := etldata.InsertDataToMySQL(dataChannel)
			if err != nil {
				log.Fatal("Write process %d failed : %w", processid, err)
			}
			// fmt.Println("Write process %d end !", processid)
		}(1)
	} else {
		for i := 0; i < common.Parallel; i++ {
			wg1.Add(1)
			start := i * tasknum
			end := (i + 1) * tasknum
			go func(processid int) {
				// fmt.Println("Read process %d start !", processid)
				defer wg1.Done()
				if processid == common.Parallel-1 {
					for x := start; x < len(splitInfo); x++ {
						err = etldata.FetchDataForMySQL(splitInfo[x], dataChannel)
					}
				} else {
					for x := start; x < end; x++ {
						err = etldata.FetchDataForMySQL(splitInfo[x], dataChannel)
					}
				}

				if err != nil {
					log.Fatal("Read process %d end !", processid)
				}
				// fmt.Println("Read process %d end !", processid)
			}(i)
		}

		for i := 0; i < common.Parallel; i++ {
			wg2.Add(1)
			go func(processid int) {
				// fmt.Println("Write process %d start !", processid)
				defer wg2.Done()
				err := etldata.InsertDataToMySQL(dataChannel)
				if err != nil {
					log.Fatal("Write process %d failed : %w", processid, err)
				}
				// fmt.Println("Write process %d end !", processid)
			}(i)
		}
	}

	wg1.Wait()
	close(dataChannel)
	wg2.Wait()
}

func ora2pgsql() {
	var splitInfo [][][2]string
	var err error

	if common.Parallel > 1 {
		splitInfo, _ = etldata.SplitTab()
	} else {
		splitInfo = nil
	}

	// echo task do split unit
	tasknum := len(splitInfo) / common.Parallel

	dataChannel := make(chan []interface{}, common.Parallel+1)

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	if splitInfo == nil {
		wg1.Add(1)
		go func(processid int) {
			// fmt.Println("Read process %d start !", processid)
			defer wg1.Done()
			err := etldata.FetchDataForPgSQL(nil, dataChannel)
			if err != nil {
				log.Fatal("Read process %d failed : %w", processid, err)
			}
			// fmt.Println("Read process %d end !", processid)
		}(1)

		wg2.Add(1)
		go func(processid int) {
			// fmt.Println("Write process %d start !", processid)
			defer wg2.Done()
			err := etldata.CopyDataToPgSQL(dataChannel)
			if err != nil {
				log.Fatal("Write process %d failed : %w", processid, err)
			}
			// fmt.Println("Write process %d end !", processid)
		}(1)
	} else {
		for i := 0; i < common.Parallel; i++ {
			wg1.Add(1)
			start := i * tasknum
			end := (i + 1) * tasknum
			go func(processid int) {
				// fmt.Println("Read process %d start !", processid)
				defer wg1.Done()
				if processid == common.Parallel-1 {
					for x := start; x < len(splitInfo); x++ {
						err = etldata.FetchDataForPgSQL(splitInfo[x], dataChannel)
					}
				} else {
					for x := start; x < end; x++ {
						err = etldata.FetchDataForPgSQL(splitInfo[x], dataChannel)
					}
				}

				if err != nil {
					log.Fatal("Read process %d end !", processid)
				}
				// fmt.Println("Read process %d end !", processid)
			}(i)
		}

		for i := 0; i < common.Parallel; i++ {
			wg2.Add(1)
			go func(processid int) {
				// fmt.Println("Write process %d start !", processid)
				defer wg2.Done()
				err := etldata.CopyDataToPgSQL2(dataChannel)
				if err != nil {
					log.Fatal("Write process %d failed : %w", processid, err)
				}
				// fmt.Println("Write process %d end !", processid)
			}(i)
		}
	}

	wg1.Wait()
	close(dataChannel)
	wg2.Wait()
}

func main() {
	log.SetOutput(ioutil.Discard)
	runtime.GOMAXPROCS(common.Parallel)

	if common.DbType == "mysql" {
		ora2mysql()
	} else {
		ora2pgsql()
	}
}
