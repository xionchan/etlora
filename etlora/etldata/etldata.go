package etldata

import (
	"etlo2m/common"
	"fmt"
	pq "gitee.com/opengauss/openGauss-connector-go-pq"
	"github.com/godror/godror"
	// "github.com/jackc/pgx/v5"
	"log"
	"strings"

	_ "gitee.com/opengauss/openGauss-connector-go-pq"
)

// split oracle table in rowid
func SplitTab() ([][][2]string, error) {
	if common.Parallel == 1 {
		return nil, nil
	} else {
		getTabRowSql := "select coalesce(num_rows, 0) from all_tables where table_name = '" + common.STable + "' and owner = '" + common.SSchema + "'"
		var tabRows int
		oradb, err := common.CreateOraConn(common.OraDsn)
		if err != nil {
			log.Fatal(err)
		}

		err = oradb.QueryRow(getTabRowSql).Scan(&tabRows)
		if err != nil {
			log.Fatal("Failed to get table rows information!")
		}

		if (common.LobFlag > 0 && tabRows > 5*10000) || tabRows > 50*10000 {
			splitSql := `select dbms_rowid.rowid_create(1, data_object_id, relative_fno, block_id, 0),
       						dbms_rowid.rowid_create(1, data_object_id, relative_fno, block_id + blocks - 1, 32767) 
					 from dba_Extents a, all_objects b
    				where a.segment_name = b.object_name  AND (a.PARTITION_NAME is null or a.PARTITION_NAME = b.SUBOBJECT_NAME) 
    				  and a.owner = b.owner and a.owner = '` + common.SSchema + "' and a.segment_name = '" + common.STable + "'"
			rows, err := oradb.Query(splitSql)
			if err != nil {
				log.Fatal("Get table rowid info failed, ", err)
			}

			var allSplit [][2]string
			for rows.Next() {
				var rowid1, rowid2 string
				_ = rows.Scan(&rowid1, &rowid2)
				allSplit = append(allSplit, [2]string{rowid1, rowid2})
			}

			var splitNum int
			if common.LobFlag > 0 {
				splitNum = tabRows/(5*10000) + 1
			} else {
				splitNum = tabRows/(50*10000) + 1
			}

			// split rec for splitnum
			chunkSize := len(allSplit) / splitNum
			returnSplit := make([][][2]string, int(splitNum))
			for i := 0; i < splitNum; i++ {
				start := i * chunkSize
				end := start + chunkSize

				if i == splitNum-1 {
					end = len(allSplit)
				}
				returnSplit[i] = allSplit[start:end]
			}
			return returnSplit, nil
		} else {
			return nil, nil
		}
	}
}

// Fetch data for mysql
func FetchDataForMySQL(rowidlist [][2]string, datachannel chan []interface{}) error {
	oradb, err := common.CreateOraConn(common.OraDsn)
	if err != nil {
		log.Fatal(err)
	}

	// put thread do task sql into selice
	getDataSqlList := []string{}
	if rowidlist == nil {
		getDataSqlList = append(getDataSqlList, `select "`+strings.Join(common.ColList, `","`)+`" from `+common.ST+" "+common.WhereS)
	} else {
		if common.WhereS != "" {
			if strings.HasPrefix(strings.ToLower(common.WhereS), "where") {
				ewhstr := common.WhereS[5:]
				for _, value := range rowidlist {
					getDataSqlList = append(getDataSqlList, `select "`+strings.Join(common.ColList, `","`)+`" from `+common.ST+" where rowid between '"+value[0]+"' and '"+value[1]+"' and "+ewhstr)
				}
			} else {
				ewhstr := common.WhereS
				for _, value := range rowidlist {
					getDataSqlList = append(getDataSqlList, `select "`+strings.Join(common.ColList, `","`)+`" from `+common.ST+" "+ewhstr+" where rowid between '"+value[0]+"' and '"+value[1]+"'")
				}
			}
		} else {
			for _, value := range rowidlist {
				getDataSqlList = append(getDataSqlList, `select "`+strings.Join(common.ColList, `","`)+`" from `+common.ST+" where rowid between '"+value[0]+"' and '"+value[1]+"'")
				// getDataSqlList = append(getDataSqlList, "select * from "+common.ST+" where rowid between '"+value[0]+"' and '"+value[1]+"'")
			}
		}
	}

	// put results
	var results []interface{}
	rowstr := make([]interface{}, common.ColLen)
	for i := 0; i < common.ColLen; i++ {
		rowstr[i] = new(interface{})
	}

	for _, getDataSql := range getDataSqlList {
		if getDataSql != "" {
			mRows, err := oradb.Query(getDataSql, godror.PrefetchCount(common.FetchSize), godror.FetchArraySize(common.FetchSize))
			if err != nil {
				return fmt.Errorf("Failed to fetch data from source database : ", err)
			}
			defer mRows.Close()

			for mRows.Next() {
				err := mRows.Scan(rowstr...)
				if err != nil {
					return fmt.Errorf("Failed to scan data from source database : ", err)
				}
				// 每一行结果集
				var rowData interface{}
				// 转换结果集
				for i := 0; i < common.ColLen; i++ {
					val := *rowstr[i].(*interface{})
					switch val.(type) {
					case godror.Number:
						rowData = fmt.Sprintf("%v", val)
					case string:
						if val == "" {
							rowData = nil
						} else {
							rowData = val
						}
					default:
						rowData = val
					}
					results = append(results, rowData)
				}

				if len(results)%(common.FetchSize*common.ColLen) == 0 {
					datachannel <- results
					results = nil
				}
			}
		}
	}
	datachannel <- results
	return nil
}

// Fetch data for postgres
func FetchDataForPgSQL(rowidlist [][2]string, datachannel chan []interface{}) error {
	oradb, err := common.CreateOraConn(common.OraDsn)
	if err != nil {
		log.Fatal(err)
	}

	// put thread do task sql into selice
	getDataSqlList := []string{}
	if rowidlist == nil {
		getDataSqlList = append(getDataSqlList, "select "+strings.Join(common.ColList, ",")+" from "+common.ST)
	} else {
		for _, value := range rowidlist {
			getDataSqlList = append(getDataSqlList, "select "+strings.Join(common.ColList, ",")+" from "+common.ST+" where rowid between '"+value[0]+"' and '"+value[1]+"'")
		}
	}

	// put result
	var results []interface{}
	rowstr := make([]interface{}, common.ColLen)
	for i := 0; i < common.ColLen; i++ {
		rowstr[i] = new(interface{})
	}

	for _, getDataSql := range getDataSqlList {
		if getDataSql != "" {
			mRows, err := oradb.Query(getDataSql, godror.PrefetchCount(common.FetchSize), godror.FetchArraySize(common.FetchSize))
			if err != nil {
				return fmt.Errorf("Failed to fetch data from source database : ", err)
			}
			defer mRows.Close()

			for mRows.Next() {
				err := mRows.Scan(rowstr...)
				if err != nil {
					return fmt.Errorf("Failed to scan data from source database : ", err)
				}

				// every column data
				var rowData interface{}
				// 转换结果集
				for i := 0; i < common.ColLen; i++ {
					val := *rowstr[i].(*interface{})
					switch val.(type) {
					case godror.Number:
						rowData = fmt.Sprintf("%v", val)
					case string:
						if val == "" {
							rowData = nil
						} else {
							rowData = val
						}
					default:
						rowData = val
					}
					results = append(results, rowData)
				}

				if len(results)%(common.FetchSize*common.ColLen) == 0 {
					datachannel <- results
					results = nil
				}
			}
		}
	}
	datachannel <- results
	return nil
}

// Insert data to mysql
func InsertDataToMySQL(datachannel chan []interface{}) error {
	mysdb, err := common.CreateMysConn(common.TarDsn)
	if err != nil {
		log.Fatal(err)
	}

	insertSql := "INSERT INTO " + common.TT + "(`"
	for _, col := range common.ColList {
		insertSql = insertSql + col + "`,`"
	}
	insertSql = insertSql[0 : len(insertSql)-2]
	stmt1 := insertSql + ") VALUES " + strings.Repeat("("+strings.Repeat("?,", common.ColLen-1)+"?),", common.FetchSize)
	stmt1 = stmt1[0 : len(stmt1)-1]

	stmt2, err := mysdb.Prepare(stmt1)
	if err != nil {
		return fmt.Errorf("%s Failed to prepare statement: [%w]", common.ST, err)
	}

	for data := range datachannel {
		if len(data) == common.FetchSize*common.ColLen {
			_, err := stmt2.Exec(data...)
			if err != nil {
				return fmt.Errorf("%s : Failed insert data to target database : [%w]", common.ST, err)
			}
		} else if len(data) > 0 {
			stmt3 := insertSql + ") VALUES " + strings.Repeat("("+strings.Repeat("?,", common.ColLen-1)+"?),", len(data)/common.ColLen)
			stmt3 = stmt3[0 : len(stmt3)-1]
			_, err := mysdb.Exec(stmt3, data...)
			if err != nil {
				return fmt.Errorf("%s : Failed insert data to target database : [%w]", common.ST, err)
			}
		} else {
			continue
		}
	}

	mysdb.Close()
	return nil
}

// Insert data to openGauss
func CopyDataToPgSQL2(datachannel chan []interface{}) error {
	pgsdb, err := common.CreatePgsConn(common.TarDsn)
	if err != nil {
		log.Fatal(err)
	}

	insertSql := "INSERT INTO " + common.TT + "("
	for _, col := range common.ColList {
		insertSql = insertSql + col + ","
	}
	insertSql = insertSql[0 : len(insertSql)-1]
	stmt1 := insertSql + ") VALUES "
	ss := ""
	for num := 1; num <= common.ColLen*common.FetchSize; num++ {
		if (num-1)%common.ColLen == 0 {
			ss = ss + "($" + fmt.Sprintf("%d", num) + ","
		} else if (num)%common.ColLen == 0 {
			ss = ss + "$" + fmt.Sprintf("%d", num) + "),"
		} else {
			ss = ss + "$" + fmt.Sprintf("%d", num) + ","
		}
	}
	stmt := stmt1 + ss[0:len(ss)-1]

	// stmt1 = "INSERT INTO chenxw.CHENXW(ID,V1,N1,D1,V2,N2,V3,T1,N3,V4,N4,D2,V5,N5,V6,N6,T2,V7,N7,T3,V8) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)"
	stmt2, err := pgsdb.Prepare(stmt)
	if err != nil {
		return fmt.Errorf("%s Failed to prepare statement: [%w]", common.ST, err)
	}

	for data := range datachannel {
		if len(data) == common.FetchSize*common.ColLen {
			_, err := stmt2.Exec(data...)
			if err != nil {
				return fmt.Errorf("%s : Failed insert data to target database : [%w]", common.ST, err)
			}
		} else if len(data) > 0 {
			cc := ""
			for num := 1; num <= len(data); num++ {
				if (num-1)%common.ColLen == 0 {
					cc = cc + "($" + fmt.Sprintf("%d", num) + ","
				} else if (num)%common.ColLen == 0 {
					cc = cc + "$" + fmt.Sprintf("%d", num) + "),"
				} else {
					cc = cc + "$" + fmt.Sprintf("%d", num) + ","
				}
			}
			stmt3 := insertSql + ") VALUES " + cc[0:len(cc)-1]
			_, err := pgsdb.Exec(stmt3, data...)
			if err != nil {
				return fmt.Errorf("%s : Failed insert data to target database : [%w]", common.ST, err)
			}
		} else {
			continue
		}
	}

	pgsdb.Close()
	return nil
}

// Copy data to openGauss
func CopyDataToPgSQL(datachannel chan []interface{}) error {
	pgsdb, err := common.CreatePgsConn(common.TarDsn)
	if err != nil {
		log.Fatal(err)
	}

	for data := range datachannel {
		if len(data) != 0 {
			txn, err := pgsdb.Begin()
			if err != nil {
				log.Fatal(err)
			}

			// stmt, err := txn.Prepare(pq.CopyIn(common.TT, common.ColList...))
			stmt, err := txn.Prepare(pq.CopyIn(common.TTable, common.ColList...))
			if err != nil {
				log.Fatal(err)
			}

			n := len(data) / common.ColLen
			for i := 0; i < n; i++ {
				_, err = stmt.Exec(data[i*common.ColLen : (i+1)*common.ColLen]...)
				if err != nil {
					log.Fatal(err)
				}
			}

			_, err = stmt.Exec()
			if err != nil {
				log.Fatal(err)
			}

			err = stmt.Close()
			if err != nil {
				log.Fatal(err)
			}

			err = txn.Commit()
			if err != nil {
				log.Fatal(err)
			}
		} else {
			continue
		}
	}

	pgsdb.Close()
	return nil
}
