package common

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	_ "gitee.com/opengauss/openGauss-connector-go-pq"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
)

var (
	OraDsn    string
	TarDsn    string
	Parallel  int
	SSchema   string
	STable    string
	TSchema   string
	TTable    string
	ST        string
	TT        string
	FetchSize int
	LobFlag   int
	ColLen    int
	ColList   []string
	DbType    string
	WhereS    string
)

func init() {
	// initial args
	// oraetl -su username -sp password -sdsn ip:port/servicename -tu username -tp password -tdsn ip:port/dbname -p parallel -s owner,tablename -t owner.tablename -d dbtype
	oracleUser := flag.String("su", "", "force, oracle username")
	oraclePasswd := flag.String("sp", "", "force, oracle password, enclose in single quota")
	oracleDsn := flag.String("sdsn", "", "force, oracle connection info, example : ip:port/service_name")
	targetUser := flag.String("tu", "", "force, target db username")
	targetPasswd := flag.String("tp", "", "force, target db password, enclose in single quota")
	targetDsn := flag.String("tdsn", "", "force, target db connection info, example : ip:port/dbname")
	parallel := flag.Int("p", 1, "When you can query dba_extents, you can parallel etl data, max 16")
	sourceTable := flag.String("s", "", "force, etl from oracle table. owner.tablename")
	targetTable := flag.String("t", "", "etl to target table. db.tablename, default dbname.sourcetable")
	targetDbType := flag.String("d", "", "force, target database type : mysql, pgsql")
	fetchSize := flag.Int("f", 1, "fetchsize")
	tableHaveData := flag.Int("h", 1, "Behavior when the target table has data: 1 : interrupt program, 2 : Continue execution")
	whereInfo := flag.String("w", "", "Set filtering conditions, enclose in quota, example : where id > 100;  partition(p20220101)")

	// parge args
	flag.Parse()

	// check force args
	requireArgs := []*string{oracleUser, oraclePasswd, oracleDsn, targetUser, targetPasswd, targetDsn, sourceTable, targetDbType}
	for _, param := range requireArgs {
		if *param == "" {
			fmt.Println("Error : please input force args")
			flag.Usage()
			os.Exit(1)
		}
	}

	// check dsn
	formatArgs := []*string{oracleDsn, targetDsn}
	for _, param := range formatArgs {
		if !validateDsn(*param) {
			fmt.Println("Error : Incorrent formatting ", *param)
			flag.Usage()
			os.Exit(1)
		}
	}

	// check target db type
	DbType = *targetDbType
	if DbType != "mysql" && DbType != "pgsql" {
		fmt.Println("Target database type must be mysql or pgsql!")
		flag.Usage()
		os.Exit(1)
	}

	OraDsn = `user="` + *oracleUser + `" password="` + *oraclePasswd + `" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=` +
		strings.Split(*oracleDsn, ":")[0] + `)(PORT=` + strings.Split(strings.Split(*oracleDsn, ":")[1], "/")[0] + `))(CONNECT_DATA=(SERVICE_NAME=` +
		strings.Split(*oracleDsn, "/")[1] + `)))" heterogeneousPool=false poolIncrement=1 poolMaxSessions=1000 poolMinSessions=1 poolSessionMaxLifetime=1h0m0s poolSessionTimeout=5m0s  standaloneConnection=false poolWaitTimeout=30s timezone="+08:00"`

	if DbType == "mysql" {
		TarDsn = *targetUser + ":" + *targetPasswd + "@tcp(" + strings.Split(*targetDsn, "/")[0] + ")/" + strings.Split(*targetDsn, "/")[1] + "?parseTime=true&loc=Local"
	} else {
		// TarDsn = "postgres://" + *targetUser + ":" + *targetPasswd + "@" + *targetDsn
		TarDsn = "host=" + strings.Split(*targetDsn, ":")[0] + " port=" + strings.Split(strings.Split(*targetDsn, ":")[1], "/")[0] + " user=" + *targetUser +
			" password=" + *targetPasswd + " dbname=" + strings.Split(*targetDsn, "/")[1] + " sslmode=disable"
	}

	if *parallel > 16 {
		Parallel = 16
	} else if *parallel < 1 {
		Parallel = 1
	} else {
		Parallel = *parallel
	}

	// table information
	if strings.Contains(*sourceTable, ".") {
		SSchema = strings.ToUpper(strings.Split(*sourceTable, ".")[0])
		STable = strings.ToUpper(strings.Split(*sourceTable, ".")[1])
	} else {
		SSchema = ""
		STable = strings.ToUpper(*sourceTable)
	}

	if SSchema == "" {
		ST = STable
	} else {
		ST = SSchema + "." + STable
	}

	if *targetTable == "" {
		TSchema = strings.Split(*targetDsn, "/")[1]
		TTable = strings.ToLower(STable)
	} else {
		if strings.Contains(*targetTable, ".") {
			TSchema = strings.Split(*targetTable, ".")[0]
			TTable = strings.ToLower(strings.Split(*targetTable, ".")[1])
		} else {
			TSchema = strings.Split(*targetDsn, "/")[1]
			TTable = strings.ToLower(*targetTable)
		}
	}

	TT = TSchema + "." + TTable

	// print task header
	fmt.Printf("Copy data from %s[%s] to %s[%s] !\n", *oracleDsn, ST, *targetDsn, TT)

	// check target table hava data
	if *tableHaveData == 1 {
		var dataFlag int
		checkDataSql := "select count(*) from (select * from " + TT + " limit 1) a"
		if DbType == "mysql" {
			targetDB, err := CreateMysConn(TarDsn)
			if err != nil {
				log.Fatal(err)
			}

			err = targetDB.QueryRow(checkDataSql).Scan(&dataFlag)
			if err != nil {
				log.Fatal("Failed to query target database : ", err)
			}

			targetDB.Close()
		} else {
			targetDB, err := CreatePgsConn(TarDsn)
			if err != nil {
				log.Fatal(err)
			}

			err = targetDB.QueryRow(checkDataSql).Scan(&dataFlag)
			if err != nil {
				log.Fatal("Failed to query target database : ", err)
			}

			targetDB.Close()
		}

		if dataFlag > 0 {
			log.Fatal("Table in target database is not empty!")
		}
	}

	// get FetchSize
	oradb, err := CreateOraConn(OraDsn)
	if err != nil {
		log.Fatal(err)
	}

	// if blob, fetchsize = 1
	colInfoSql := "select column_name from all_tab_cols where column_name not like 'SYS\\_%$' ESCAPE '\\' and owner = '" +
		SSchema + "' and table_name = '" + STable + "' order by column_id"
	rows, err := oradb.Query(colInfoSql)
	if err != nil {
		log.Fatal("Failed to get column information from source database : ", err)
	}

	for rows.Next() {
		var colName string
		err := rows.Scan(&colName)
		if err != nil {
			log.Fatal("Failed to fetch column information from source database :", err)
		}
		ColList = append(ColList, colName)
	}

	ColLen = len(ColList)

	blobSql := "select count(*) from all_tab_cols where data_type like '%BLOB' and table_name = '" + STable + "' and owner = '" + SSchema + "'"
	err = oradb.QueryRow(blobSql).Scan(&LobFlag)
	if err != nil {
		log.Fatal("Failed to get blob columh information from source database : ", err)
	}

	if *fetchSize > 1 {
		FetchSize = *fetchSize
	} else {
		if LobFlag > 0 {
			FetchSize = 1
		} else {
			if DbType == "mysql" {
				FetchSize = 65535 / ColLen
			} else {
				FetchSize = 65535
			}
		}
	}

	// check parallel info
	extentSql := "select count(*) from dba_extents where 1=0"
	_, err = oradb.Query(extentSql)
	if err != nil {
		fmt.Println("Warnning : You can not access dba_extents, set parallel to 1!")
		Parallel = 1
	}

	// 解析where条件
	if *whereInfo != "" {
		if validataWhere(*whereInfo) {
			WhereS = strings.TrimLeft(*whereInfo, " ")
		} else {
			fmt.Println("The format of the input where condition is incorrect!")
			flag.Usage()
			os.Exit(1)
		}
	} else {
		WhereS = ""
	}

	oradb.Close()
}

// where条件必须是以where 或者 partition开头
func validataWhere(input string) bool {
	wherestr := strings.TrimLeft(input, " ")
	pattern := `(?i)^(where|partition)`
	regex := regexp.MustCompile(pattern)

	return regex.MatchString(wherestr)
}

// check dsn
func validateDsn(input string) bool {
	pattern := `^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+\/[a-zA-Z0-9_]+$`
	regex := regexp.MustCompile(pattern)

	return regex.MatchString(input)
}

func CreateOraConn(dsn string) (*sql.DB, error) {
	db, err := sql.Open("godror", dsn)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to Oracle database : %w", err)
	}
	return db, nil
}

func CreateMysConn(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to mysql database : %w", err)
	}
	return db, nil
}

func CreatePgsConn(dsn string) (*sql.DB, error) {
	db, err := sql.Open("opengauss", dsn)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to postgres database : %w", err)
	}
	return db, nil
}
