package foo

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func CSV2mysql(filename, dbname string) {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println(err)
	}
	reader := bytes.NewReader(file)
	csvReader := csv.NewReader(reader)

	// 链接数据库
	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/subject_diagnose?charset=utf8")
	if err != nil {
		fmt.Println(err)
	}

	firstLine, err := csvReader.Read() // 忽略第一行
	if err != nil {
		fmt.Println(err)
		return
	}

	// 字段
	var fields []string
	for _, r := range firstLine {
		fields = append(fields, "`"+r+"`")
	}
	fieldSql := strings.Join(fields, ",")

	// 问号
	var mark []string
	for i := 0; i < len(firstLine); i++ {
		mark = append(mark, "?")
	}
	markSql := strings.Join(mark, ",")

	index := 1
	for {
		re, err := csvReader.Read()
		if err == io.EOF {
			return
		}
		var record []interface{}

		for _, r := range re {
			record = append(record, r)
		}

		fmt.Printf("正在上传第%d条数据\n", index)
		index++

		stmt, err := db.Prepare("INSERT " + dbname + " (" + fieldSql + ") values (" + markSql + ")")
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(record...)
		stmt.Exec(record...)
	}
}
