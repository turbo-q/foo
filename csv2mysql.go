package foo

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"

	_ "github.com/go-sql-driver/mysql"
)

func CSV2mysql() {
	file, _ := ioutil.ReadFile("./recite_v2_resources.csv")
	reader := bytes.NewReader(file)
	csvReader := csv.NewReader(reader)

	// 链接数据库
	db, _ := sql.Open("mysql", "root:kingyin123@tcp(localhost:3306)/recitation_square?charset=utf8")

	csvReader.Read() // 忽略第一行

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

		stmt, err := db.Prepare(`INSERT t_resource (id,title,author,words,cover_l,cover_s,content,reading,read_num,created_at) values (?,?,?,?,?,?,?,?,?,?)`)

		stmt.Exec(record...)
	}
}
