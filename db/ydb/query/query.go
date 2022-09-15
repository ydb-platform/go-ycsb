package query

import (
	"bytes"
	_ "embed"
	"sort"
	"strconv"
	"strings"
	"text/template"
)

var (
	queryTemplate *template.Template
)

func render(query string, data interface{}) string {
	var buf bytes.Buffer
	err := queryTemplate.ExecuteTemplate(&buf, query, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

var (
	//go:embed create_table.yql
	createTableQuery string

	//go:embed drop_table.yql
	dropTableQuery string

	//go:embed scan.yql
	scanQuery string

	//go:embed read.yql
	readQuery string

	//go:embed update.yql
	updateQuery string

	//go:embed delete.yql
	deleteQuery string

	//go:embed insert.yql
	insertQuery string

	//go:embed batch_read.yql
	batchReadQuery string

	//go:embed batch_insert.yql
	batchInsertQuery string

	//go:embed batch_update.yql
	batchUpdateQuery string

	//go:embed batch_delete.yql
	batchDeleteQuery string
)

func init() {
	queryTemplate = template.New("")
	for _, q := range []string{
		createTableQuery,
		dropTableQuery,
		insertQuery,
		deleteQuery,
		updateQuery,
		readQuery,
		batchInsertQuery,
		batchUpdateQuery,
		batchDeleteQuery,
		batchReadQuery,
		scanQuery,
	} {
		_, err := queryTemplate.New(q).Parse(q)
		if err != nil {
			panic(err)
		}
	}
}

type commonData struct {
	TablePathPrefix string
	TableName       string
}

func Update(tablePathPrefix, tableName string) string {
	return render(updateQuery, commonData{
		TablePathPrefix: tablePathPrefix,
		TableName:       tableName,
	})
}

func toUpper(ss []string) []string {
	for i, s := range ss {
		ss[i] = strings.ToUpper(s)
	}
	return ss
}

func Scan(tablePathPrefix, tableName string, columns []string) string {
	sort.Strings(columns)
	return render(scanQuery, commonDataWithColumns{
		commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
		},
		toUpper(columns),
	})
}

func BatchRead(tablePathPrefix, tableName string, columns []string) string {
	sort.Strings(columns)
	return render(batchReadQuery, commonDataWithColumns{
		commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
		},
		toUpper(columns),
	})
}

func Read(tablePathPrefix, tableName string, columns []string) string {
	sort.Strings(columns)
	return render(readQuery, commonDataWithColumns{
		commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
		},
		toUpper(columns),
	})
}

func BatchUpdate(tablePathPrefix, tableName string) string {
	return render(batchUpdateQuery, commonData{
		TablePathPrefix: tablePathPrefix,
		TableName:       tableName,
	})
}

func BatchInsert(tablePathPrefix, tableName string) string {
	return render(batchInsertQuery, commonData{
		TablePathPrefix: tablePathPrefix,
		TableName:       tableName,
	})
}

func Insert(tablePathPrefix, tableName string) string {
	return render(insertQuery, commonData{
		TablePathPrefix: tablePathPrefix,
		TableName:       tableName,
	})
}

func BatchDelete(tablePathPrefix, tableName string) string {
	return render(batchDeleteQuery, commonData{
		TablePathPrefix: tablePathPrefix,
		TableName:       tableName,
	})
}

func Delete(tablePathPrefix, tableName string) string {
	return render(deleteQuery, commonData{
		TablePathPrefix: tablePathPrefix,
		TableName:       tableName,
	})
}

func DropTable(tablePathPrefix, tableName string) string {
	return render(dropTableQuery, commonData{
		TablePathPrefix: tablePathPrefix,
		TableName:       tableName,
	})
}

type commonDataWithColumns struct {
	commonData
	Columns []string
}

func CreateTable(tablePathPrefix, tableName string, fieldsCount int) string {
	columns := make([]string, fieldsCount)
	for i := 0; i < fieldsCount; i++ {
		columns[i] = "FIELD" + strconv.Itoa(i)
	}
	return render(createTableQuery, commonDataWithColumns{
		commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
		},
		columns,
	})
}
