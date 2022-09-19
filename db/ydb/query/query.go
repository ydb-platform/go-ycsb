package query

import (
	"bytes"
	"database/sql"
	_ "embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"

	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	queryTemplate *template.Template
	queryCache    sync.Map
)

type cacheKey struct {
	query string
	data  any
}

func renderMemo(query string, data interface{}) (s string, fromCache bool) {
	key := fmt.Sprintf("%+v", cacheKey{
		query: query,
		data:  data,
	})
	if v, ok := queryCache.Load(key); ok {
		return v.(string), true
	}
	defer func() {
		queryCache.Store(key, s)
	}()
	var buf bytes.Buffer
	err := queryTemplate.ExecuteTemplate(&buf, query, data)
	if err != nil {
		panic(err)
	}
	return buf.String(), false
}

func render(query string, data interface{}) string {
	s, _ := renderMemo(query, data)
	return s
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
	Declares        []string
}

func Update(tablePathPrefix, tableName string, values types.Value) Request {
	args := []sql.NamedArg{
		sql.Named("values", values),
	}
	return request{
		query: render(updateQuery, commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
			Declares:        asDeclares(args),
		}),
		args: asInterfaces(args),
	}
}

func toUpper(ss []string) []string {
	for i, s := range ss {
		ss[i] = strings.ToUpper(s)
	}
	return ss
}

func Scan(tablePathPrefix, tableName string, columns []string, key string, limit uint64) Request {
	sort.Strings(columns)
	args := []sql.NamedArg{
		sql.Named("key", key),
		sql.Named("limit", limit),
	}
	return request{
		query: render(scanQuery, commonDataWithColumns{
			commonData{
				TablePathPrefix: tablePathPrefix,
				TableName:       tableName,
				Declares:        asDeclares(args),
			},
			toUpper(columns),
		}),
		args: asInterfaces(args),
	}
}

func BatchRead(tablePathPrefix, tableName string, columns []string, keys []string) Request {
	sort.Strings(columns)
	args := []sql.NamedArg{
		sql.Named("keys", keys),
	}
	return request{
		query: render(batchReadQuery, commonDataWithColumns{
			commonData{
				TablePathPrefix: tablePathPrefix,
				TableName:       tableName,
				Declares:        asDeclares(args),
			},
			toUpper(columns),
		}),
		args: asInterfaces(args),
	}
}

func Read(tablePathPrefix, tableName string, columns []string, key string) Request {
	sort.Strings(columns)
	args := []sql.NamedArg{
		sql.Named("key", key),
	}
	return request{
		query: render(readQuery, commonDataWithColumns{
			commonData{
				TablePathPrefix: tablePathPrefix,
				TableName:       tableName,
				Declares:        asDeclares(args),
			},
			toUpper(columns),
		}),
		args: asInterfaces(args),
	}
}

func BatchUpdate(tablePathPrefix, tableName string, values types.Value) Request {
	args := []sql.NamedArg{
		sql.Named("values", values),
	}
	return request{
		query: render(batchUpdateQuery, commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
			Declares:        asDeclares(args),
		}),
		args: asInterfaces(args),
	}
}

func BatchInsert(tablePathPrefix, tableName string, values types.Value) Request {
	args := []sql.NamedArg{
		sql.Named("values", values),
	}
	return request{
		query: render(batchInsertQuery, commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
			Declares:        asDeclares(args),
		}),
		args: asInterfaces(args),
	}
}

func Insert(tablePathPrefix, tableName string, values types.Value) Request {
	args := []sql.NamedArg{
		sql.Named("values", values),
	}
	return request{
		query: render(insertQuery, commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
			Declares:        asDeclares(args),
		}),
		args: asInterfaces(args),
	}
}

func BatchDelete(tablePathPrefix, tableName string, keys []string) Request {
	args := []sql.NamedArg{
		sql.Named("keys", keys),
	}
	return request{
		query: render(batchDeleteQuery, commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
			Declares:        asDeclares(args),
		}),
		args: asInterfaces(args),
	}
}

func asDeclares(args []sql.NamedArg) (declares []string) {
	declares = make([]string, len(args))
	for i, arg := range args {
		param, err := sugar.ToYdbParam(arg)
		if err != nil {
			panic(err)
		}
		declares[i] = fmt.Sprintf("DECLARE %s AS %s", param.Name(), param.Value().Type().String())
	}
	sort.Strings(declares)
	return declares
}

func Delete(tablePathPrefix, tableName string, key string) Request {
	args := []sql.NamedArg{
		sql.Named("key", key),
	}
	return request{
		query: render(deleteQuery, commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
			Declares:        asDeclares(args),
		}),
		args: asInterfaces(args),
	}
}

func DropTable(tablePathPrefix, tableName string) Request {
	return request{
		query: render(dropTableQuery, commonData{
			TablePathPrefix: tablePathPrefix,
			TableName:       tableName,
		}),
	}
}

type commonDataWithColumns struct {
	commonData
	Columns []string
}

func CreateTable(tablePathPrefix, tableName string, fieldsCount int) Request {
	columns := make([]string, fieldsCount)
	for i := 0; i < fieldsCount; i++ {
		columns[i] = "FIELD" + strconv.Itoa(i)
	}
	return request{
		query: render(createTableQuery, commonDataWithColumns{
			commonData{
				TablePathPrefix: tablePathPrefix,
				TableName:       tableName,
			},
			columns,
		}),
	}
}

type request struct {
	query string
	args  []interface{}
}

func (r request) Query() string {
	return r.query
}

func (r request) Args() []interface{} {
	return r.args
}

var _ Request = &request{}

type Request interface {
	Query() string
	Args() []interface{}
}

func asInterfaces(in []sql.NamedArg) (out []interface{}) {
	out = make([]interface{}, len(in))
	for i := range in {
		out[i] = in[i]
	}
	return out
}
