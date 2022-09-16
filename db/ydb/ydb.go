// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ydb

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/db/ydb/query"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	// ydb package
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// ydb properties
const (
	ydbDSN = "ydb.dsn"
)

type ydbCreator struct {
}

type ydbDB struct {
	p       *properties.Properties
	db      *sql.DB
	verbose bool

	databasePath string

	bufPool *util.BufPool
}

func (db *ydbDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *ydbDB) CleanupThread(_ context.Context) {

}

func (c ydbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(ydbDB)
	d.p = p

	dsn := p.GetString(ydbDSN, "grpcs://localhost:2135/local")

	var err error
	db, err := sql.Open("ydb", dsn)
	if err != nil {
		return nil, err
	}

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	db.SetMaxIdleConns(threadCount + 1)
	db.SetMaxOpenConns(threadCount * 2)

	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.db = db

	d.bufPool = util.NewBufPool()

	cc, err := ydb.Unwrap(d.db)
	if err != nil {
		return nil, err
	}

	d.databasePath = cc.Name()

	if err = d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *ydbDB) createTable() (err error) {
	ctx := context.Background()

	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) {
		_ = db.execQuery(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			query.DropTable(db.databasePath, tableName),
		)
	}

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)

	return db.execQuery(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		query.CreateTable(db.databasePath, tableName, int(fieldCount)),
	)
}

func (db *ydbDB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

func (db *ydbDB) Read(ctx context.Context, tableName string, key string, columns []string) (map[string][]byte, error) {
	rows, err := db.queryRows(ctx, query.Read(db.databasePath, tableName, columns, key))

	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *ydbDB) BatchRead(ctx context.Context, tableName string, keys []string, columns []string) ([]map[string][]byte, error) {
	return db.queryRows(ydb.WithQueryMode(ctx, ydb.ScanQueryMode),
		query.BatchRead(db.databasePath, tableName, columns, keys),
	)
}

func (db *ydbDB) Scan(ctx context.Context, tableName string, startKey string, count int, columns []string) ([]map[string][]byte, error) {
	return db.queryRows(ydb.WithQueryMode(ctx, ydb.ScanQueryMode),
		query.Scan(db.databasePath, tableName, columns, startKey, uint64(count)),
	)
}

func (db *ydbDB) execQuery(ctx context.Context, request query.Request) (err error) {
	if db.verbose {
		fmt.Printf("%s %v\n", request.Query(), request.Args())
	}
	err = retry.Do(ctx, db.db, func(ctx context.Context, cc *sql.Conn) error {
		_, err = cc.ExecContext(ctx, request.Query(), request.Args()...)
		return err
	})
	if err != nil {
		log.Println(err)
	}
	return err
}

func (db *ydbDB) queryRows(ctx context.Context, request query.Request) (vs []map[string][]byte, _ error) {
	if db.verbose {
		fmt.Printf("%s %v\n", request.Query(), request.Args())
	}
	err := retry.Do(ctx, db.db, func(ctx context.Context, cc *sql.Conn) error {
		rows, err := cc.QueryContext(ctx, request.Query(), request.Args()...)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()

		cols, err := rows.Columns()
		if err != nil {
			return err
		}

		vs = make([]map[string][]byte, 0)
		for rows.Next() {
			m := make(map[string][]byte, len(cols))
			dest := make([]interface{}, len(cols))
			for i := 0; i < len(cols); i++ {
				v := new([]byte)
				dest[i] = v
			}
			if err = rows.Scan(dest...); err != nil {
				return err
			}

			for i, v := range dest {
				m[cols[i]] = *v.(*[]byte)
			}

			vs = append(vs, m)
		}

		return rows.Err()
	})
	if err != nil {
		log.Println(err)
	}
	return vs, err
}

func (db *ydbDB) Update(ctx context.Context, tableName string, key string, row map[string][]byte) error {
	fields := make([]types.StructValueOption, 0, len(row)+1)
	fields = append(fields, types.StructFieldValue("YCSB_KEY", types.TextValue(key)))
	for field, value := range row {
		fields = append(fields, types.StructFieldValue(strings.ToUpper(field), types.BytesValue(value)))
	}
	return db.execQuery(ctx, query.Update(db.databasePath, tableName, types.ListValue(types.StructValue(fields...))))
}

func (db *ydbDB) BatchUpdate(ctx context.Context, tableName string, keys []string, values []map[string][]byte) error {
	columns := make(map[string]struct{}, 0)
	for _, kv := range values {
		for k, v := range kv {
			delete(kv, k)
			k = strings.ToUpper(k)
			kv[k] = v
			columns[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(columns))
	for k := range columns {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	ydbRows := make([]types.Value, 0, len(values))
	for i, key := range keys {
		row := values[i]
		fields := make([]types.StructValueOption, 0, len(cols)+1)
		fields = append(fields, types.StructFieldValue("YCSB_KEY", types.TextValue(key)))
		for _, column := range cols {
			if value, has := row[column]; has {
				fields = append(fields, types.StructFieldValue(column, types.NullableBytesValue(&value)))
			} else {
				fields = append(fields, types.StructFieldValue(column, types.NullableBytesValue(nil)))
			}
		}
		ydbRows = append(ydbRows, types.StructValue(fields...))
	}
	return db.execQuery(ctx, query.BatchUpdate(db.databasePath, tableName, types.ListValue(ydbRows...)))
}

func (db *ydbDB) Insert(ctx context.Context, tableName string, key string, row map[string][]byte) error {
	fields := make([]types.StructValueOption, 0, len(row)+1)
	fields = append(fields, types.StructFieldValue("YCSB_KEY", types.TextValue(key)))
	for field, value := range row {
		fields = append(fields, types.StructFieldValue(strings.ToUpper(field), types.BytesValue(value)))
	}
	return db.execQuery(ctx, query.Insert(db.databasePath, tableName, types.ListValue(types.StructValue(fields...))))
}

func (db *ydbDB) BatchInsert(ctx context.Context, tableName string, keys []string, values []map[string][]byte) error {
	columns := make(map[string]struct{}, 0)
	for _, kv := range values {
		for k, v := range kv {
			delete(kv, k)
			k = strings.ToUpper(k)
			kv[k] = v
			columns[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(columns))
	for k := range columns {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	ydbRows := make([]types.Value, 0, len(values))
	for i, key := range keys {
		row := values[i]
		fields := make([]types.StructValueOption, 0, len(cols)+1)
		fields = append(fields, types.StructFieldValue("YCSB_KEY", types.TextValue(key)))
		for _, column := range cols {
			if value, has := row[column]; has {
				fields = append(fields, types.StructFieldValue(column, types.NullableBytesValue(&value)))
			} else {
				fields = append(fields, types.StructFieldValue(column, types.NullableBytesValue(nil)))
			}
		}
		ydbRows = append(ydbRows, types.StructValue(fields...))
	}
	return db.execQuery(ctx, query.BatchInsert(db.databasePath, tableName, types.ListValue(ydbRows...)))
}

func (db *ydbDB) Delete(ctx context.Context, tableName string, key string) error {
	return db.execQuery(ctx, query.Delete(db.databasePath, tableName, key))
}

func (db *ydbDB) BatchDelete(ctx context.Context, tableName string, keys []string) error {
	return db.execQuery(ctx, query.BatchDelete(db.databasePath, tableName, keys))
}

func init() {
	ycsb.RegisterDBCreator("ydb", ydbCreator{})
}
