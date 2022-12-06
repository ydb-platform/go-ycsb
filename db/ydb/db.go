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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/magiconair/properties"

	// ydb package
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
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

	bufPool *util.BufPool
}

func (c ydbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(ydbDB)
	d.p = p

	dsn := p.GetString(ydbDSN, "grpc://localhost:2136/local")

	var err error
	db, err := sql.Open("ydb", dsn)
	if err != nil {
		fmt.Printf("open ydb failed %v", err)
		return nil, err
	}

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	db.SetMaxIdleConns(threadCount + 1)
	db.SetMaxOpenConns(threadCount * 2)

	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.db = db

	d.bufPool = util.NewBufPool()

	if err := d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *ydbDB) createTable() error {
	ctx := ydb.WithQueryMode(context.Background(), ydb.SchemeQueryMode)
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) {
		_, _ = db.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", tableName))
	}

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE %s (YCSB_KEY Text NOT NULL", tableName)
	buf.WriteString(s)

	for i := int64(0); i < fieldCount; i++ {
		buf.WriteString(fmt.Sprintf(", FIELD%d Bytes", i))
	}

	buf.WriteString(", PRIMARY KEY (YCSB_KEY)")
	buf.WriteString(");")

	if db.verbose {
		fmt.Println(buf.String())
	}

	_, err := db.db.ExecContext(ctx, buf.String())
	return err
}

func (db *ydbDB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

func (db *ydbDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *ydbDB) CleanupThread(ctx context.Context) {
}

func (db *ydbDB) queryRows(ctx context.Context, query string, count int, args ...interface{}) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	vs := make([]map[string][]byte, 0, count)
	for rows.Next() {
		m := make(map[string][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			v := new([]byte)
			dest[i] = v
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}

		for i, v := range dest {
			m[cols[i]] = *v.(*[]byte)
		}

		vs = append(vs, m)
	}

	return vs, rows.Err()
}

func (db *ydbDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	query := "DECLARE $key AS Text;"
	if len(fields) == 0 {
		query += fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY = $key`, table)
	} else {
		query += fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY = $key`, strings.Join(fields, ","), table)
	}

	rows, err := db.queryRows(ctx, query, 1, sql.Named("key", key))
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *ydbDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	query := "DECLARE $key AS Text; DECLARE $limit AS Uint64;"
	if len(fields) == 0 {
		query += fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY >= $key LIMIT $limit`, table)
	} else {
		query += fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY >= $key LIMIT $limit`, strings.Join(fields, ","), table)
	}

	rows, err := db.queryRows(ctx, query, count, sql.Named("key", startKey), sql.Named("limit", count))
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (db *ydbDB) execQuery(ctx context.Context, query string, args ...interface{}) error {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	_, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	return nil
}

func (db *ydbDB) upsert(ctx context.Context, table string, key string, values map[string][]byte) error {
	args := make([]interface{}, 0, 1+len(values))
	args = append(args, sql.Named("key", key))

	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("DECLARE $key AS Text; ")
	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, sql.Named(p.Field, p.Value))
		buf.WriteString("DECLARE $")
		buf.WriteString(p.Field)
		buf.WriteString(" AS Bytes; ")
	}

	buf.WriteString("UPSERT INTO ")
	buf.WriteString(table)
	buf.WriteString(" (YCSB_KEY")
	for _, p := range pairs {
		buf.WriteString(" ,")
		buf.WriteString(strings.ToUpper(p.Field))
	}
	buf.WriteString(") VALUES ($key")

	for _, p := range pairs {
		buf.WriteString(fmt.Sprintf(" ,$%s", p.Field))
	}

	buf.WriteString(")")

	return db.execQuery(ctx, buf.String(), args...)
}

func (db *ydbDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.upsert(ctx, table, key, values)
}

func (db *ydbDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.upsert(ctx, table, key, values)
}

func (db *ydbDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf(`DECLARE $key AS Text; DELETE FROM %s WHERE YCSB_KEY = $key`, table)

	return db.execQuery(ctx, query, sql.Named("key", key))
}

func init() {
	ycsb.RegisterDBCreator("ydb", ydbCreator{})
}
