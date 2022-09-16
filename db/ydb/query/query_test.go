package query

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func splitAndSimplify(request Request) []string {
	ss := strings.Split(request.Query(), "\n")
	n := 0
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if len(s) > 0 {
			ss[n] = s
			n++
		}
	}
	ss = ss[:n]
	return ss
}

func TestScan(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		columns         []string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			columns:         nil,
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $key AS Utf8;",
				"DECLARE $limit AS Uint64;",
				"SELECT * FROM ycsbtable WHERE YCSB_KEY > $key LIMIT $limit;",
			},
		},
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			columns: []string{
				"col0",
				"col5",
				"col3",
				"col1",
				"col2",
				"col4",
			},
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $key AS Utf8;",
				"DECLARE $limit AS Uint64;",
				"SELECT `COL0`, `COL1`, `COL2`, `COL3`, `COL4`, `COL5` FROM ycsbtable WHERE YCSB_KEY > $key LIMIT $limit;",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(Scan(tt.tablePathPrefix, tt.tableName, tt.columns, "", 5))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestBatchDelete(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $keys AS List<Utf8>;",
				"DELETE FROM ycsbtable WHERE YCSB_KEY IN $keys;",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(BatchDelete(tt.tablePathPrefix, tt.tableName, []string{""}))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestBatchInsert(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $values AS List<Struct<key:Utf8,value:Uint64>>;",
				"UPSERT INTO ycsbtable SELECT * FROM AS_TABLE($values);",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(BatchInsert(tt.tablePathPrefix, tt.tableName, types.ListValue(
				types.StructValue(
					types.StructFieldValue("key", types.TextValue("")),
					types.StructFieldValue("value", types.Uint64Value(0)),
				),
			)))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestBatchRead(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		columns         []string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			columns:         nil,
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $keys AS List<Utf8>;",
				"SELECT * FROM ycsbtable WHERE YCSB_KEY IN $keys;",
			},
		},
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			columns: []string{
				"col0",
				"col5",
				"col3",
				"col1",
				"col2",
				"col4",
			},
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $keys AS List<Utf8>;",
				"SELECT `COL0`, `COL1`, `COL2`, `COL3`, `COL4`, `COL5` FROM ycsbtable WHERE YCSB_KEY IN $keys;",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(BatchRead(tt.tablePathPrefix, tt.tableName, tt.columns, []string{""}))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestBatchUpdate(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $values AS List<Struct<key:Utf8,value:Uint64>>;",
				"UPDATE ycsbtable ON SELECT * FROM AS_TABLE($values);",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(BatchUpdate(tt.tablePathPrefix, tt.tableName, types.ListValue(
				types.StructValue(
					types.StructFieldValue("key", types.TextValue("")),
					types.StructFieldValue("value", types.Uint64Value(0)),
				),
			)))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestCreateTable(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		columnsCount    int
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			columnsCount:    0,
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"CREATE TABLE ycsbtable (",
				"YCSB_KEY Text NOT NULL,",
				"PRIMARY KEY (YCSB_KEY)",
				");",
			},
		},
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			columnsCount:    2,
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"CREATE TABLE ycsbtable (",
				"YCSB_KEY Text NOT NULL,",
				"`FIELD0` Bytes,",
				"`FIELD1` Bytes,",
				"PRIMARY KEY (YCSB_KEY)",
				");",
			},
		},
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			columnsCount:    5,
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"CREATE TABLE ycsbtable (",
				"YCSB_KEY Text NOT NULL,",
				"`FIELD0` Bytes,",
				"`FIELD1` Bytes,",
				"`FIELD2` Bytes,",
				"`FIELD3` Bytes,",
				"`FIELD4` Bytes,",
				"PRIMARY KEY (YCSB_KEY)",
				");",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(CreateTable(tt.tablePathPrefix, tt.tableName, tt.columnsCount))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $key AS Utf8;",
				"DELETE FROM ycsbtable WHERE YCSB_KEY = $key;",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(Delete(tt.tablePathPrefix, tt.tableName, ""))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestDropTable(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DROP TABLE ycsbtable;",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(DropTable(tt.tablePathPrefix, tt.tableName))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestInsert(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $values AS List<Struct<key:Utf8,value:Uint64>>;",
				"INSERT INTO ycsbtable SELECT * FROM AS_TABLE($values);",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(Insert(tt.tablePathPrefix, tt.tableName, types.ListValue(
				types.StructValue(
					types.StructFieldValue("key", types.TextValue("")),
					types.StructFieldValue("value", types.Uint64Value(0)),
				),
			)))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestRead(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		columns         []string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			columns:         nil,
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $key AS Utf8;",
				"SELECT * FROM ycsbtable WHERE YCSB_KEY = $key;",
			},
		},
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			columns: []string{
				"col0",
				"col5",
				"col3",
				"col1",
				"col2",
				"col4",
			},
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $key AS Utf8;",
				"SELECT `COL0`, `COL1`, `COL2`, `COL3`, `COL4`, `COL5` FROM ycsbtable WHERE YCSB_KEY = $key;",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(Read(tt.tablePathPrefix, tt.tableName, tt.columns, ""))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	for _, tt := range []struct {
		tablePathPrefix string
		tableName       string
		exp             []string
	}{
		{
			tablePathPrefix: "/local",
			tableName:       "ycsbtable",
			exp: []string{
				"PRAGMA TablePathPrefix(\"/local\");",
				"DECLARE $values AS List<Struct<key:Utf8,value:Uint64>>;",
				"UPSERT INTO ycsbtable SELECT * FROM AS_TABLE($values);",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := splitAndSimplify(Update(tt.tablePathPrefix, tt.tableName, types.ListValue(
				types.StructValue(
					types.StructFieldValue("key", types.TextValue("")),
					types.StructFieldValue("value", types.Uint64Value(0)),
				),
			)))
			if !reflect.DeepEqual(got, tt.exp) {
				t.Errorf("got:\n\n`%v`\n\nwant:\n\n`%v`", got, tt.exp)
			}
		})
	}
}
