package sqly

import (
	"context"
	"reflect"
	"testing"

	_ "modernc.org/sqlite"
)

var (
	ctx = context.Background()
)

func withDB(t *testing.T, f func(db *DB)) {
	t.Helper()
	db, err := Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	f(db)

}

type testStruct struct {
	unexported int
	Uint       uint
	Uint8      uint8
	Uint16     uint16
	Uint32     uint32
	Uint64     uint64
	Int        int `sqly:"pkey,autoinc"`
	Int8       int8
	Int16      int16
	Int32      int32
	Int64      int64
	String     string
	Bool       bool
	Float32    float32
	Float64    float64
	Blob       []byte
}

type indexedTestStruct struct {
	Id            int `sqly:"pkey"`
	Indexed       int `sqly:"index"`
	Unique        int `sqly:"unique"`
	ThreeIndexed1 int
	ThreeIndexed2 int
	ThreeIndexed3 int `sqly:"indexWith(ThreeIndexed1;ThreeIndexed2)"`
	ThreeUnique1  int
	ThreeUnique2  int
	ThreeUnique3  int `sqly:"uniqueWith(ThreeUnique1;ThreeUnique2)"`
}

func yeserr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("got nil, wanted some error")
	}
}

func noerr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		if err, ok := err.(StackTracer); ok {
			t.Logf("%s", err.StackTrace())
		}
		t.Fatal(err)
	}
}

func TestCRU(t *testing.T) {
	withDB(t, func(db *DB) {
		noerr(t, db.CreateTableIfNotExists(ctx, testStruct{}))
		wantStr := &testStruct{
			unexported: 1,
			Uint:       2,
			Uint8:      3,
			Uint16:     4,
			Uint32:     5,
			Uint64:     6,
			Int8:       8,
			Int16:      9,
			Int32:      10,
			Int64:      11,
			String:     "12",
			Bool:       true,
			Float32:    13.0,
			Float64:    14.0,
			Blob:       []byte("15"),
		}
		noerr(t, db.Upsert(ctx, wantStr, false))
		if wantStr.Int == 0 {
			t.Fatal("wanted a new primary key, got 0")
		}
		gotStr := &testStruct{}
		noerr(t, db.Get(gotStr, "SELECT * FROM testStruct WHERE Int = ?", wantStr.Int))
		if gotStr.unexported != 0 {
			t.Errorf("got unexported %v, wanted 0", gotStr.unexported)
		}
		gotStr.unexported = wantStr.unexported
		if !reflect.DeepEqual(gotStr, wantStr) {
			t.Errorf("got %+v, wanted %+v", gotStr, wantStr)
		}
		yeserr(t, db.Upsert(ctx, wantStr, false))
		noerr(t, db.Upsert(ctx, wantStr, true))
	})
}

func TextIndex(t *testing.T) {
	withDB(t, func(db *DB) {
		noerr(t, db.CreateTableIfNotExists(ctx, testStruct{}))
		str := &indexedTestStruct{
			Indexed:       1,
			Unique:        2,
			ThreeIndexed1: 1,
			ThreeIndexed2: 2,
			ThreeIndexed3: 3,
			ThreeUnique1:  1,
			ThreeUnique2:  2,
			ThreeUnique3:  3,
		}
		noerr(t, db.Upsert(ctx, str, false))
		noerr(t, db.Upsert(ctx, &indexedTestStruct{
			Indexed: 1,
		}, false))
		yeserr(t, db.Upsert(ctx, &indexedTestStruct{
			Unique: 1,
		}, false))
		noerr(t, db.Upsert(ctx, &indexedTestStruct{
			Unique: 2,
		}, false))
		noerr(t, db.Upsert(ctx, &indexedTestStruct{
			ThreeIndexed1: 1,
			ThreeIndexed2: 2,
			ThreeIndexed3: 3,
		}, false))
		yeserr(t, db.Upsert(ctx, &indexedTestStruct{
			ThreeUnique1: 1,
			ThreeUnique2: 2,
			ThreeUnique3: 3,
		}, false))
		noerr(t, db.Upsert(ctx, &indexedTestStruct{
			ThreeUnique1: 2,
			ThreeUnique2: 2,
			ThreeUnique3: 3,
		}, false))
		noerr(t, db.Upsert(ctx, &indexedTestStruct{
			ThreeUnique1: 1,
			ThreeUnique2: 3,
			ThreeUnique3: 3,
		}, false))
		noerr(t, db.Upsert(ctx, &indexedTestStruct{
			ThreeUnique1: 1,
			ThreeUnique2: 2,
			ThreeUnique3: 4,
		}, false))
	})
}
