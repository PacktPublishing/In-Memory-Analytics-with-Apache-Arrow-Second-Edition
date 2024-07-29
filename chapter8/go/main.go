// MIT License
//
// Copyright (c) 2021 Packt
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

func stdinterface() {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver": "adbc_driver_sqlite",
		"uri":    "file:data.db",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	ctx := context.Background()
	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer cnxn.Close()

	sc, err := cnxn.GetTableSchema(ctx, nil, nil, "foo")
	if err != nil {
		panic(err)
	}
	fmt.Println(sc)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	stmt.SetSqlQuery("SELECT * FROM foo")
	rdr, n, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		panic(err)
	}
	defer rdr.Release()

	fmt.Println(n)

	for rdr.Next() {
		fmt.Println(rdr.Record())
	}
}

func postgresToDuckdb() {
	ctx := context.Background()

	rdr, err := file.OpenParquetFile("/home/zeroshade/Downloads/yellow_tripdata_2015-01.parquet", false)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	pqrdr, err := pqarrow.NewFileReader(rdr,
		pqarrow.ArrowReadProperties{
			Parallel:  true,
			BatchSize: 102400},
		memory.DefaultAllocator)
	if err != nil {
		panic(err)
	}

	colIndices := make([]int, 0)
	for _, f := range pqrdr.Manifest.Fields {
		if f.Field.Type.ID() != arrow.NULL {
			colIndices = append(colIndices, f.ColIndex)
		}
	}

	recrdr, err := pqrdr.GetRecordReader(ctx, colIndices, nil)
	if err != nil {
		panic(err)
	}
	defer recrdr.Release()

	var pgDrv drivermgr.Driver
	db, err := pgDrv.NewDatabase(map[string]string{
		"driver":     "duckdb",
		"entrypoint": "duckdb_adbc_init",
		"path":       "test.db",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	// stmt.SetSqlQuery("SELECT * FROM taxisample")
	// results, n, err := stmt.ExecuteQuery(ctx)

	stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace)
	stmt.SetOption(adbc.OptionKeyIngestTargetTable, "taxisample")

	if err := stmt.BindStream(ctx, recrdr); err != nil {
		panic(err)
	}

	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println(n)
}

func populateDuckdb(db adbc.Database) {
	ctx := context.Background()

	rdr, err := file.OpenParquetFile("/home/zeroshade/Downloads/yellow_tripdata_2015-01.parquet", false)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	pqrdr, err := pqarrow.NewFileReader(rdr,
		pqarrow.ArrowReadProperties{
			Parallel:  true,
			BatchSize: 102400},
		memory.DefaultAllocator)
	if err != nil {
		panic(err)
	}

	colIndices := make([]int, 0)
	for _, f := range pqrdr.Manifest.Fields {
		if f.Field.Type.ID() != arrow.NULL {
			colIndices = append(colIndices, f.ColIndex)
		}
	}

	recrdr, err := pqrdr.GetRecordReader(ctx, colIndices, nil)
	if err != nil {
		panic(err)
	}
	defer recrdr.Release()

	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace)
	stmt.SetOption(adbc.OptionKeyIngestTargetTable, "taxisample")

	if err := stmt.BindStream(ctx, recrdr); err != nil {
		panic(err)
	}

	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println(n)
}

func sqlInterface() {
	sql.Register("adbc", sqldriver.Driver{Driver: &drivermgr.Driver{}})

	db, err := sql.Open("adbc", "driver=adbc_driver_sqlite")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT ?", 1)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			panic(err)
		}
		fmt.Println(v)
	}
}

func main() {
	var duckDrv drivermgr.Driver
	db, err := duckDrv.NewDatabase(map[string]string{
		"driver":     "duckdb",
		"entrypoint": "duckdb_adbc_init",
		"path":       "test.db",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// sfDrv := snowflake.NewDriver(memory.DefaultAllocator)
	// sfDb, err := sfDrv.NewDatabase(map[string]string{})

	stdinterface()
	postgresToDuckdb()
	sqlInterface()
}
