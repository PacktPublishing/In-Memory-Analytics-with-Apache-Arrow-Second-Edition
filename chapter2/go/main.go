// MIT License
//
// Copyright (c) 2024 Packt
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
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
	"fmt"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/csv"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wolfeidau/s3iofs"
)

func readParquet() {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"))
	if err != nil {
		panic(err)
	}

	client := s3.NewFromConfig(cfg)
	s3fs := s3iofs.NewWithClient("nyc-tlc", client)

	f, err := s3fs.Open("trip data/yellow_tripdata_2015-01.parquet")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	tbl, err := pqarrow.ReadTable(context.Background(),
		f.(parquet.ReaderAtSeeker), nil,
		pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		panic(err)
	}
	defer tbl.Release()

	fmt.Println(tbl.NumCols())
	fmt.Println(tbl.NumRows())

}

func readCSV() {
	f, err := os.Open("../../sample_data/sliced.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	rdr := csv.NewInferringReader(f, csv.WithHeader(true),
		csv.WithChunk(50000), // 50k rows per record adjust to adjust performance
		csv.WithColumnTypes(map[string]arrow.DataType{
			"extra":        arrow.PrimitiveTypes.Float64,
			"fare_amount":  arrow.PrimitiveTypes.Float64,
			"tolls_amount": arrow.PrimitiveTypes.Float64,
		}))
	defer rdr.Release()

	// call rdr.Next once so that it runs the schema inferring
	if !rdr.Next() {
		panic("no data")
	}

	w, _ := os.Create("tripdata.arrow")
	writer := ipc.NewWriter(w, ipc.WithSchema(rdr.Schema()))
	defer writer.Close()

	if err := writer.Write(rdr.Record()); err != nil {
		panic(err)
	}

	for rdr.Next() {
		if err := writer.Write(rdr.Record()); err != nil {
			panic(err)
		}
	}

	if rdr.Err() != nil {
		panic(rdr.Err())
	}
}

func main() {
	readParquet()
	readCSV()
}
