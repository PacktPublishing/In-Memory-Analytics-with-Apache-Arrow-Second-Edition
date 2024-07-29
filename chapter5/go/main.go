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

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/scalar"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

const filepath = "../../sample_data/yellow_tripdata_2015-01.parquet"

func main() {
	f, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	tbl, err := pqarrow.ReadTable(context.Background(), f,
		parquet.NewReaderProperties(memory.DefaultAllocator),
		pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		panic(err)
	}
	defer tbl.Release()

	idx := tbl.Schema().FieldIndices("total_amount")[0]
	col := tbl.Column(idx)

	result, err := compute.Add(context.Background(), compute.ArithmeticOptions{},
		compute.NewDatumWithoutOwning(col.Data()),
		&compute.ScalarDatum{Value: scalar.MakeScalar(5.5)})
	if err != nil {
		panic(err)
	}
	defer result.Release()

	// just print the first 100 values of the column and the incremented version
	slice := array.NewSlice(col.Data().Chunk(0), 0, 100)
	sliceInc := array.NewSlice(result.(*compute.ChunkedDatum).Value.Chunk(0), 0, 100)
	fmt.Println(slice)
	fmt.Println(sliceInc)
	slice.Release()
	sliceInc.Release()
}
