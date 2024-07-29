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
	"fmt"
	"math/rand"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func simple_example() {
	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.AppendValues([]int64{1, 2, 3, 4}, nil)
	arr := bldr.NewArray()
	defer arr.Release()
	fmt.Println(arr)
}

func random_example() {
	fltBldr := array.NewFloat64Builder(memory.DefaultAllocator)
	defer fltBldr.Release()
	const ncols = 16
	columns := make([]arrow.Array, ncols)
	fields := make([]arrow.Field, ncols)
	const nrows = 8192
	for i := range columns {
		for j := 0; j < nrows; j++ {
			fltBldr.Append(rand.Float64())
		}
		columns[i] = fltBldr.NewArray()
		defer columns[i].Release()
		fields[i] = arrow.Field{
			Name: "c" + strconv.Itoa(i),
			Type: arrow.PrimitiveTypes.Float64}
	}
	record := array.NewRecord(arrow.NewSchema(fields, nil),
		columns, nrows)
	defer record.Release()
	fmt.Println(record)
}

func struct_example() {
	archerType := arrow.StructOf(
		arrow.Field{Name: "archer", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "location", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "year", Type: arrow.PrimitiveTypes.Int16},
	)

	archers := []string{"Legolas", "Oliver", "Merida", "Lara", "Artemis"}
	locations := []string{"Murkwood", "Star City", "Scotland", "London", "Greece"}
	years := []int16{1954, 1941, 2012, 1996, -600}

	mem := memory.DefaultAllocator
	strBldr := array.NewStringBuilder(mem)
	defer strBldr.Release()
	yearBldr := array.NewInt16Builder(mem)
	defer yearBldr.Release()

	strBldr.AppendValues(archers, nil)
	names := strBldr.NewArray()
	defer names.Release()
	strBldr.AppendValues(locations, nil)
	locArr := strBldr.NewArray()
	defer locArr.Release()
	yearBldr.AppendValues(years, nil)
	yearArr := yearBldr.NewArray()
	defer yearArr.Release()

	data := array.NewData(archerType, names.Len(), []*memory.Buffer{nil},
		[]arrow.ArrayData{names.Data(), locArr.Data(), yearArr.Data()}, 0, 0)
	defer data.Release()
	archerArr := array.NewStructData(data)
	defer archerArr.Release()
	fmt.Println(archerArr)
}

func struct_example_struct_builder() {
	archerList := []struct {
		archer   string
		location string
		year     int16
	}{
		{"Legolas", "Murkwood", 1954},
		{"Oliver", "Star City", 1941},
		{"Merida", "Scotland", 2012},
		{"Lara", "London", 1996},
		{"Artemis", "Greece", -600},
	}

	archerType := arrow.StructOf(
		arrow.Field{Name: "archer", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "location", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "year", Type: arrow.PrimitiveTypes.Int16},
	)

	bldr := array.NewStructBuilder(memory.DefaultAllocator, archerType)
	defer bldr.Release()
	f1b := bldr.FieldBuilder(0).(*array.StringBuilder)
	f2b := bldr.FieldBuilder(1).(*array.StringBuilder)
	f3b := bldr.FieldBuilder(2).(*array.Int16Builder)
	for _, ar := range archerList {
		bldr.Append(true)
		f1b.Append(ar.archer)
		f2b.Append(ar.location)
		f3b.Append(ar.year)
	}
	archers := bldr.NewStructArray()
	defer archers.Release()
	fmt.Println(archers)
}

type DataRow struct {
	ID            int64
	Component     int64
	ComponentCost []float64
}

func VectorToColumnar(rows []DataRow) arrow.Table {
	pool := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "components", Type: arrow.PrimitiveTypes.Int64},
		{Name: "component_cost", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)},
	}, nil)

	recBldr := array.NewRecordBuilder(pool, schema)
	defer recBldr.Release()

	idBldr := recBldr.Field(0).(*array.Int64Builder)
	compBldr := recBldr.Field(1).(*array.Int64Builder)
	compCostBldr := recBldr.Field(2).(*array.ListBuilder)
	compItemCostBldr := compCostBldr.ValueBuilder().(*array.Float64Builder)

	for _, r := range rows {
		idBldr.Append(r.ID)
		compBldr.Append(r.Component)
		compCostBldr.Append(true)
		compItemCostBldr.AppendValues(r.ComponentCost, nil)
	}

	rec := recBldr.NewRecord()
	defer rec.Release()

	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func ColumnarToVector(tbl arrow.Table) []DataRow {
	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "components", Type: arrow.PrimitiveTypes.Int64},
		{Name: "component_cost", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)},
	}, nil)

	if !expectedSchema.Equal(tbl.Schema()) {
		panic("schemas do not match!")
	}

	ids := tbl.Column(0).Data().Chunk(0).(*array.Int64)
	comps := tbl.Column(1).Data().Chunk(0).(*array.Int64)
	compCost := tbl.Column(2).Data().Chunk(0).(*array.List)
	compCostValues := compCost.ListValues().(*array.Float64)

	ccvRaw := compCostValues.Float64Values()
	out := make([]DataRow, 0, tbl.NumRows())
	for i := 0; i < int(tbl.NumRows()); i++ {
		start, end := compCost.ValueOffsets(i)

		out = append(out, DataRow{
			ID:            ids.Value(i),
			Component:     comps.Value(i),
			ComponentCost: ccvRaw[start:end],
		})
	}

	return out
}

func runRowConversions() {
	orig := []DataRow{
		{1, 1, []float64{10}},
		{2, 3, []float64{11, 12, 13}},
		{3, 2, []float64{15, 25}},
	}

	tbl := VectorToColumnar(orig)
	defer tbl.Release()

	converted := ColumnarToVector(tbl)
	if len(converted) != int(tbl.NumRows()) {
		panic("mismatched number of rows")
	}

	fmt.Println(tbl)
	fmt.Println(converted)
}

func main() {
	simple_example()
	random_example()
	struct_example()
	struct_example_struct_builder()
	runRowConversions()
}
