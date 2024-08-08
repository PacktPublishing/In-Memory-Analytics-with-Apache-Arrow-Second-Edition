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
	"compress/gzip"
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/csv"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	if err != nil {
		panic(err)
	}

	client := s3.NewFromConfig(cfg)
	obj, err := client.GetObject(context.Background(),
		&s3.GetObjectInput{
			Bucket: aws.String("dataforgood-fb-data"),
			Key:    aws.String("csv/month=2019-06/country=ZWE/type=children_under_five/ZWE_children_under_five.csv.gz")})
	if err != nil {
		panic(err)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "latitude",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "longitude",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "population",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
	}, nil)

	rdr, err := gzip.NewReader(obj.Body)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	reader := csv.NewReader(rdr, schema, csv.WithChunk(500000),
		csv.WithHeader(true), csv.WithComma('\t'))
	defer reader.Release()

	w, _ := os.Create("tripdata.arrow")
	writer := ipc.NewWriter(w, ipc.WithSchema(reader.Schema()))
	defer writer.Close()

	for reader.Next() {
		if err := writer.Write(reader.Record()); err != nil {
			break
		}
	}

	if reader.Err() != nil {
		fmt.Println(reader.Err())
	}
}
