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

#include <iostream>

#include <arrow/acero/api.h>    // plans and nodes
#include <arrow/compute/api.h>  // field refs and exprs
#include <arrow/io/api.h>       // ReadableFile
#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>

namespace aio = ::arrow::io;
namespace cp = ::arrow::compute;
namespace ac = ::arrow::acero;

arrow::Status simple_acero(std::string path) {
  auto* pool = arrow::default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto input, aio::ReadableFile::Open(path));

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

  std::unique_ptr<arrow::RecordBatchReader> rdr;
  ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(&rdr));

  ac::Declaration reader_source("record_batch_reader_source",
                                ac::RecordBatchReaderSourceNodeOptions{std::move(rdr)});

  ac::Declaration project{
      "project",
      {std::move(reader_source)},
      ac::ProjectNodeOptions(
          {cp::field_ref("name"), cp::field_ref("species"), cp::field_ref("homeworld")})};

  ARROW_ASSIGN_OR_RAISE(auto result, ac::DeclarationToTable(std::move(project)));
  std::cout << "Results: " << result->ToString() << std::endl;
  return arrow::Status::OK();
}

arrow::Status complex_plan(std::string path) {
  auto* pool = arrow::default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto input, aio::ReadableFile::Open(path));

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

  std::unique_ptr<arrow::RecordBatchReader> rdr;
  ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(&rdr));

  ac::Declaration reader_source{"record_batch_reader_source",
                                ac::RecordBatchReaderSourceNodeOptions{std::move(rdr)}};

  ac::Declaration agg_plan{
      "aggregate",
      {std::move(reader_source)},
      ac::AggregateNodeOptions({{{"hash_list", nullptr, "name", "name_list"}}},
                               {"homeworld"})};

  ARROW_ASSIGN_OR_RAISE(auto result, ac::DeclarationToTable(std::move(agg_plan)));
  std::cout << "Results: " << result->ToString() << std::endl;
  return arrow::Status::OK();
}

arrow::Status sequence_plan(std::string path) {
  auto* pool = arrow::default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto input, aio::ReadableFile::Open(path));

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

  std::unique_ptr<arrow::RecordBatchReader> rdr;
  ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(&rdr));

  arrow::StringBuilder excl_bldr;
  ARROW_RETURN_NOT_OK(excl_bldr.Append("Skako"));
  ARROW_RETURN_NOT_OK(excl_bldr.Append("Utapau"));
  ARROW_RETURN_NOT_OK(excl_bldr.Append("Nal Hutta"));
  std::shared_ptr<arrow::StringArray> exclusions;
  ARROW_RETURN_NOT_OK(excl_bldr.Finish(&exclusions));

  auto filter_expr = cp::call("invert", {cp::call("is_in", {cp::field_ref("homeworld")},
                                                  cp::SetLookupOptions{*exclusions})});

  auto plan = ac::Declaration::Sequence(
      {{"record_batch_reader_source",
        ac::RecordBatchReaderSourceNodeOptions{std::move(rdr)}},
       {"filter", ac::FilterNodeOptions{std::move(filter_expr)}},
       {"aggregate",
        ac::AggregateNodeOptions({{{"hash_list", nullptr, "name", "name_list"},
                                   {"hash_list", nullptr, "species", "species_list"},
                                   {"hash_mean", nullptr, "height", "avg_height"}}},
                                 {"homeworld"})}});
  
  ARROW_ASSIGN_OR_RAISE(auto result, ac::DeclarationToTable(std::move(plan)));
  std::cout << "Results: " << result->ToString() << std::endl;
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  // ARROW_UNUSED(simple_acero("../../sample_data/starwars.parquet"));
  // ARROW_UNUSED(complex_plan("../../sample_data/starwars.parquet"));
  ARROW_UNUSED(sequence_plan("../../sample_data/starwars.parquet"));
}