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

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>

#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <vector>

#define ABORT_NOT_OK(expr)                                          \
  do {                                                              \
    auto _res = (expr);                                             \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res); \
    if (!_st.ok()) {                                                \
      _st.Abort();                                                  \
    }                                                               \
  } while(false);

void first_example() {
  std::vector<int64_t> data{1, 2, 3, 4};
  auto arr = std::make_shared<arrow::Int64Array>(data.size(), arrow::Buffer::Wrap(data));
  std::cout << arr->ToString() << std::endl;
}

void random_data_example() {
  std::random_device rd;
  std::mt19937 gen{rd()};
  std::normal_distribution<> d{5, 2};

  auto pool = arrow::default_memory_pool();
  arrow::DoubleBuilder builder{arrow::float64(), pool};

  constexpr auto ncols = 16;
  constexpr auto nrows = 8192;
  arrow::ArrayVector columns(ncols);
  arrow::FieldVector fields;
  for (int i = 0; i < ncols; ++i) {
    for (int j = 0; j < nrows; ++j) {
      ABORT_NOT_OK(builder.Append(d(gen)));
    }
    auto status = builder.Finish(&columns[i]);
    if (!status.ok()) {
      std::cerr << status.message() << std::endl;
      // do something!
      return;
    }
    fields.push_back(arrow::field("c" + std::to_string(i), arrow::float64()));
  }

  auto rb =
      arrow::RecordBatch::Make(arrow::schema(fields), columns[0]->length(), columns);
  std::cout << rb->ToString() << std::endl;
}

void building_struct_array() {
  using arrow::field;
  using arrow::int16;
  using arrow::utf8;
  arrow::ArrayVector children;

  std::vector<std::string> archers{"Legolas", "Oliver", "Merida", "Lara", "Artemis"};
  std::vector<std::string> locations{"Murkwood", "Star City", "Scotland", "London",
                                     "Greece"};
  std::vector<int16_t> years{1954, 1941, 2012, 1996, -600};

  children.resize(3);  
  arrow::StringBuilder str_bldr;
  ABORT_NOT_OK(str_bldr.AppendValues(archers));
  ABORT_NOT_OK(str_bldr.Finish(&children[0]));
  ABORT_NOT_OK(str_bldr.AppendValues(locations));
  ABORT_NOT_OK(str_bldr.Finish(&children[1]));
  arrow::Int16Builder year_bldr;
  ABORT_NOT_OK(year_bldr.AppendValues(years));
  ABORT_NOT_OK(year_bldr.Finish(&children[2]));

  arrow::StructArray arr{
      arrow::struct_(
          {field("archer", utf8()), field("location", utf8()), field("year", int16())}),
      children[0]->length(), children};
  std::cout << arr.ToString() << std::endl;
}

void build_struct_builder() {
  using arrow::field;
  std::shared_ptr<arrow::DataType> st_type =
      arrow::struct_({field("archer", arrow::utf8()), field("location", arrow::utf8()),
                      field("year", arrow::int16())});

  std::unique_ptr<arrow::ArrayBuilder> tmp;
  ABORT_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(), st_type, &tmp));
  std::shared_ptr<arrow::StructBuilder> builder;
  builder.reset(static_cast<arrow::StructBuilder*>(tmp.release()));

  using namespace arrow;
  StringBuilder* archer_builder = static_cast<StringBuilder*>(builder->field_builder(0));
  StringBuilder* location_builder =
      static_cast<StringBuilder*>(builder->field_builder(1));
  Int16Builder* year_builder = static_cast<Int16Builder*>(builder->field_builder(2));

  std::vector<std::string> archers{"Legolas", "Oliver", "Merida", "Lara", "Artemis"};
  std::vector<std::string> locations{"Murkwood", "Star City", "Scotland", "London",
                                     "Greece"};
  std::vector<int16_t> years{1954, 1941, 2012, 1996, -600};

  for (int i = 0; i < archers.size(); ++i) {
    ABORT_NOT_OK(builder->Append());
    ABORT_NOT_OK(archer_builder->Append(archers[i]));
    ABORT_NOT_OK(location_builder->Append(locations[i]));
    ABORT_NOT_OK(year_builder->Append(years[i]));
  }

  std::shared_ptr<arrow::Array> out;
  ABORT_NOT_OK(builder->Finish(&out));  // ignoring status
  std::cout << out->ToString() << std::endl;
}

struct data_row {
  int64_t id;
  int64_t components;
  std::vector<double> component_cost;
};

arrow::Result<std::shared_ptr<arrow::Table>> vector_to_columnar(
    const std::vector<data_row>& rows) {
  using arrow::DoubleBuilder;
  using arrow::Int64Builder;
  using arrow::ListBuilder;
  using arrow::RecordBatchBuilder;

  // the builders are more efficient since they use the memory pools
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  auto schema = arrow::schema(
      {arrow::field("id", arrow::int64()), arrow::field("components", arrow::int64()),
       arrow::field("component_cost", arrow::list(arrow::float64()))});

  ARROW_ASSIGN_OR_RAISE(auto record_bldr, RecordBatchBuilder::Make(schema, pool));
  auto id_bldr = record_bldr->GetFieldAs<Int64Builder>(0);
  auto comp_bldr = record_bldr->GetFieldAs<Int64Builder>(1);
  auto comp_cost_bldr = record_bldr->GetFieldAs<ListBuilder>(2);
  auto comp_item_cost_bldr =
      (static_cast<DoubleBuilder*>(comp_cost_bldr->value_builder()));

  // just loop over existing data and insert it, check return values in case
  // we can't allocate enough additional memory
  for (const auto& row : rows) {
    ARROW_RETURN_NOT_OK(id_bldr->Append(row.id));
    ARROW_RETURN_NOT_OK(comp_bldr->Append(row.components));

    // start a new list
    ARROW_RETURN_NOT_OK(comp_cost_bldr->Append());
    // add actual values
    ARROW_RETURN_NOT_OK(comp_item_cost_bldr->AppendValues(row.component_cost.data(),
                                                          row.component_cost.size()));
  }

  ARROW_ASSIGN_OR_RAISE(auto rec, record_bldr->Flush());
  return arrow::Table::FromRecordBatches({rec});
}

arrow::Result<std::vector<data_row>> columnar_to_vector(
    const std::shared_ptr<arrow::Table>& table) {
  // first check the table conforms to our expected schema, then we can
  // build up the vector of rows incrementally
  auto expected_schema = arrow::schema(
      {arrow::field("id", arrow::int64()), arrow::field("components", arrow::int64()),
       arrow::field("component_cost", arrow::list(arrow::float64()))});

  if (!expected_schema->Equals(*table->schema())) {
    return arrow::Status::Invalid("Schemas do not match!");
  }

  // now we can just unpack everything
  auto ids = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
  auto comps = std::static_pointer_cast<arrow::Int64Array>(table->column(1)->chunk(0));
  auto comp_cost = std::static_pointer_cast<arrow::ListArray>(table->column(2)->chunk(0));
  auto comp_cost_values =
      std::static_pointer_cast<arrow::DoubleArray>(comp_cost->values());

  // to enable zero-copy slices, we would need to account for the slicing offset. Since
  // we're going to use the native raw values pointer, we have to handle this ourselves.
  // If we were just using Value(...), then it would already account for offsets
  // internally. for this example, we're assuming an offset of 0.
  const double* ccv_ptr = comp_cost_values->raw_values();
  std::vector<data_row> rows;
  for (int64_t i = 0; i < table->num_rows(); i++) {
    // we're also going to assume there are no nulls in this table for this example
    // otherwise we'd need to also check IsValid/IsNull or optimize by leveraging
    // the validity bitmap.
    int64_t id = ids->Value(i);
    int64_t comp_val = comps->Value(i);
    const double* first = ccv_ptr + comp_cost->value_offset(i);
    const double* last = ccv_ptr + comp_cost->value_offset(i + 1);
    std::vector<double> comp_vec(first, last);
    rows.push_back({id, comp_val, comp_vec});
  }

  return rows;
}

void run_row_conversions() {
  std::vector<data_row> orig = {
      {1, 1, {10.0}}, {2, 3, {11.0, 12.0, 13.0}}, {3, 2, {15.0, 25.0}}};
  std::shared_ptr<arrow::Table> table;
  std::vector<data_row> converted_rows;

  table = vector_to_columnar(orig).ValueOrDie();
  converted_rows = columnar_to_vector(table).ValueOrDie();
  assert(orig.size() == converted_rows.size());

  // Print out contents of table, should get
  // ID Components Component prices
  // 1  1          10
  // 2  3          11  12  13
  // 3  2          15  25
  std::cout << std::left << std::setw(3) << "ID " << std::left << std::setw(11)
            << "Components " << std::left << std::setw(15) << "Component prices "
            << std::endl;
  for (const auto& row : converted_rows) {
    std::cout << std::left << std::setw(3) << row.id << std::left << std::setw(11)
              << row.components;
    for (const auto& cost : row.component_cost) {
      std::cout << std::left << std::setw(4) << cost;
    }
    std::cout << std::endl;
  }
}

int main(int argc, char** argv) {
  first_example();
  random_data_example();
  building_struct_array();
  build_struct_builder();
  run_row_conversions();
}