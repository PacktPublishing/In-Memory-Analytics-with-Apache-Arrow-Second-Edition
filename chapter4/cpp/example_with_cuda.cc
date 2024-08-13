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

#include <cudf/column/column_factories.hpp>
#include <cudf/groupby.hpp>
#include <cudf/interop.hpp>
#include <cudf/reduction.hpp>

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_device.hpp"

extern "C" {
void get_sum(struct ArrowSchema*, struct ArrowDeviceArray*, struct ArrowSchema*,
             struct ArrowDeviceArray*);
}

void get_sum(ArrowSchema* in_schema, ArrowDeviceArray* input, ArrowSchema* out_schema,
             ArrowDeviceArray* output) {
  auto col = cudf::from_arrow_device_column(in_schema, input);
  auto sumagg = cudf::make_sum_aggregation();
  auto scalar = cudf::reduce(*col, *dynamic_cast<cudf::reduce_aggregation*>(sumagg.get()),
                             col->type());
  auto result = cudf::make_column_from_scalar(*scalar, 1);

  ArrowSchemaMove(cudf::to_arrow_schema(cudf::table_view({*result}),
                                        std::vector<cudf::column_metadata>{{"result"}})
                      .get(),
                  out_schema);

  ArrowDeviceArrayMove(cudf::to_arrow_device(cudf::table_view({*result})).get(), output);

  ArrowArrayRelease(&input->array);
  ArrowSchemaRelease(in_schema);
}
