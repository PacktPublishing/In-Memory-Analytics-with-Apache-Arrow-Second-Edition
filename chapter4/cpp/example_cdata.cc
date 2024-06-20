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

#include <algorithm>
#include <cassert>
#include <iostream>
#include <limits>
#include <random>
#include <vector>

#include <arrow/c/abi.h>
#include <arrow/c/helpers.h>
#include <cudf/column/column_factories.hpp>
#include <cudf/groupby.hpp>
#include <cudf/interop.hpp>
#include <cudf/reduction.hpp>

std::vector<int32_t> generate_data(size_t size) {
  static std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int32_t>::min(),
                                                     std::numeric_limits<int32_t>::max());
  static std::random_device rnd_device;
  std::default_random_engine generator(rnd_device());
  std::vector<int32_t> data(size);
  std::generate(data.begin(), data.end(), [&]() { return dist(generator); });
  return data;
}

extern "C" {
void export_int32_data(struct ArrowArray*);
void get_sum(struct ArrowSchema* in_schema, struct ArrowDeviceArray* input,
             struct ArrowSchema* out_schema, struct ArrowDeviceArray* output);
}

void export_int32_data(struct ArrowArray* array) {
  std::vector<int32_t>* vecptr = new std::vector<int32_t>(std::move(generate_data(1000)));

  *array = (struct ArrowArray){
      .length = static_cast<int64_t>(vecptr->size()),
      .null_count = 0,
      .offset = 0,
      .n_buffers = 2,
      .n_children = 0,
      .buffers = (const void**)malloc(sizeof(void*) * 2),
      .children = nullptr,
      .dictionary = nullptr,
      .release =
          [](struct ArrowArray* arr) {
            free(arr->buffers);
            delete reinterpret_cast<std::vector<int32_t>*>(arr->private_data);
            arr->release = nullptr;
          },
      .private_data = reinterpret_cast<void*>(vecptr),
  };
  array->buffers[0] = nullptr;
  array->buffers[1] = vecptr->data();
}  // end of function

// if using Arrow < v17, you need to include these helpers manually
// if using Arrow >= v17, these are in <arrow/c/helpers.h>
inline int ArrowDeviceArrayIsReleased(const struct ArrowDeviceArray* array) {
  return ArrowArrayIsReleased(&array->array);
}

inline void ArrowDeviceArrayMarkReleased(struct ArrowDeviceArray* array) {
  ArrowArrayMarkReleased(&array->array);
}

inline void ArrowDeviceArrayMove(struct ArrowDeviceArray* src,
                                 struct ArrowDeviceArray* dest) {
  assert(dest != src);
  assert(!ArrowDeviceArrayIsReleased(src));
  memcpy(dest, src, sizeof(struct ArrowDeviceArray));
  ArrowDeviceArrayMarkReleased(src);
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
