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
#include <iostream>
#include <limits>
#include <memory>
#include <random>

#include "abi.h"

#ifdef USE_NANOARROW
#include "nanoarrow/nanoarrow.hpp" // for export_int32_data_nanoarrow
#endif

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
#ifdef USE_NANOARROW
// returns 0 on success, anything else on failure
int export_int32_data_nanoarrow(struct ArrowArray*);
#endif
}

void export_int32_data(struct ArrowArray* array) {
  const int64_t length = 1000;
  std::unique_ptr<std::vector<int32_t>> data =
      std::make_unique<std::vector<int32_t>>(generate_data(length));

  *array = ArrowArray{
      length,
      0,                                                                   // null_count
      0,                                                                   // offset
      2,                                                                   // n_buffers
      0,                                                                   // n_children
      new const void*[2]{nullptr, reinterpret_cast<void*>(data->data())},  // buffers
      nullptr,                                                             // children
      nullptr,                                                             // dictionary
      [](struct ArrowArray* arr) {  // release callback
        delete[] arr->buffers;
        delete reinterpret_cast<std::vector<int32_t>*>(arr->private_data);
        arr->release = nullptr;
      },
      reinterpret_cast<void*>(data.release()),  // private_data
  };
}

#ifdef USE_NANOARROW
// use nanoarrow instead
int export_int32_data_nanoarrow(struct ArrowArray* array) {
  const int64_t length = 1000;
  auto data = generate_data(length);

  // initialize the structure with INT32 type
  NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromType(array, NANOARROW_TYPE_INT32));
  // initialize the data buffer from the generated data and set up the deallocator
  // nanoarrow handles all the lifetimes for you and moves it into the array object
  // along with constructing the release callback function for you.
  nanoarrow::BufferInitSequence(ArrowArrayBuffer(array, 1), std::move(data));

  // now we set our null count and length
  array->null_count = 0;
  array->length = length;

  // finally perform basic validity checks and finalization before returning
  return ArrowArrayFinishBuildingDefault(array, nullptr);
}
#endif