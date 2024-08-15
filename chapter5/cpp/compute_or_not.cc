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

#include <arrow/api.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <algorithm>
#include <iostream>
#include <numeric>
#include <vector>
#include "timer.h"

namespace cp = arrow::compute;

int main(int argc, char** argv) {
  for (int n = 10000; n <= 10000000; n += 10000) {
    std::vector<int32_t> testvalues(n);
    std::iota(std::begin(testvalues), std::end(testvalues), 0);

    arrow::Int32Builder num_bldr;
    ARROW_UNUSED(num_bldr.AppendValues(testvalues));
    std::shared_ptr<arrow::Array> numarr;
    ARROW_UNUSED(num_bldr.Finish(&numarr));

    std::cout << "N: " << n << std::endl;

    auto arr = std::static_pointer_cast<arrow::Int32Array>(numarr);

    arrow::Datum res1;
    {
      timer t;
      res1 = cp::Add(arr, arrow::Datum{(int32_t)2}).MoveValueUnsafe();
    }

    arrow::Datum res2;
    {
      timer t;
      arrow::Int32Builder bldr;
      for (size_t i = 0; i < arr->length(); ++i) {
        if (arr->IsValid(i)) {
          ARROW_UNUSED(bldr.Append(arr->Value(i) + 2));
        } else {
          ARROW_UNUSED(bldr.AppendNull());
        }
      }
      std::shared_ptr<arrow::Array> output;
      ARROW_UNUSED(bldr.Finish(&output));
      res2 = arrow::Datum{std::move(output)};
    }
    std::cout << std::boolalpha << (res1 == res2) << std::endl;

    arrow::Datum res3;
    {
      timer t;
      auto arr = std::static_pointer_cast<arrow::Int32Array>(numarr);
      arrow::Int32Builder bldr;
      ARROW_UNUSED(bldr.Reserve(arr->length()));
      std::for_each(std::begin(*arr), std::end(*arr),
                    [&bldr](const auto& v) {
                      if (v) {
                        ARROW_UNUSED(bldr.Append(*v + 2));
                      } else {
                        ARROW_UNUSED(bldr.AppendNull());
                      }
                    });
      std::shared_ptr<arrow::Array> output;
      ARROW_UNUSED(bldr.Finish(&output));
      res3 = arrow::Datum{std::move(output)};
    }
    std::cout << std::boolalpha << (res1 == res3) << std::endl;

    arrow::Datum res4;
    {
      timer t;
      auto arr = std::static_pointer_cast<arrow::Int32Array>(numarr);
      std::shared_ptr<arrow::Buffer> newbuf =
          arrow::AllocateBuffer(sizeof(int32_t) * arr->length())
              .MoveValueUnsafe();
      auto output = reinterpret_cast<int32_t*>(newbuf->mutable_data());
      std::transform(arr->raw_values(), arr->raw_values() + arr->length(),
                     output, [](const int32_t v) { return v + 2; });

      res4 = arrow::Datum{arrow::MakeArray(
          arrow::ArrayData::Make(arr->type(), arr->length(),
                                 std::vector<std::shared_ptr<arrow::Buffer>>{
                                     arr->null_bitmap(), newbuf},
                                 arr->null_count()))};
    }
    std::cout << std::boolalpha << (res1 == res4) << std::endl;
  }
}