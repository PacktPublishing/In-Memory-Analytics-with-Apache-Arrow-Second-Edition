#!/usr/bin/env python3

# MIT License
#
# Copyright (c) 2024 Packt
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import numba.cuda
import pyarrow as pa
from pyarrow import cuda
import numpy as np
from pyarrow.cffi import ffi
ctx = cuda.Context(0)

def run_export():
    ffi.cdef("""
        void export_int32_data(struct ArrowArray*);    
    """)

    lib = ffi.dlopen("../cpp/libexample-cdata.so")
    # create a new pointer with ffi
    c_arr = ffi.new("struct ArrowArray*")
    # cast it to a uintptr_t
    c_ptr = int(ffi.cast("uintptr_t", c_arr))
    # call the function we made!
    lib.export_int32_data(c_arr)

    # import it via the C Data API so we can use it
    arrnew = pa.Array._import_from_c(c_ptr, pa.int32())
    # do stuff with the array like print it
    print(arrnew)
    del arrnew # will call the release callback once it is garbage collected

def run_cuda():
    ffi.cdef("""    
        void get_sum(struct ArrowSchema*, struct ArrowDeviceArray*,
                     struct ArrowSchema*, struct ArrowDeviceArray*);
    """)

    lib = ffi.dlopen("../cpp/build/libget-sum.so")

    arr = np.arange(10, 14, dtype=np.int32)
    device_arr = numba.cuda.to_device(arr)
    cuda_buf = cuda.CudaBuffer.from_numba(device_arr.gpu_data)
    arrow_arr = pa.Array.from_buffers(pa.int32(), 4, [None, cuda_buf], null_count=0)

    c_array = ffi.new("struct ArrowDeviceArray*")
    c_schema = ffi.new("struct ArrowSchema*")
    ptr_array = int(ffi.cast("uintptr_t", c_array))
    ptr_schema = int(ffi.cast("uintptr_t", c_schema))

    arrow_arr._export_to_c_device(ptr_array, ptr_schema)

    out_array = ffi.new("struct ArrowDeviceArray*")
    out_schema = ffi.new("struct ArrowSchema*")

    lib.get_sum(c_schema, c_array, out_schema, out_array)
    ptr_out_schema = int(ffi.cast("uintptr_t", out_schema))
    ptr_out = int(ffi.cast("uintptr_t", out_array))

    result = pa.Array._import_from_c_device(ptr_out, ptr_out_schema)
    del result

if __name__ == '__main__':
    run_export()
    run_cuda()