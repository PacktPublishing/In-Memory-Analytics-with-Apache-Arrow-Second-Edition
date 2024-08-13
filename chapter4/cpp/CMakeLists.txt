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
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

cmake_minimum_required(VERSION 3.20)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
project(example-cdata CXX)

include(CMakeParseArguments)
include(FetchContent)

set(NANOARROW_NAMESPACE "ExampleCData")
FetchContent_Declare(nanoarrow
                    GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
                    GIT_TAG 16f4306f58193eff18013cc75d8b7b49f1ad4108
                    GIT_SHALLOW TRUE)

FetchContent_MakeAvailable(nanoarrow)

option(WITH_CUDA "build get_sum with CUDA support")
add_library(example-cdata SHARED example_cdata.cc)
target_compile_definitions(example-cdata PUBLIC USE_NANOARROW)
set_target_properties(example-cdata 
    PROPERTIES CXX_STANDARD 20
               CXX_STANDARD_REQUIRED ON
               CXX_EXTENSIONS ON)

target_link_libraries(example-cdata PRIVATE nanoarrow)

if(WITH_CUDA)
    find_package(cudf REQUIRED)

    add_library(get-sum SHARED example_with_cuda.cc)
    set_target_properties(get-sum
        PROPERTIES CXX_STANDARD 20
                CXX_STANDARD_REQUIRED ON
                CXX_EXTENSIONS ON)

    target_link_libraries(get-sum PRIVATE nanoarrow cudf::cudf)
endif(WITH_CUDA)