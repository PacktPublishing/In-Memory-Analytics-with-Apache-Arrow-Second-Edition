# In-Memory Analytics with Apache Arrow

<a href="https://www.packtpub.com/en-us/product/in-memory-analytics-with-apache-arrow-9781835461228"><img src="https://static.packt-cdn.com/products/<13-P ISBN>/cover/smaller" alt="In-Memory Analytics with Apache Arrow
" height="256px" align="right"></a>

This is the code repository for [<Book name>](https://www.packtpub.com/en-us/product/in-memory-analytics-with-apache-arrow-9781835461228), published by Packt.

**Accelerate data analytics for efficient processing of flat and hierarchical data structures**

## What is this book about?
Apache Arrow is an open source, columnar in-memory data format designed for efficient data processing and analytics. This book harnesses the author’s 15 years of experience to show you a standardized way to work with tabular data across various programming languages and environments, enabling high-performance data processing and exchange.

This book covers the following exciting features: 
* Use Apache Arrow libraries to access data files, both locally and in the cloud
* Understand the zero-copy elements of the Apache Arrow format
* Improve the read performance of data pipelines by memory-mapping Arrow files
* Produce and consume Apache Arrow data efficiently by sharing memory with the C API
* Leverage the Arrow compute engine, Acero, to perform complex operations
* Create Arrow Flight servers and clients for transferring data quickly
* Build the Arrow libraries locally and contribute to the community
  
If you feel this book is for you, get your [copy](https://www.amazon.com/dp/1835461220) today!

<a href="https://www.packtpub.com/?utm_source=github&utm_medium=banner&utm_campaign=GitHubBanner"><img src="https://raw.githubusercontent.com/PacktPublishing/GitHub/master/GitHub.png" alt="https://www.packtpub.com/" border="5" /></a>

## Instructions and Navigations
All of the code is organized into folders.

The code will look like the following:
```
>>> import numba.cuda
>>> import pyarrow as pa
>>> from pyarrow import cuda
>>> import numpy as np
>>> from pyarrow.cffi import ffi
```

**Following is what you need for this book:**
This book is for developers, data engineers, and data scientists looking to explore the capabilities of Apache Arrow from the ground up. Whether you’re building utilities for data analytics and query engines, or building full pipelines with tabular data, this book can help you out regardless of your preferred programming language. A basic understanding of data analysis concepts is needed, but not necessary. Code examples are provided using C++, Python, and Go throughout the book.

With the following software and hardware list you can run all code files present in the book (Chapter 1-12).

### Software and Hardware List

| Chapter  | Software required                                                                    | OS required                        |
| -------- | -------------------------------------------------------------------------------------| -----------------------------------|
|  1-12		 |   							      Python 3.8 or higher                                      			  | Windows, Mac OS X, and Linux (Any) |
|  1-12        |   			C++ compiler capable of C++17 or higher| Windows, macOS, or Linux|
|			1-12															  |          conda/mamba (optional) |Windows, macOS, or Linux                          |
| 1-12  |vcpkg (optional) |Windows|
|1-12|MSYS2 (optional) |Windows|
|1-12|CMake 3.16 or higher| Windows, macOS, or Linux|
|1-12|make or ninja |macOS or Linux|
|1-12|Docker |Windows, macOS, or Linux|
|1-12|Go 1.21 or higher| Windows, macOS|

### Related products <Other books you may enjoy>
* <Book name #1 from backmatter> [[Packt]](<Book link on Packtpub>) [[Amazon]](https://www.amazon.com/dp/<10P-ISBN>)

* <Book name #2 from backmatter> [[Packt]](<Book link on Packtpub>) [[Amazon]](https://www.amazon.com/dp/<10P-ISBN>)

## Get to Know the Author(s)
**Matt Topol** is a software engineering enthusiast with roots at Brooklyn Polytechnic (now NYU-Poly). He joined FactSet Research Systems, Inc. in 2009, specializing in financial data systems. Matt’s career spans infrastructure and app development, team leadership, and architecting large-scale distributed systems for financial analytics. He is a key member of the Apache Arrow Project Management Committee (PMC), dedicated to expanding the Arrow community. Recently, he joined Voltron Data to focus on Arrow’s Golang library, promoting it globally through conferences and talks. Originally from Brentwood, NY, he now resides in Connecticut. Outside of work, Matt enjoys coding, creating intricate fantasy games, and enthusiastically sharing his expertise.
