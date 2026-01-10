/*
 * MIT License
 *
 * Copyright (c) 2024 Packt
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package chapter2;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;

/**
 * Java equivalent of csv_reader.cc - demonstrates reading CSV files into Arrow tables
 */
public class CsvReader {

    /**
     * Read a CSV file using Arrow Dataset API
     */
    public static void readCsvWithDataset(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            // Use the Dataset API to read CSV
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();

            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.CSV, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                System.out.println("Schema: " + reader.getVectorSchemaRoot().getSchema());

                int totalRows = 0;
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    totalRows += root.getRowCount();
                    System.out.println("Batch rows: " + root.getRowCount());
                }
                System.out.println("Total rows read: " + totalRows);
            }
        } catch (Exception e) {
            System.err.println("Error reading CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Simple CSV reader example using basic file reading
     * (For cases where Dataset API is not available)
     */
    public static void readCsvSimple(String filepath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            // Read header
            String header = reader.readLine();
            if (header != null) {
                String[] columns = header.split(",");
                System.out.println("Columns: " + String.join(", ", columns));
            }

            // Count rows
            int rowCount = 0;
            while (reader.readLine() != null) {
                rowCount++;
            }
            System.out.println("Row count: " + rowCount);
        } catch (IOException e) {
            System.err.println("Error reading CSV: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String filepath = "../sample_data/train.csv";

        System.out.println("Reading CSV file: " + filepath);
        System.out.println("\n--- Using Dataset API ---");
        readCsvWithDataset(filepath);
    }
}
