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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;

/**
 * Java equivalent of csv_writer.cc - demonstrates writing Arrow tables to CSV
 */
public class CsvWriter {

    /**
     * Read CSV file into VectorSchemaRoot batches
     */
    public static void readAndWriteCsv(String inputPath, String outputPath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String uri = "file://" + Paths.get(inputPath).toAbsolutePath();

            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.CSV, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches();
                 BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {

                boolean headerWritten = false;
                int totalRows = 0;

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    // Write header on first batch
                    if (!headerWritten) {
                        writeHeader(writer, root.getSchema());
                        headerWritten = true;
                    }

                    // Write data rows
                    writeData(writer, root);
                    totalRows += root.getRowCount();
                }

                System.out.println("Wrote " + totalRows + " rows to " + outputPath);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Write CSV header from Arrow schema
     */
    private static void writeHeader(BufferedWriter writer, Schema schema) throws IOException {
        StringBuilder header = new StringBuilder();
        for (int i = 0; i < schema.getFields().size(); i++) {
            if (i > 0) header.append(",");
            header.append(schema.getFields().get(i).getName());
        }
        writer.write(header.toString());
        writer.newLine();
    }

    /**
     * Write data rows from VectorSchemaRoot
     */
    private static void writeData(BufferedWriter writer, VectorSchemaRoot root) throws IOException {
        for (int row = 0; row < root.getRowCount(); row++) {
            StringBuilder line = new StringBuilder();
            for (int col = 0; col < root.getFieldVectors().size(); col++) {
                if (col > 0) line.append(",");
                FieldVector vector = root.getFieldVectors().get(col);

                if (vector.isNull(row)) {
                    line.append("");
                } else {
                    Object value = vector.getObject(row);
                    if (value != null) {
                        String strValue = value.toString();
                        // Escape commas and quotes
                        if (strValue.contains(",") || strValue.contains("\"")) {
                            strValue = "\"" + strValue.replace("\"", "\"\"") + "\"";
                        }
                        line.append(strValue);
                    }
                }
            }
            writer.write(line.toString());
            writer.newLine();
        }
    }

    /**
     * Incremental write example - writes batch by batch
     */
    public static void incrementalWrite(String inputPath, String outputPath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String uri = "file://" + Paths.get(inputPath).toAbsolutePath();

            ScanOptions options = new ScanOptions(/*batchSize*/ 1024);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.CSV, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches();
                 BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {

                boolean headerWritten = false;
                int batchCount = 0;

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    if (!headerWritten) {
                        writeHeader(writer, root.getSchema());
                        headerWritten = true;
                    }

                    writeData(writer, root);
                    batchCount++;
                    System.out.println("Wrote batch " + batchCount + " with " + root.getRowCount() + " rows");
                }

                System.out.println("Incremental write complete: " + batchCount + " batches written");
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String inputPath = "../sample_data/train.csv";
        String outputPath = "train_output.csv";

        System.out.println("=== Standard Write ===");
        readAndWriteCsv(inputPath, outputPath);

        System.out.println("\n=== Incremental Write ===");
        incrementalWrite(inputPath, "train_incremental.csv");
    }
}
