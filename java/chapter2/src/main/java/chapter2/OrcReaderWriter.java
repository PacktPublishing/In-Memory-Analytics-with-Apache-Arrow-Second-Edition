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
import org.apache.arrow.adapter.orc.OrcReader;
//import org.apache.arrow.adapter.orc.OrcReaderJniWrapper;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;

/**
 * Java equivalent of orc_reader_writer.cc - demonstrates reading and writing ORC files
 *
 * Note: Apache Arrow Java has ORC support through arrow-orc module and Dataset API
 */
public class OrcReaderWriter {

    /**
     * Read ORC file using Arrow Dataset API
     */
    public static void readOrcWithDataset(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();

            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.ORC, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                System.out.println("Schema: " + reader.getVectorSchemaRoot().getSchema());

                int totalRows = 0;
                int batchCount = 0;
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    totalRows += root.getRowCount();
                    batchCount++;
                    System.out.println("Batch " + batchCount + ": " + root.getRowCount() + " rows");
                }
                System.out.println("Total rows read: " + totalRows);
            }
        } catch (Exception e) {
            System.err.println("Error reading ORC file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Read ORC file using native ORC reader (if available)
     */
    public static void readOrcNative(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            // OrcReader requires the native ORC library
            // OrcReader reader = new OrcReader(filepath, allocator);

            System.out.println("Native ORC reading requires arrow-orc native library");
            System.out.println("Use Dataset API for cross-platform compatibility");
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * Write ORC file
     * Note: Writing ORC files in Java Arrow typically requires the Dataset API
     * or using Apache ORC library directly with Arrow integration
     */
    public static void writeOrc(String filepath) {
        System.out.println("\n=== ORC Writing ===");
        System.out.println("ORC writing in Arrow Java is typically done via:");
        System.out.println("1. Arrow Dataset API with FileFormat.ORC");
        System.out.println("2. Apache ORC library with Arrow batch conversion");

        // Example using Dataset write (conceptual - requires proper setup)
        /*
        try (BufferAllocator allocator = new RootAllocator()) {
            // Create sample data
            Schema schema = new Schema(Arrays.asList(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("value", new ArrowType.FloatingPoint(FloatingPoint.Precision.DOUBLE))
            ));

            // Dataset write options would be configured here
            // FileSystemDataset.write(scanner, FileFormat.ORC, outputPath);
        }
        */
    }

    public static void main(String[] args) {
        String inputPath = "../sample_data/train.orc";
        String outputPath = "train_output.orc";

        System.out.println("=== ORC Reader ===");
        System.out.println("Reading ORC file: " + inputPath);
        readOrcWithDataset(inputPath);

        writeOrc(outputPath);
    }
}
