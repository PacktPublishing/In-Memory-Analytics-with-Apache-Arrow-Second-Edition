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
import org.apache.arrow.vector.ipc.JsonFileReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Java equivalent of json_reader.cc - demonstrates reading JSON files into Arrow tables
 *
 * Note: Apache Arrow Java provides JsonFileReader for reading Arrow IPC JSON format,
 * not general JSON files. For general JSON, you would typically use a library like
 * Jackson to parse JSON and then populate Arrow vectors.
 */
public class JsonReader {

    /**
     * Read Arrow IPC JSON file using JsonFileReader
     */
    public static void readArrowJson(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            File jsonFile = new File(filepath);

            try (JsonFileReader reader = new JsonFileReader(jsonFile, allocator)) {
                Schema schema = reader.start();
                System.out.println("Schema: " + schema);

                // Read all record batches
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    int batchCount = 0;
                    while (reader.read(root)) {
                        batchCount++;
                        System.out.println("Batch " + batchCount + ": " + root.getRowCount() + " rows");
                        System.out.println(root.contentToTSVString());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading JSON file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Example of reading general JSON and converting to Arrow vectors
     * This is a simplified example showing the concept
     */
    public static void demonstrateJsonToArrow() {
        System.out.println("\n=== JSON to Arrow Conversion Concept ===");
        System.out.println("For general JSON files, you would:");
        System.out.println("1. Use a JSON library (Jackson, Gson) to parse the JSON");
        System.out.println("2. Create Arrow vectors based on the JSON structure");
        System.out.println("3. Populate the vectors with parsed values");
        System.out.println("4. Create a VectorSchemaRoot from the vectors");

        // Example with Arrow vectors
        try (BufferAllocator allocator = new RootAllocator();
             org.apache.arrow.vector.VarCharVector nameVector =
                 new org.apache.arrow.vector.VarCharVector("name", allocator);
             org.apache.arrow.vector.IntVector ageVector =
                 new org.apache.arrow.vector.IntVector("age", allocator)) {

            // Simulating parsed JSON data
            String[] names = {"Alice", "Bob", "Charlie"};
            int[] ages = {30, 25, 35};

            nameVector.allocateNew(names.length);
            ageVector.allocateNew(names.length);

            for (int i = 0; i < names.length; i++) {
                nameVector.setSafe(i, names[i].getBytes());
                ageVector.setSafe(i, ages[i]);
            }

            nameVector.setValueCount(names.length);
            ageVector.setValueCount(ages.length);

            System.out.println("\nSample Arrow vectors from 'JSON' data:");
            System.out.println("Names: " + nameVector);
            System.out.println("Ages: " + ageVector);
        }
    }

    public static void main(String[] args) {
        // Note: Arrow IPC JSON format is different from general JSON
        // This example demonstrates both approaches

        String filepath = "sample.json";

        System.out.println("=== Arrow JSON File Reader ===");
        System.out.println("Attempting to read: " + filepath);
        System.out.println("(Note: This expects Arrow IPC JSON format, not general JSON)");

        // Uncomment when you have an Arrow IPC JSON file
        // readArrowJson(filepath);

        // Demonstrate concept of JSON to Arrow conversion
        demonstrateJsonToArrow();
    }
}
