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
package chapter1;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Java equivalent of examples.cc - demonstrates basic Arrow data structures */
public class Examples {

  /** First example: Create a simple Int64 array */
  public static void firstExample() {
    try (BufferAllocator allocator = new RootAllocator();
        BigIntVector vector = new BigIntVector("data", allocator)) {

      long[] data = {1, 2, 3, 4};
      vector.allocateNew(data.length);
      for (int i = 0; i < data.length; i++) {
        vector.set(i, data[i]);
      }
      vector.setValueCount(data.length);

      System.out.println("First Example:");
      System.out.println(vector);
    }
  }

  /** Random data example: Create a RecordBatch with random double values */
  public static void randomDataExample() {
    try (BufferAllocator allocator = new RootAllocator()) {
      Random random = new Random();
      double mean = 5.0;
      double stdDev = 2.0;

      int ncols = 16;
      int nrows = 8192;

      List<Field> fields = new ArrayList<>();
      List<Float8Vector> columns = new ArrayList<>();

      for (int i = 0; i < ncols; i++) {
        Float8Vector column = new Float8Vector("c" + i, allocator);
        column.allocateNew(nrows);

        for (int j = 0; j < nrows; j++) {
          // Normal distribution using Box-Muller transform
          double value = mean + stdDev * random.nextGaussian();
          column.set(j, value);
        }
        column.setValueCount(nrows);

        fields.add(
            Field.nullable("c" + i, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columns.add(column);
      }

      Schema schema = new Schema(fields);
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        root.setRowCount(nrows);

        System.out.println("\nRandom Data Example:");
        System.out.println("Schema: " + schema);
        System.out.println("Row count: " + nrows);
        System.out.println("Column count: " + ncols);
        System.out.println("First few values from column 0: ");
        Float8Vector col0 = columns.get(0);
        for (int i = 0; i < Math.min(5, nrows); i++) {
          System.out.println("  " + col0.get(i));
        }
      }

      // Clean up columns
      for (Float8Vector col : columns) {
        col.close();
      }
    }
  }

  public record Archer(String name, String location, short year) {}

  /** Building struct array: Create a struct array with archers data */
  public static void buildingStructArray() {
    // Data to be used
    Archer[] archers = {
      new Archer("Legolas", "Murkwood", (short) 1954),
      new Archer("Oliver", "Star City", (short) 1941),
      new Archer("Merida", "Scotland", (short) 2012),
      new Archer("Lara", "London", (short) 1996),
      new Archer("Artemis", "Greece", (short) -600)
    };

    try (BufferAllocator allocator = new RootAllocator()) {
      // Create field definitions
      List<Field> structFields =
          Arrays.asList(
              Field.nullable("archer", new ArrowType.Utf8()),
              Field.nullable("location", new ArrowType.Utf8()),
              Field.nullable("year", new ArrowType.Int(16, true)));

      FieldType structType = new FieldType(true, new ArrowType.Struct(), null);
      Field structField = new Field("struct", structType, structFields);

      try (StructVector structVector = StructVector.empty("struct", allocator)) {
        structVector.initializeChildrenFromFields(structFields);
        structVector.allocateNew();

        VarCharVector archerVector = (VarCharVector) structVector.getChild("archer");
        VarCharVector locationVector = (VarCharVector) structVector.getChild("location");
        SmallIntVector yearVector = (SmallIntVector) structVector.getChild("year");

        for (int i = 0; i < archers.length; i++) {
          archerVector.setSafe(i, archers[i].name.getBytes(StandardCharsets.UTF_8));
          locationVector.setSafe(i, archers[i].location.getBytes(StandardCharsets.UTF_8));
          yearVector.setSafe(i, archers[i].year);
          structVector.setIndexDefined(i);
        }

        structVector.setValueCount(archers.length);

        System.out.println("\nBuilding Struct Array:");
        System.out.println(structVector);
      }
    }
  }

  /** Data row class for row conversion examples */
  static class DataRow {
    long id;
    long components;
    List<Double> componentCost;

    DataRow(long id, long components, List<Double> componentCost) {
      this.id = id;
      this.components = components;
      this.componentCost = componentCost;
    }
  }

  /** Convert vector of rows to columnar format (Arrow VectorSchemaRoot) */
  public static VectorSchemaRoot vectorToColumnar(BufferAllocator allocator, List<DataRow> rows) {
    List<Field> fields =
        Arrays.asList(
            Field.nullable("id", new ArrowType.Int(64, true)),
            Field.nullable("components", new ArrowType.Int(64, true)),
            new Field(
                "component_cost",
                new FieldType(true, new ArrowType.List(), null),
                Arrays.asList(
                    Field.nullable(
                        "item", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))));
    Schema schema = new Schema(fields);

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    BigIntVector idVector = (BigIntVector) root.getVector("id");
    BigIntVector compVector = (BigIntVector) root.getVector("components");
    ListVector costListVector = (ListVector) root.getVector("component_cost");

    UnionListWriter listWriter = costListVector.getWriter();

    for (int i = 0; i < rows.size(); i++) {
      DataRow row = rows.get(i);
      idVector.setSafe(i, row.id);
      compVector.setSafe(i, row.components);

      listWriter.setPosition(i);
      listWriter.startList();
      for (Double cost : row.componentCost) {
        listWriter.float8().writeFloat8(cost);
      }
      listWriter.endList();
    }

    root.setRowCount(rows.size());
    return root;
  }

  /** Convert columnar format back to vector of rows */
  public static List<DataRow> columnarToVector(VectorSchemaRoot root) {
    List<DataRow> rows = new ArrayList<>();

    BigIntVector idVector = (BigIntVector) root.getVector("id");
    BigIntVector compVector = (BigIntVector) root.getVector("components");
    ListVector costListVector = (ListVector) root.getVector("component_cost");

    for (int i = 0; i < root.getRowCount(); i++) {
      long id = idVector.get(i);
      long components = compVector.get(i);

      List<Double> costs = new ArrayList<>();
      int start = costListVector.getOffsetBuffer().getInt(i * 4);
      int end = costListVector.getOffsetBuffer().getInt((i + 1) * 4);
      Float8Vector valuesVector = (Float8Vector) costListVector.getDataVector();

      for (int j = start; j < end; j++) {
        costs.add(valuesVector.get(j));
      }

      rows.add(new DataRow(id, components, costs));
    }

    return rows;
  }

  /** Run row conversion examples */
  public static void runRowConversions() {
    try (BufferAllocator allocator = new RootAllocator()) {
      List<DataRow> orig =
          Arrays.asList(
              new DataRow(1, 1, Arrays.asList(10.0)),
              new DataRow(2, 3, Arrays.asList(11.0, 12.0, 13.0)),
              new DataRow(3, 2, Arrays.asList(15.0, 25.0)));

      try (VectorSchemaRoot table = vectorToColumnar(allocator, orig)) {
        List<DataRow> convertedRows = columnarToVector(table);

        assert orig.size() == convertedRows.size();

        System.out.println("\nRow Conversions:");
        System.out.printf("%-3s %-11s %-15s%n", "ID", "Components", "Component prices");
        for (DataRow row : convertedRows) {
          System.out.printf("%-3d %-11d", row.id, row.components);
          for (Double cost : row.componentCost) {
            System.out.printf("%-4.0f", cost);
          }
          System.out.println();
        }
      }
    }
  }

  public static void main(String[] args) {
    firstExample();
    randomDataExample();
    buildingStructArray();
    runRowConversions();
  }
}
