/*
 * Copyright 2022 Foursquare Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.shchoi.trino.h3;

import static io.shchoi.trino.h3.H3PluginTest.assertQueryResults;
import static io.shchoi.trino.h3.H3PluginTest.createQueryRunner;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.trino.testing.QueryRunner;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

@TestInstance(Lifecycle.PER_CLASS)
public class VertexFunctionsTest {
  @Test
  public void testConstructor() {
    assertNotNull(new VertexFunctions());
  }

  @Test
  public void testCellToVertex() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(from_base('85283473fffffff', 16), 0), h3_cell_to_vertex(from_base('85283473fffffff', 16), 3)",
          List.of(List.of(0x22528340bfffffffL, 0x255283463fffffffL)));

      // Test all 6 vertices of a hexagon (0-5)
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(from_base('85283473fffffff', 16), 5)",
          List.of(List.of(0x23528340bfffffffL)));

      // Test out of range vertexNum (hexagon has 6 vertices: 0-5)
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(from_base('85283473fffffff', 16), 6)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(from_base('85283473fffffff', 16), 7)",
          List.of(Collections.singletonList(null)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(null, 4)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(from_base('85283473fffffff', 16), null)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(from_base('85283473fffffff', 16), -1)",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToVertexes() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertexes(from_base('85283473fffffff', 16))",
          List.of(
              List.of(
                  List.of(
                      0x22528340bfffffffL,
                      0x235283447fffffffL,
                      0x205283463fffffffL,
                      0x255283463fffffffL,
                      0x22528340ffffffffL,
                      0x23528340bfffffffL))));
      // Pentagon:
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertexes(from_base('811c3ffffffffff', 16))",
          List.of(
              List.of(
                  List.of(
                      0x2011c3ffffffffffL,
                      0x2111c3ffffffffffL,
                      0x2211c3ffffffffffL,
                      0x2311c3ffffffffffL,
                      0x2411c3ffffffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertexes(0) hex",
          List.of(
              List.of(
                  List.of(
                      0x2000000000000000L,
                      0x2100000000000000L,
                      0x2200000000000000L,
                      0x2300000000000000L,
                      0x2400000000000000L,
                      0x2500000000000000L))));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertexes(null) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testVertexToLatLng() throws ParseException {
    try (QueryRunner queryRunner = createQueryRunner()) {
      GeometryFactory geometryFactory = new GeometryFactory();
      WKTReader wktReader = new WKTReader(geometryFactory);
      Geometry expectedPoint = wktReader.read("POINT (-122.03773496427027 37.42012867767779)");
      assertQueryResults(
          queryRunner,
          "SELECT ST_AsText(h3_vertex_to_latlng(from_base('255283463fffffff', 16)))",
          List.of(List.of(expectedPoint)));

      Geometry expectedPoint2 = wktReader.read("POINT (31.831280499087402 68.92995788193981)");
      assertQueryResults(
          queryRunner,
          "SELECT ST_AsText(h3_vertex_to_latlng(0))",
          List.of(List.of(expectedPoint2)));
      assertQueryResults(
          queryRunner,
          "SELECT ST_AsText(h3_vertex_to_latlng(-1))",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_vertex_to_latlng(null)",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testIsValidVertex() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_is_valid_vertex(from_base('85283473fffffff', 16)), h3_is_valid_vertex(from_base('255283463fffffff', 16))",
          List.of(List.of(false, true)));

      assertQueryResults(queryRunner, "SELECT h3_is_valid_vertex(0)", List.of(List.of(false)));
      assertQueryResults(queryRunner, "SELECT h3_is_valid_vertex(-1)", List.of(List.of(false)));
      assertQueryResults(
          queryRunner, "SELECT h3_is_valid_vertex(null)", List.of(Collections.singletonList(null)));
    }
  }
}
