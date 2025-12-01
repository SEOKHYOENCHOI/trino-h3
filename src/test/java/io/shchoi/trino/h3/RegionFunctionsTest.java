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
public class RegionFunctionsTest {
  @Test
  public void testConstructor() {
    assertNotNull(new RegionFunctions());
  }

  @Test
  public void testPolygonToCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_polygon_to_cells(ST_GeometryFromText('POLYGON ((0 0, 1 1, 1 0, 0 0))'), 4) hex",
          List.of(
              List.of(
                  List.of(
                      0x84754ebffffffffL,
                      0x84754e3ffffffffL,
                      0x84754c5ffffffffL,
                      0x84754c7ffffffffL))));
      // Test polygon with hole - outer ring with inner hole
      // Polygon: large square with smaller square hole in center
      // At resolution 4, the hole should exclude some cells from the result
      assertQueryResults(
          queryRunner,
          "SELECT cardinality(h3_polygon_to_cells(ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0), (0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))'), 4)) > 0",
          List.of(List.of(true)));

      // Verify polygon with hole returns fewer cells than same polygon without hole
      assertQueryResults(
          queryRunner,
          "SELECT cardinality(h3_polygon_to_cells(ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0), (0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))'), 4)) < cardinality(h3_polygon_to_cells(ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'), 4))",
          List.of(List.of(true)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_polygon_to_cells(ST_GeometryFromText('POINT (40 4)'), 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_polygon_to_cells(null, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_polygon_to_cells(ST_GeometryFromText('POLYGON ((0 0, 1 1, 1 0, 0 0))'), null) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellsToMultiPolygon() throws ParseException {
    try (QueryRunner queryRunner = createQueryRunner()) {
      GeometryFactory geometryFactory = new GeometryFactory();
      WKTReader wktReader = new WKTReader(geometryFactory);
      // Trino 436 normalizes polygon ring orientation, resulting in reversed coordinate order
      Geometry expectedMultiPolygon =
          wktReader.read(
              "MULTIPOLYGON (((-121.92354999630156 37.42834118609436, -122.03773496427027 37.42012867767779, -122.090428929044 37.33755608435299, -122.02910130918998 37.26319797461824, -121.91508032705622 37.2713558667319, -121.86222328902491 37.353926450852256, -121.92354999630156 37.42834118609436)))");
      assertQueryResults(
          queryRunner,
          "SELECT ST_AsText(h3_cells_to_multi_polygon(ARRAY [from_base('85283473fffffff', 16)])) multipolygon",
          List.of(List.of(expectedMultiPolygon)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cells_to_multi_polygon(null) multipolygon",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT ST_AsText(h3_cells_to_multi_polygon(ARRAY [])) multipolygon",
          List.of(List.of("MULTIPOLYGON EMPTY")));
    }
  }
}
