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

@TestInstance(Lifecycle.PER_CLASS)
public class TraversalFunctionsTest {
  @Test
  public void testConstructor() {
    assertNotNull(new TraversalFunctions());
  }

  @Test
  public void testGridDisk() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk(from_base('85283473fffffff', 16), 0), h3_grid_disk(from_base('85283473fffffff', 16), 2)",
          List.of(
              List.of(
                  List.of(0x85283473fffffffL),
                  List.of(
                      0x85283473fffffffL,
                      0x85283447fffffffL,
                      0x8528347bfffffffL,
                      0x85283463fffffffL,
                      0x85283477fffffffL,
                      0x8528340ffffffffL,
                      0x8528340bfffffffL,
                      0x85283457fffffffL,
                      0x85283443fffffffL,
                      0x8528344ffffffffL,
                      0x852836b7fffffffL,
                      0x8528346bfffffffL,
                      0x8528346ffffffffL,
                      0x85283467fffffffL,
                      0x8528342bfffffffL,
                      0x8528343bfffffffL,
                      0x85283407fffffffL,
                      0x85283403fffffffL,
                      0x8528341bfffffffL))));

      assertQueryResults(queryRunner, "SELECT h3_grid_disk(0, 4) hex", List.of(List.of(List.of())));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk(null, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk(from_base('85283473fffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk(from_base('85283473fffffff', 16), -1) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridDiskDistances() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      // Test k=0 (single cell)
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_distances(from_base('85283473fffffff', 16), 0)",
          List.of(List.of(List.of(List.of(0x85283473fffffffL)))));

      // Test k=1 (origin + 1 ring)
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_distances(from_base('85283473fffffff', 16), 1)",
          List.of(
              List.of(
                  List.of(
                      List.of(0x85283473fffffffL),
                      List.of(
                          0x85283447fffffffL,
                          0x8528347bfffffffL,
                          0x85283463fffffffL,
                          0x85283477fffffffL,
                          0x8528340ffffffffL,
                          0x8528340bfffffffL)))));

      // Null tests
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_distances(null, 1)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_distances(from_base('85283473fffffff', 16), null)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_distances(from_base('85283473fffffff', 16), -1)",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridDiskUnsafe() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(from_base('85283473fffffff', 16), 0), h3_grid_disk_unsafe(from_base('85283473fffffff', 16), 2)",
          List.of(
              List.of(
                  List.of(0x85283473fffffffL),
                  List.of(
                      0x85283473fffffffL,
                      0x85283447fffffffL,
                      0x8528347bfffffffL,
                      0x85283463fffffffL,
                      0x85283477fffffffL,
                      0x8528340ffffffffL,
                      0x8528340bfffffffL,
                      0x85283457fffffffL,
                      0x85283443fffffffL,
                      0x8528344ffffffffL,
                      0x852836b7fffffffL,
                      0x8528346bfffffffL,
                      0x8528346ffffffffL,
                      0x85283467fffffffL,
                      0x8528342bfffffffL,
                      0x8528343bfffffffL,
                      0x85283407fffffffL,
                      0x85283403fffffffL,
                      0x8528341bfffffffL))));
      // Pentagon:
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(from_base('811c3ffffffffff', 16), 1) hex",
          List.of(Collections.singletonList(null)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(0, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(null, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(from_base('85283473fffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(from_base('85283473fffffff', 16), -1) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridRing() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      // Test k=0 (origin cell)
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring(from_base('85283473fffffff', 16), 0)",
          List.of(List.of(List.of(0x85283473fffffffL))));

      // Test k=1 (first ring)
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring(from_base('85283473fffffff', 16), 1)",
          List.of(
              List.of(
                  List.of(
                      0x8528340bfffffffL,
                      0x85283447fffffffL,
                      0x8528347bfffffffL,
                      0x85283463fffffffL,
                      0x85283477fffffffL,
                      0x8528340ffffffffL))));

      // Null tests
      assertQueryResults(
          queryRunner, "SELECT h3_grid_ring(null, 1)", List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring(from_base('85283473fffffff', 16), null)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring(from_base('85283473fffffff', 16), -1)",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridRingUnsafe() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(from_base('85283473fffffff', 16), 0), h3_grid_ring_unsafe(from_base('85283473fffffff', 16), 2)",
          List.of(
              List.of(
                  List.of(0x85283473fffffffL),
                  List.of(
                      0x8528341bfffffffL,
                      0x85283457fffffffL,
                      0x85283443fffffffL,
                      0x8528344ffffffffL,
                      0x852836b7fffffffL,
                      0x8528346bfffffffL,
                      0x8528346ffffffffL,
                      0x85283467fffffffL,
                      0x8528342bfffffffL,
                      0x8528343bfffffffL,
                      0x85283407fffffffL,
                      0x85283403fffffffL))));
      // Pentagon:
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(from_base('811c3ffffffffff', 16), 1) hex",
          List.of(Collections.singletonList(null)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(0, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(null, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(from_base('85283473fffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(from_base('85283473fffffff', 16), -1) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridPathCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(from_base('85283473fffffff', 16), from_base('8528342ffffffff', 16)) hex",
          List.of(
              List.of(
                  List.of(
                      0x85283473fffffffL,
                      0x85283477fffffffL,
                      0x8528342bfffffffL,
                      0x8528342ffffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(0, from_base('8528342ffffffff', 16)) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(null, from_base('8528342ffffffff', 16)) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(from_base('8528342ffffffff', 16), 0) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(from_base('8528342ffffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridDistance() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(from_base('85283473fffffff', 16), from_base('8528342ffffffff', 16)) hex",
          List.of(List.of(3L)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(0, from_base('8528342ffffffff', 16)) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(null, from_base('8528342ffffffff', 16)) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(from_base('8528342ffffffff', 16), 0) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(from_base('8528342ffffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToLocalIj() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      // Test ROW type return - access fields using .i and .j
      assertQueryResults(
          queryRunner,
          "SELECT r.i, r.j FROM (SELECT h3_cell_to_local_ij(from_base('85283473fffffff', 16), from_base('8528342ffffffff', 16)) AS r)",
          List.of(List.of(24, 12)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_local_ij(0, from_base('8528342ffffffff', 16)) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_local_ij(null, from_base('8528342ffffffff', 16)) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_local_ij(from_base('8528342ffffffff', 16), 0) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_local_ij(from_base('8528342ffffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testLocalIjToCell() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      // Test with ROW type - using CAST to match the expected signature
      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(from_base('85283473fffffff', 16), CAST(ROW(0, 0) AS ROW(i INTEGER, j INTEGER))) hex",
          List.of(List.of(0x85280003fffffffL)));

      // Test with invalid coordinates
      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(from_base('85283473fffffff', 16), CAST(ROW(1000000000, 0) AS ROW(i INTEGER, j INTEGER))) hex",
          List.of(Collections.singletonList(null)));

      // Test chaining with h3_cell_to_local_ij
      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(from_base('85283473fffffff', 16), h3_cell_to_local_ij(from_base('85283473fffffff', 16), from_base('8528342ffffffff', 16))) hex",
          List.of(List.of(0x8528342ffffffffL)));

      // Null tests
      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(null, CAST(ROW(0, 0) AS ROW(i INTEGER, j INTEGER))) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(from_base('8528342ffffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
    }
  }
}
