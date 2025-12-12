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
public class HierarchyFunctionsTest {
  @Test
  public void testConstructor() {
    assertNotNull(new HierarchyFunctions());
  }

  @Test
  public void testCellToParent() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(from_base('85283473fffffff', 16), 4) hex",
          List.of(List.of(0x8428347ffffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(0, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(null, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(from_base('85283473fffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(from_base('85283473fffffff', 16), -1) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToChildren() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(from_base('85283473fffffff', 16), 6) hex",
          List.of(
              List.of(
                  List.of(
                      0x862834707ffffffL,
                      0x86283470fffffffL,
                      0x862834717ffffffL,
                      0x86283471fffffffL,
                      0x862834727ffffffL,
                      0x86283472fffffffL,
                      0x862834737ffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(0, 4) hex",
          List.of(Collections.singletonList(Collections.emptyList())));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(null, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(from_base('85283473fffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(from_base('85283473fffffff', 16), -1) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToCenterChild() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(from_base('85283473fffffff', 16), 6) hex",
          List.of(List.of(0x862834707ffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(0, 4) hex",
          List.of(Collections.singletonList(0x40000000000000L)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(null, 4) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(from_base('85283473fffffff', 16), null) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(from_base('85283473fffffff', 16), -1) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToChildrenSize() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      // res 5 -> res 6 (7 children)
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children_size(from_base('85283473fffffff', 16), 6)",
          List.of(List.of(7L)));

      // res 5 -> res 7 (49 children)
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children_size(from_base('85283473fffffff', 16), 7)",
          List.of(List.of(49L)));

      // Null tests
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children_size(null, 6)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children_size(from_base('85283473fffffff', 16), null)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children_size(from_base('85283473fffffff', 16), -1)",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToChildPos() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      // Get child position for multiple children of the same parent
      // Parent: 85283473fffffff (res 5)
      // Children at res 6: 862834707, 86283470f, 862834717, 86283471f, 862834727, 86283472f,
      // 862834737
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('862834707ffffff', 16), 5)",
          List.of(List.of(0L)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('86283470fffffff', 16), 5)",
          List.of(List.of(1L)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('862834717ffffff', 16), 5)",
          List.of(List.of(2L)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('86283471fffffff', 16), 5)",
          List.of(List.of(3L)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('862834727ffffff', 16), 5)",
          List.of(List.of(4L)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('86283472fffffff', 16), 5)",
          List.of(List.of(5L)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('862834737ffffff', 16), 5)",
          List.of(List.of(6L)));

      // Null tests
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(null, 5)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('862834707ffffff', 16), null)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('862834707ffffff', 16), -1)",
          List.of(Collections.singletonList(null)));
      // Test with invalid parent resolution (higher than child)
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_child_pos(from_base('862834707ffffff', 16), 7)",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testChildPosToCell() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      // Get children at all valid positions (0-6 for hexagon)
      // Parent: 85283473fffffff (res 5), Target res: 6
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(0, from_base('85283473fffffff', 16), 6)",
          List.of(List.of(0x862834707ffffffL)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(1, from_base('85283473fffffff', 16), 6)",
          List.of(List.of(0x86283470fffffffL)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(2, from_base('85283473fffffff', 16), 6)",
          List.of(List.of(0x862834717ffffffL)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(3, from_base('85283473fffffff', 16), 6)",
          List.of(List.of(0x86283471fffffffL)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(4, from_base('85283473fffffff', 16), 6)",
          List.of(List.of(0x862834727ffffffL)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(5, from_base('85283473fffffff', 16), 6)",
          List.of(List.of(0x86283472fffffffL)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(6, from_base('85283473fffffff', 16), 6)",
          List.of(List.of(0x862834737ffffffL)));

      // Test invalid position (out of range - hexagons have 7 children, positions 0-6)
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(7, from_base('85283473fffffff', 16), 6)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(100, from_base('85283473fffffff', 16), 6)",
          List.of(Collections.singletonList(null)));

      // Null tests
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(null, from_base('85283473fffffff', 16), 6)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(0, null, 6)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(0, from_base('85283473fffffff', 16), null)",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_child_pos_to_cell(0, from_base('85283473fffffff', 16), -1)",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCompactCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_compact_cells(h3_cell_to_children(from_base('85283473fffffff', 16), 7)) hex",
          List.of(List.of(List.of(0x85283473fffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_compact_cells(repeat(from_base('85283473fffffff', 16), 100)) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner, "SELECT h3_compact_cells(ARRAY []) hex", List.of(List.of(List.of())));
      assertQueryResults(
          queryRunner,
          "SELECT h3_compact_cells(null) hex",
          List.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testUncompactCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(ARRAY [from_base('85283473fffffff', 16), from_base('85283477fffffff', 16)], 7) hex",
          List.of(
              List.of(
                  List.of(
                      0x872834700ffffffL,
                      0x872834701ffffffL,
                      0x872834702ffffffL,
                      0x872834703ffffffL,
                      0x872834704ffffffL,
                      0x872834705ffffffL,
                      0x872834706ffffffL,
                      0x872834708ffffffL,
                      0x872834709ffffffL,
                      0x87283470affffffL,
                      0x87283470bffffffL,
                      0x87283470cffffffL,
                      0x87283470dffffffL,
                      0x87283470effffffL,
                      0x872834710ffffffL,
                      0x872834711ffffffL,
                      0x872834712ffffffL,
                      0x872834713ffffffL,
                      0x872834714ffffffL,
                      0x872834715ffffffL,
                      0x872834716ffffffL,
                      0x872834718ffffffL,
                      0x872834719ffffffL,
                      0x87283471affffffL,
                      0x87283471bffffffL,
                      0x87283471cffffffL,
                      0x87283471dffffffL,
                      0x87283471effffffL,
                      0x872834720ffffffL,
                      0x872834721ffffffL,
                      0x872834722ffffffL,
                      0x872834723ffffffL,
                      0x872834724ffffffL,
                      0x872834725ffffffL,
                      0x872834726ffffffL,
                      0x872834728ffffffL,
                      0x872834729ffffffL,
                      0x87283472affffffL,
                      0x87283472bffffffL,
                      0x87283472cffffffL,
                      0x87283472dffffffL,
                      0x87283472effffffL,
                      0x872834730ffffffL,
                      0x872834731ffffffL,
                      0x872834732ffffffL,
                      0x872834733ffffffL,
                      0x872834734ffffffL,
                      0x872834735ffffffL,
                      0x872834736ffffffL,
                      0x872834740ffffffL,
                      0x872834741ffffffL,
                      0x872834742ffffffL,
                      0x872834743ffffffL,
                      0x872834744ffffffL,
                      0x872834745ffffffL,
                      0x872834746ffffffL,
                      0x872834748ffffffL,
                      0x872834749ffffffL,
                      0x87283474affffffL,
                      0x87283474bffffffL,
                      0x87283474cffffffL,
                      0x87283474dffffffL,
                      0x87283474effffffL,
                      0x872834750ffffffL,
                      0x872834751ffffffL,
                      0x872834752ffffffL,
                      0x872834753ffffffL,
                      0x872834754ffffffL,
                      0x872834755ffffffL,
                      0x872834756ffffffL,
                      0x872834758ffffffL,
                      0x872834759ffffffL,
                      0x87283475affffffL,
                      0x87283475bffffffL,
                      0x87283475cffffffL,
                      0x87283475dffffffL,
                      0x87283475effffffL,
                      0x872834760ffffffL,
                      0x872834761ffffffL,
                      0x872834762ffffffL,
                      0x872834763ffffffL,
                      0x872834764ffffffL,
                      0x872834765ffffffL,
                      0x872834766ffffffL,
                      0x872834768ffffffL,
                      0x872834769ffffffL,
                      0x87283476affffffL,
                      0x87283476bffffffL,
                      0x87283476cffffffL,
                      0x87283476dffffffL,
                      0x87283476effffffL,
                      0x872834770ffffffL,
                      0x872834771ffffffL,
                      0x872834772ffffffL,
                      0x872834773ffffffL,
                      0x872834774ffffffL,
                      0x872834775ffffffL,
                      0x872834776ffffffL))));

      assertQueryResults(
          queryRunner, "SELECT h3_uncompact_cells(ARRAY [], 5) hex", List.of(List.of(List.of())));
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(null, 5) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(ARRAY [from_base('85283473fffffff', 16)], null) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(ARRAY [from_base('85283473fffffff', 16)], -1) hex",
          List.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(ARRAY [from_base('85283473fffffff', 16)], 16) hex",
          List.of(Collections.singletonList(null)));
    }
  }
}
