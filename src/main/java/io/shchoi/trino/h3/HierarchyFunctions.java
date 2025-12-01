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

import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import java.util.List;

/** Function wrapping {@link com.uber.h3core.H3Core#cellToParent(long, int)} */
public final class HierarchyFunctions {
  @ScalarFunction(value = "h3_cell_to_parent")
  @Description("Truncate H3 index to parent")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long cellToParent(
      @SqlType(StandardTypes.BIGINT) long cell, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      return H3Plugin.H3.cellToParent(cell, H3Plugin.longToInt(res));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_children")
  @Description("Find children of an H3 index at given resolution")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block cellToChildren(
      @SqlType(StandardTypes.BIGINT) long cell, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      List<Long> children = H3Plugin.H3.cellToChildren(cell, H3Plugin.longToInt(res));
      return H3Plugin.longListToBlock(children);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_center_child")
  @Description("Find the center child of an H3 index at a given resolution")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long cellToCenterChild(
      @SqlType(StandardTypes.BIGINT) long cell, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      return H3Plugin.H3.cellToCenterChild(cell, H3Plugin.longToInt(res));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_children_size")
  @Description("Returns the number of children at the given resolution")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long cellToChildrenSize(
      @SqlType(StandardTypes.BIGINT) long cell, @SqlType(StandardTypes.INTEGER) long childRes) {
    try {
      return H3Plugin.H3.cellToChildrenSize(cell, H3Plugin.longToInt(childRes));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_child_pos")
  @Description("Returns the position of the child cell within the parent")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long cellToChildPos(
      @SqlType(StandardTypes.BIGINT) long child, @SqlType(StandardTypes.INTEGER) long parentRes) {
    try {
      return H3Plugin.H3.cellToChildPos(child, H3Plugin.longToInt(parentRes));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_child_pos_to_cell")
  @Description("Returns the child cell at the given position")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long childPosToCell(
      @SqlType(StandardTypes.BIGINT) long childPos,
      @SqlType(StandardTypes.BIGINT) long parent,
      @SqlType(StandardTypes.INTEGER) long childRes) {
    try {
      return H3Plugin.H3.childPosToCell(childPos, parent, H3Plugin.longToInt(childRes));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_compact_cells")
  @Description("Compact indexes to coarser resolutions")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block compactCells(@SqlType(H3Plugin.TYPE_ARRAY_BIGINT) Block cellsBlock) {
    try {
      List<Long> cells = H3Plugin.longBlockToList(cellsBlock);
      List<Long> compacted = H3Plugin.H3.compactCells(cells);
      return H3Plugin.longListToBlock(compacted);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_uncompact_cells")
  @Description("Uncompact indexes to finer resolutions")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block uncompactCells(
      @SqlType(H3Plugin.TYPE_ARRAY_BIGINT) Block cellsBlock,
      @SqlType(StandardTypes.INTEGER) long res) {
    try {
      List<Long> cells = H3Plugin.longBlockToList(cellsBlock);
      List<Long> uncompacted = H3Plugin.H3.uncompactCells(cells, H3Plugin.longToInt(res));
      return H3Plugin.longListToBlock(uncompacted);
    } catch (Exception e) {
      return null;
    }
  }
}
