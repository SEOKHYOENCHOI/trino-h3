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

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;

import com.uber.h3core.util.CoordIJ;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowValueBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Wraps https://h3geo.org/docs/api/traversal */
public final class TraversalFunctions {
  @ScalarFunction(value = "h3_grid_disk")
  @Description("Finds all nearby cells in a disk around the origin")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block gridDisk(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.INTEGER) long k) {
    try {
      List<Long> disk = H3Plugin.H3.gridDisk(origin, H3Plugin.longToInt(k));
      return H3Plugin.longListToBlock(disk);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_grid_disk_distances")
  @Description("Finds all nearby cells in a disk around the origin, grouped by distance")
  @SqlNullable
  @SqlType("ARRAY(ARRAY(BIGINT))")
  public static Block gridDiskDistances(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.INTEGER) long k) {
    try {
      List<List<Long>> disksByDistance =
          H3Plugin.H3.gridDiskDistances(origin, H3Plugin.longToInt(k));

      // Build all inner arrays and concatenate their elements
      int totalElements = disksByDistance.stream().mapToInt(List::size).sum();
      BlockBuilder allElementsBuilder = BIGINT.createBlockBuilder(null, totalElements);
      int[] innerOffsets = new int[disksByDistance.size() + 1];
      innerOffsets[0] = 0;

      int idx = 0;
      for (List<Long> distanceGroup : disksByDistance) {
        for (Long cell : distanceGroup) {
          BIGINT.writeLong(allElementsBuilder, cell);
        }
        innerOffsets[idx + 1] = innerOffsets[idx] + distanceGroup.size();
        idx++;
      }

      // Create ArrayBlock representing ARRAY(ARRAY(BIGINT))
      // The scalar function returns one value - the outer array containing inner arrays
      Block allElements = allElementsBuilder.build();
      return ArrayBlock.fromElementBlock(
          disksByDistance.size(), Optional.empty(), innerOffsets, allElements);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_grid_disk_unsafe")
  @Description(
      "Efficiently finds all nearby cells in a disk around the origin, but will return null if a pentagon is encountered")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block gridDiskUnsafe(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.INTEGER) long k) {
    try {
      List<Long> disk =
          H3Plugin.H3.gridDiskUnsafe(origin, H3Plugin.longToInt(k)).stream()
              .flatMap(List::stream)
              .collect(Collectors.toList());
      return H3Plugin.longListToBlock(disk);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_grid_ring_unsafe")
  @Description(
      "Efficiently finds nearby cells in a ring of distance k around the origin, but will return null if a pentagon is encountered")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block gridRingUnsafe(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.INTEGER) long k) {
    try {
      List<Long> disk = H3Plugin.H3.gridRingUnsafe(origin, H3Plugin.longToInt(k));
      return H3Plugin.longListToBlock(disk);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_grid_ring")
  @Description("Finds cells in a ring of distance k around the origin")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block gridRing(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.INTEGER) long k) {
    try {
      List<Long> ring = H3Plugin.H3.gridRing(origin, H3Plugin.longToInt(k));
      return H3Plugin.longListToBlock(ring);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_grid_path_cells")
  @Description("Finds cells comprising a path between origin and destination, in order")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block gridPathCells(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.BIGINT) long destination) {
    try {
      List<Long> path = H3Plugin.H3.gridPathCells(origin, destination);
      return H3Plugin.longListToBlock(path);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_grid_distance")
  @Description("Finds distance in grid cells between origin and destination")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long gridDistance(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.BIGINT) long destination) {
    try {
      return H3Plugin.H3.gridDistance(origin, destination);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_local_ij")
  @Description("Finds local IJ coordinates for a cell, returns ROW(i INTEGER, j INTEGER)")
  @SqlNullable
  @SqlType("ROW(i INTEGER, j INTEGER)")
  public static SqlRow cellToLocalIj(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.BIGINT) long cell) {
    try {
      CoordIJ ij = H3Plugin.H3.cellToLocalIj(origin, cell);
      RowType rowType =
          RowType.from(
              List.of(
                  new RowType.Field(Optional.of("i"), INTEGER),
                  new RowType.Field(Optional.of("j"), INTEGER)));
      return RowValueBuilder.buildRowValue(
          rowType,
          fieldBuilders -> {
            INTEGER.writeLong(fieldBuilders.get(0), ij.i);
            INTEGER.writeLong(fieldBuilders.get(1), ij.j);
          });
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_local_ij_to_cell")
  @Description("Finds cell given local IJ coordinates as ROW(i INTEGER, j INTEGER)")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long localIjToCell(
      @SqlType(StandardTypes.BIGINT) long origin,
      @SqlType("ROW(i INTEGER, j INTEGER)") SqlRow ijRow) {
    try {
      int rawIndex = ijRow.getRawIndex();
      int i = INTEGER.getInt(ijRow.getRawFieldBlock(0), rawIndex);
      int j = INTEGER.getInt(ijRow.getRawFieldBlock(1), rawIndex);
      CoordIJ ij = new CoordIJ(i, j);
      return H3Plugin.H3.localIjToCell(origin, ij);
    } catch (Exception e) {
      return null;
    }
  }
}
