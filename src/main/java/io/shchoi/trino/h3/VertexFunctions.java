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

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;

import com.uber.h3core.util.LatLng;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import java.util.List;

/** Wraps https://h3geo.org/docs/api/vertex */
public final class VertexFunctions {
  @ScalarFunction(value = "h3_cell_to_vertex")
  @Description("Finds an index for the specified topological cell vertex")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long cellToVertex(
      @SqlType(StandardTypes.BIGINT) long cell, @SqlType(StandardTypes.INTEGER) long vertexNum) {
    try {
      return H3Plugin.H3.cellToVertex(cell, H3Plugin.longToInt(vertexNum));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_vertexes")
  @Description("Finds indexes for the topological vertexes of a cell")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block cellToVertexes(@SqlType(StandardTypes.BIGINT) long cell) {
    try {
      List<Long> vertexes = H3Plugin.H3.cellToVertexes(cell);
      return H3Plugin.longListToBlock(vertexes);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_vertex_to_latlng")
  @Description("Finds coordinates of a topological vertex index")
  @SqlNullable
  @SqlType(GEOMETRY_TYPE_NAME)
  public static Slice vertexToLatLng(@SqlType(StandardTypes.BIGINT) long vertex) {
    try {
      LatLng latLng = H3Plugin.H3.vertexToLatLng(vertex);
      return H3Plugin.latLngToGeometry(latLng);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_is_valid_vertex")
  @Description("Returns true if this is a valid vertex index")
  @SqlType(StandardTypes.BOOLEAN)
  public static boolean isValidVertex(@SqlType(StandardTypes.BIGINT) long vertex) {
    return H3Plugin.H3.isValidVertex(vertex);
  }
}
