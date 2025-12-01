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

import static io.trino.geospatial.GeometryType.LINE_STRING;
import static io.trino.geospatial.GeometryType.POLYGON;
import static io.trino.geospatial.serde.JtsGeometrySerde.serialize;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import io.airlift.slice.Slice;
import io.trino.geospatial.GeometryType;
import io.trino.spi.Plugin;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class H3Plugin implements Plugin {
  static final String TYPE_ARRAY_BIGINT = "ARRAY(BIGINT)";
  static final String TYPE_ARRAY_INTEGER = "ARRAY(INTEGER)";

  static final H3Core H3;

  static {
    try {
      H3 = H3Core.newInstance();
    } catch (IOException e) {
      throw new RuntimeException("H3 setup failed", e);
    }
  }

  /**
   * Trino passes integer parameters in as `long`s in Java. These need to be cast down to `int` for
   * H3 functions. Throws if out of range.
   */
  static int longToInt(long l) {
    if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
      throw new RuntimeException("integer out of range");
    } else {
      return (int) l;
    }
  }

  static List<Long> longBlockToList(Block block) {
    List<Long> list = new ArrayList<>(block.getPositionCount());
    for (int i = 0; i < block.getPositionCount(); i++) {
      list.add(BIGINT.getLong(block, i));
    }
    return list;
  }

  static Block longListToBlock(List<Long> list) {
    BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(list.size());
    for (Long cell : list) {
      BIGINT.writeLong(blockBuilder, cell);
    }
    return blockBuilder.build();
  }

  static Slice latLngListToGeometry(List<LatLng> list, GeometryType geometryType) {
    Coordinate[] coordinates =
        list.stream().map(ll -> new Coordinate(ll.lng, ll.lat)).toArray(Coordinate[]::new);
    GeometryFactory geomFactory = new GeometryFactory();
    if (LINE_STRING.equals(geometryType)) {
      return serialize(geomFactory.createLineString(coordinates));
    } else if (POLYGON.equals(geometryType)) {
      return serialize(geomFactory.createPolygon(coordinates));
    } else {
      throw new IllegalArgumentException("Cannot serialize with GeometryType " + geometryType);
    }
  }

  static Block intListToBlock(List<Integer> list) {
    BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(list.size());
    for (Integer val : list) {
      INTEGER.writeLong(blockBuilder, val);
    }
    return blockBuilder.build();
  }

  static Slice latLngToGeometry(LatLng latLng) {
    GeometryFactory geomFactory = new GeometryFactory();
    Point point = geomFactory.createPoint(new Coordinate(latLng.lng, latLng.lat));
    return serialize(point);
  }

  @Override
  public Set<Class<?>> getFunctions() {
    return Set.of(
        IndexingFunctions.class,
        InspectionFunctions.class,
        HierarchyFunctions.class,
        TraversalFunctions.class,
        RegionFunctions.class,
        DirectedEdgeFunctions.class,
        VertexFunctions.class,
        MiscellaneousFunctions.class);
  }
}
