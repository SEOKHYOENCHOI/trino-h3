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

import static io.trino.geospatial.serde.JtsGeometrySerde.deserialize;
import static io.trino.geospatial.serde.JtsGeometrySerde.serialize;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static org.locationtech.jts.geom.Geometry.TYPENAME_POLYGON;

import com.uber.h3core.util.LatLng;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

/** Functions wrapping https://h3geo.org/docs/api/regions */
public final class RegionFunctions {
  @ScalarFunction(value = "h3_polygon_to_cells")
  @Description("Convert a polygon to H3 cells")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block polygonToCells(
      @SqlType(GEOMETRY_TYPE_NAME) Slice polygonSlice, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      Geometry polygonGeomUntyped = deserialize(polygonSlice);
      if (!TYPENAME_POLYGON.equals(polygonGeomUntyped.getGeometryType())) {
        throw new IllegalArgumentException("Invalid polygon geometry");
      }
      Polygon polygonGeom = (Polygon) polygonGeomUntyped;
      List<LatLng> polygon = linearRingTolatLngList(polygonGeom.getExteriorRing());

      List<List<LatLng>> holes =
          IntStream.range(0, polygonGeom.getNumInteriorRing())
              .mapToObj(polygonGeom::getInteriorRingN)
              .map(RegionFunctions::linearRingTolatLngList)
              .collect(Collectors.toList());

      List<Long> cells = H3Plugin.H3.polygonToCells(polygon, holes, H3Plugin.longToInt(res));
      return H3Plugin.longListToBlock(cells);
    } catch (Exception e) {
      return null;
    }
  }

  static List<LatLng> linearRingTolatLngList(LinearRing ring) {
    return Arrays.stream(ring.getCoordinates())
        .map(c -> new LatLng(c.getY(), c.getX()))
        .collect(Collectors.toList());
  }

  @ScalarFunction(value = "h3_cells_to_multi_polygon")
  @Description("Find the multipolygon of the given cells")
  @SqlNullable
  @SqlType(GEOMETRY_TYPE_NAME)
  public static Slice cellsToMultiPolygon(@SqlType(H3Plugin.TYPE_ARRAY_BIGINT) Block h3Block) {
    try {
      List<Long> cells = H3Plugin.longBlockToList(h3Block);
      List<List<List<LatLng>>> multiPolygon = H3Plugin.H3.cellsToMultiPolygon(cells, true);

      GeometryFactory geomFactory = new GeometryFactory();

      Polygon[] polygons =
          multiPolygon.stream()
              .map(
                  polygon ->
                      geomFactory.createPolygon(
                          latLngListToLinearRing(geomFactory, polygon.get(0)),
                          polygon.subList(1, polygon.size()).stream()
                              .map(ring -> latLngListToLinearRing(geomFactory, ring))
                              .collect(Collectors.toList())
                              .toArray(new LinearRing[0])))
              .collect(Collectors.toList())
              .toArray(new Polygon[multiPolygon.size()]);

      MultiPolygon result = geomFactory.createMultiPolygon(polygons);
      return serialize(result);
    } catch (Exception e) {
      return null;
    }
  }

  static LinearRing latLngListToLinearRing(GeometryFactory geomFactory, List<LatLng> latLngs) {
    return geomFactory.createLinearRing(
        latLngs.stream()
            .map(latLng -> new Coordinate(latLng.lng, latLng.lat))
            .collect(Collectors.toList())
            .toArray(new Coordinate[latLngs.size()]));
  }
}
