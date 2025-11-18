package io.shchoi.trino.h3;

import static io.trino.geospatial.GeometryType.POLYGON;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserialize;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static org.locationtech.jts.geom.Geometry.TYPENAME_POINT;

import com.uber.h3core.util.LatLng;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/** Wraps https://h3geo.org/docs/api/indexing */
public final class IndexingFunctions {
  /** Function wrapping {@link com.uber.h3core.H3Core#latLngToCell(double, double, int)} */
  @ScalarFunction(value = "h3_latlng_to_cell")
  @Description("Convert degrees lat/lng to H3 index")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long latLngToCell(
      @SqlType(StandardTypes.DOUBLE) double lat,
      @SqlType(StandardTypes.DOUBLE) double lng,
      @SqlType(StandardTypes.INTEGER) long res) {
    try {
      return H3Plugin.H3.latLngToCell(lat, lng, H3Plugin.longToInt(res));
    } catch (Exception e) {
      return null;
    }
  }

  /** Function wrapping {@link com.uber.h3core.H3Core#latLngToCell(double, double, int)} */
  @ScalarFunction(value = "h3_latlng_to_cell")
  @Description("Convert degrees lat/lng to H3 index")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long latLngToCell(
      @SqlType(GEOMETRY_TYPE_NAME) Slice pointSlice, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      Geometry pointGeomUntyped = deserialize(pointSlice);
      if (!TYPENAME_POINT.equals(pointGeomUntyped.getGeometryType())) {
        throw new IllegalArgumentException("Invalid polygon geometry");
      }
      Point pointGeom = (Point) pointGeomUntyped;

      return H3Plugin.H3.latLngToCell(pointGeom.getY(), pointGeom.getX(), H3Plugin.longToInt(res));
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Wraps {@link com.uber.h3core.H3Core#cellToLatLng(long)}. Produces a row of latitude, longitude
   * degrees.
   */
  @ScalarFunction(value = "h3_cell_to_latlng")
  @Description("Convert H3 index to degrees lat/lng")
  @SqlNullable
  @SqlType(GEOMETRY_TYPE_NAME)
  public static Slice cellToLatLng(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      LatLng latLng = H3Plugin.H3.cellToLatLng(h3);
      return H3Plugin.latLngToGeometry(latLng);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Wraps {@link com.uber.h3core.H3Core#cellToBoundary(long)}. Produces a row of latitude,
   * longitude degrees interleaved as (lat0, lng0, lat1, lng1, ..., latN, lngN).
   */
  @ScalarFunction(value = "h3_cell_to_boundary")
  @Description("Convert H3 index to boundary degrees lat/lng, interleaved")
  @SqlNullable
  @SqlType(GEOMETRY_TYPE_NAME)
  public static Slice cellToBoundary(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      List<LatLng> boundary = H3Plugin.H3.cellToBoundary(h3);
      // Duplicate the first point at the end to form a closed ring
      boundary.add(boundary.get(0));
      return H3Plugin.latLngListToGeometry(boundary, POLYGON);
    } catch (Exception e) {
      return null;
    }
  }
}
