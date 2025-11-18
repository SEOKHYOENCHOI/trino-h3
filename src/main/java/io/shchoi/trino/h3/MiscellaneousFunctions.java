package io.shchoi.trino.h3;

import com.uber.h3core.AreaUnit;
import com.uber.h3core.LengthUnit;
import com.uber.h3core.util.LatLng;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import java.util.ArrayList;

/** Wraps https://h3geo.org/docs/api/misc */
public final class MiscellaneousFunctions {
  @ScalarFunction(value = "h3_get_hexagon_area_avg")
  @Description("Get average area of hexagon cells (unit may be km2 or m2)")
  @SqlNullable
  @SqlType(StandardTypes.DOUBLE)
  public static Double getHexagonAreaAvg(
      @SqlType(StandardTypes.INTEGER) long res, @SqlType(StandardTypes.VARCHAR) Slice unit) {
    try {
      return H3Plugin.H3.getHexagonAreaAvg(
          H3Plugin.longToInt(res), AreaUnit.valueOf(unit.toStringUtf8()));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_area")
  @Description("Get area of a cells (unit may be rads2, km2 or m2)")
  @SqlNullable
  @SqlType(StandardTypes.DOUBLE)
  public static Double cellArea(
      @SqlType(StandardTypes.BIGINT) long cell, @SqlType(StandardTypes.VARCHAR) Slice unit) {
    try {
      return H3Plugin.H3.cellArea(cell, AreaUnit.valueOf(unit.toStringUtf8()));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_get_hexagon_edge_length_avg")
  @Description("Get average edge length of hexagon cells (unit may be rads, km or m)")
  @SqlNullable
  @SqlType(StandardTypes.DOUBLE)
  public static Double getHexagonEdgeLengthAvg(
      @SqlType(StandardTypes.INTEGER) long res, @SqlType(StandardTypes.VARCHAR) Slice unit) {
    try {
      return H3Plugin.H3.getHexagonEdgeLengthAvg(
          H3Plugin.longToInt(res), LengthUnit.valueOf(unit.toStringUtf8()));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_edge_length")
  @Description("Get edge length of an edge index (unit may be rads, km or m)")
  @SqlNullable
  @SqlType(StandardTypes.DOUBLE)
  public static Double edgeLength(
      @SqlType(StandardTypes.BIGINT) long edge, @SqlType(StandardTypes.VARCHAR) Slice unit) {
    try {
      return H3Plugin.H3.edgeLength(edge, LengthUnit.valueOf(unit.toStringUtf8()));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_great_circle_distance")
  @Description(
      "Get great circle distance (Haversine) between two points (unit may be rads, km or m)")
  @SqlNullable
  @SqlType(StandardTypes.DOUBLE)
  public static Double greatCircleDistance(
      @SqlType(StandardTypes.DOUBLE) double lat1,
      @SqlType(StandardTypes.DOUBLE) double lng1,
      @SqlType(StandardTypes.DOUBLE) double lat2,
      @SqlType(StandardTypes.DOUBLE) double lng2,
      @SqlType(StandardTypes.VARCHAR) Slice unit) {
    try {
      return H3Plugin.H3.greatCircleDistance(
          new LatLng(lat1, lng1), new LatLng(lat2, lng2), LengthUnit.valueOf(unit.toStringUtf8()));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_get_num_cells")
  @Description("Get number of cells at a resolution")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long getNumCells(@SqlType(StandardTypes.INTEGER) long res) {
    try {
      return H3Plugin.H3.getNumCells(H3Plugin.longToInt(res));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_get_res0_cells")
  @Description("Get all resolution 0 cells")
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block getRes0Cells() {
    return H3Plugin.longListToBlock(new ArrayList<>(H3Plugin.H3.getRes0Cells()));
  }

  @ScalarFunction(value = "h3_get_pentagons")
  @Description("Get all pentagon cells at a resolution")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block getPentagons(@SqlType(StandardTypes.INTEGER) long res) {
    try {
      return H3Plugin.longListToBlock(
          new ArrayList<>(H3Plugin.H3.getPentagons(H3Plugin.longToInt(res))));
    } catch (Exception e) {
      return null;
    }
  }
}
