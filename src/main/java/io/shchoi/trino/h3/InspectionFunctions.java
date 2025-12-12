package io.shchoi.trino.h3;

import static io.trino.spi.type.IntegerType.INTEGER;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import java.util.Collection;

/** Wraps https://h3geo.org/docs/api/inspection/ */
public final class InspectionFunctions {
  @ScalarFunction(value = "h3_get_resolution")
  @Description("Convert H3 index to resolution (0-15)")
  @SqlNullable
  @SqlType(StandardTypes.INTEGER)
  public static Long getResolution(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return (long) H3Plugin.H3.getResolution(h3);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_get_base_cell_number")
  @Description("Convert H3 index to base cell number (0-122)")
  @SqlNullable
  @SqlType(StandardTypes.INTEGER)
  public static Long getBaseCellNumber(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return (long) H3Plugin.H3.getBaseCellNumber(h3);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_string_to_h3")
  @Description("Convert H3 index string to integer form")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long stringToH3(@SqlType(StandardTypes.VARCHAR) Slice h3) {
    try {
      return H3Plugin.H3.stringToH3(h3.toStringUtf8());
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_h3_to_string")
  @Description("Convert H3 index integer to string form")
  @SqlNullable
  @SqlType(StandardTypes.VARCHAR)
  public static Slice h3ToString(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return Slices.utf8Slice(H3Plugin.H3.h3ToString(h3));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_is_valid_cell")
  @Description("Returns true if given a valid H3 cell identifier")
  @SqlNullable
  @SqlType(StandardTypes.BOOLEAN)
  public static Boolean isValidCell(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return H3Plugin.H3.isValidCell(h3);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_is_res_class_iii")
  @Description("Returns true if the index is in resolution class III")
  @SqlNullable
  @SqlType(StandardTypes.BOOLEAN)
  public static Boolean isResClassIII(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return H3Plugin.H3.isResClassIII(h3);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_is_pentagon")
  @Description("Returns true if the cell index is a pentagon")
  @SqlNullable
  @SqlType(StandardTypes.BOOLEAN)
  public static Boolean isPentagon(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return H3Plugin.H3.isPentagon(h3);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_get_icosahedron_faces")
  @Description("Convert H3 index to icosahedron face IDs")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_INTEGER)
  public static Block getIcosahedronFaces(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      Collection<Integer> faces = H3Plugin.H3.getIcosahedronFaces(h3);
      BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(faces.size());
      for (Integer face : faces) {
        INTEGER.writeLong(blockBuilder, face);
      }
      return blockBuilder.build();
    } catch (Exception e) {
      return null;
    }
  }
}
