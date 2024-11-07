package org.roaringbitmap.realdata;

import static org.roaringbitmap.RealDataset.CENSUS1881;
import static org.roaringbitmap.RealDataset.CENSUS1881_SRT;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME_SRT;
import static org.roaringbitmap.RealDataset.DIMENSION_003;
import static org.roaringbitmap.RealDataset.DIMENSION_008;
import static org.roaringbitmap.RealDataset.DIMENSION_033;
import static org.roaringbitmap.RealDataset.USCENSUS2000;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85_SRT;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES_SRT;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.CONCISE;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.EWAH;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.EWAH32;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING_ONLY;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING_WITH_RUN;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.WAH;

import org.roaringbitmap.realdata.state.RealDataBenchmarkState;
import org.roaringbitmap.realdata.wrapper.BitmapFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public abstract class RealDataBenchmarkSanityTest {

  public static final String[] REAL_DATA_SETS = {
    CENSUS_INCOME,
    CENSUS1881,
    DIMENSION_008,
    DIMENSION_003,
    DIMENSION_033,
    USCENSUS2000,
    WEATHER_SEPT_85,
    WIKILEAKS_NOQUOTES,
    CENSUS_INCOME_SRT,
    CENSUS1881_SRT,
    WEATHER_SEPT_85_SRT,
    WIKILEAKS_NOQUOTES_SRT
  };

  public static final String[] BITMAP_TYPES =
      ROARING_ONLY.equals(System.getProperty(BitmapFactory.BITMAP_TYPES))
          ? new String[] {ROARING, ROARING_WITH_RUN}
          : new String[] {CONCISE, WAH, EWAH, EWAH32, ROARING, ROARING_WITH_RUN};

  public static final Boolean[] BITMAP_IMMUTABILITY = {false, true};

  // Ensure all tests related to the same dataset are run consecutively in order to take advantage
  // of any cache
  public static Stream<Arguments> params() {
    Arguments[] product =
        new Arguments[REAL_DATA_SETS.length * BITMAP_TYPES.length * BITMAP_IMMUTABILITY.length];
    int i = 0;
    for (String ds : REAL_DATA_SETS) {
      for (String type : BITMAP_TYPES) {
        for (boolean mutability : BITMAP_IMMUTABILITY) {
          product[i++] = Arguments.of(ds, type, mutability);
        }
      }
    }
    return Arrays.stream(product);
  }

  protected RealDataBenchmarkState bs;

  @ParameterizedTest
  @MethodSource("params")
  public void test(String dataset, String type, boolean immutable) throws Exception {
    bs.dataset = dataset;
    bs.type = type;
    bs.immutable = immutable;
    bs.setup();
    doTest(dataset, type, immutable);
  }

  @BeforeEach
  public void setup() {
    bs = new RealDataBenchmarkState();
  }

  @AfterEach
  public void tearDown() {
    if (bs != null) {
      bs.tearDown();
    }
  }

  protected abstract void doTest(String dataset, String type, boolean immutable);
}
