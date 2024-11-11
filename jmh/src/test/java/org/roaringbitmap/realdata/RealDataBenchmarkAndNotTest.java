package org.roaringbitmap.realdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
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
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.WAH;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class RealDataBenchmarkAndNotTest extends RealDataBenchmarkSanityTest {

  private static final Map<String, Integer> EXPECTED_RESULTS =
      ImmutableMap.<String, Integer>builder()
          .put(CENSUS_INCOME, 5666586)
          .put(CENSUS1881, 1003836)
          .put(DIMENSION_008, 2721459)
          .put(DIMENSION_003, 3866831)
          .put(DIMENSION_033, 3866842)
          .put(USCENSUS2000, 5970)
          .put(WEATHER_SEPT_85, 11960876)
          .put(WIKILEAKS_NOQUOTES, 271605)
          .put(CENSUS_INCOME_SRT, 5164671)
          .put(CENSUS1881_SRT, 679375)
          .put(WEATHER_SEPT_85_SRT, 14935706)
          .put(WIKILEAKS_NOQUOTES_SRT, 286904)
          .build();

  @Override
  protected void doTest(String dataset, String type, boolean immutable) {
    assumeFalse(type.equals(CONCISE) && immutable);
    assumeFalse(type.equals(WAH) && immutable);
    int expected = EXPECTED_RESULTS.get(dataset);
    RealDataBenchmarkAndNot bench = new RealDataBenchmarkAndNot();
    assertEquals(expected, bench.pairwiseAndNot(bs));
  }
}
