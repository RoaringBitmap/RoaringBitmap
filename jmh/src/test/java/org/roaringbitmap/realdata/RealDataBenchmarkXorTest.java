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

public class RealDataBenchmarkXorTest extends RealDataBenchmarkSanityTest {

  private static final Map<String, Integer> EXPECTED_RESULTS =
      ImmutableMap.<String, Integer>builder()
          .put(CENSUS_INCOME, 11241947)
          .put(CENSUS1881, 2007668)
          .put(DIMENSION_008, 5442916)
          .put(DIMENSION_003, 7733676)
          .put(DIMENSION_033, 7579526)
          .put(USCENSUS2000, 11954)
          .put(WEATHER_SEPT_85, 24086983)
          .put(WIKILEAKS_NOQUOTES, 538566)
          .put(CENSUS_INCOME_SRT, 10329567)
          .put(CENSUS1881_SRT, 1359961)
          .put(WEATHER_SEPT_85_SRT, 29800358)
          .put(WIKILEAKS_NOQUOTES_SRT, 574311)
          .build();

  @Override
  protected void doTest(String dataset, String type, boolean immutable) {
    assumeFalse(type.equals(CONCISE) && immutable);
    assumeFalse(type.equals(WAH) && immutable);
    int expected = EXPECTED_RESULTS.get(dataset);
    RealDataBenchmarkXor bench = new RealDataBenchmarkXor();
    assertEquals(expected, bench.pairwiseXor(bs));
  }
}
