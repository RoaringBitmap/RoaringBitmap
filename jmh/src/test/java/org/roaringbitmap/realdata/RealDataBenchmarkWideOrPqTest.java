package org.roaringbitmap.realdata;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.roaringbitmap.RealDataset.*;


public class RealDataBenchmarkWideOrPqTest extends RealDataBenchmarkSanityTest {

  private static final Map<String, Integer> EXPECTED_RESULTS =
      ImmutableMap.<String, Integer>builder().put(CENSUS_INCOME, 199523).put(CENSUS1881, 988653)
          .put(DIMENSION_008, 148278).put(DIMENSION_003, 3866847).put(DIMENSION_033, 3866847)
          .put(USCENSUS2000, 5985).put(WEATHER_SEPT_85, 1015367).put(WIKILEAKS_NOQUOTES, 242540)
          .put(CENSUS_INCOME_SRT, 199523).put(CENSUS1881_SRT, 656346)
          .put(WEATHER_SEPT_85_SRT, 1015367).put(WIKILEAKS_NOQUOTES_SRT, 236436).build();

  @Override
  protected void doTest(String dataset, String type, boolean immutable) {
    int expected = EXPECTED_RESULTS.get(dataset);
    RealDataBenchmarkWideOrPq bench = new RealDataBenchmarkWideOrPq();
    assertEquals(expected, bench.wideOr_pq(bs));
  }
}
