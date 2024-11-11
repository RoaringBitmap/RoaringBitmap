package org.roaringbitmap.realdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class RealDataBenchmarkAndTest extends RealDataBenchmarkSanityTest {

  private static final Map<String, Integer> EXPECTED_RESULTS =
      ImmutableMap.<String, Integer>builder()
          .put(CENSUS_INCOME, 1245448)
          .put(CENSUS1881, 23)
          .put(DIMENSION_008, 112317)
          .put(DIMENSION_003, 0)
          .put(DIMENSION_033, 0)
          .put(USCENSUS2000, 0)
          .put(WEATHER_SEPT_85, 642019)
          .put(WIKILEAKS_NOQUOTES, 3327)
          .put(CENSUS_INCOME_SRT, 927715)
          .put(CENSUS1881_SRT, 206)
          .put(WEATHER_SEPT_85_SRT, 1062989)
          .put(WIKILEAKS_NOQUOTES_SRT, 152)
          .build();

  @Override
  protected void doTest(String dataset, String type, boolean immutable) {
    int expected = EXPECTED_RESULTS.get(dataset);
    RealDataBenchmarkAnd bench = new RealDataBenchmarkAnd();
    assertEquals(expected, bench.pairwiseAnd(bs));
  }
}
