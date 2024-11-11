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

public class RealDataBenchmarkOrTest extends RealDataBenchmarkSanityTest {

  private static final Map<String, Integer> EXPECTED_RESULTS =
      ImmutableMap.<String, Integer>builder()
          .put(CENSUS_INCOME, 12487395)
          .put(CENSUS1881, 2007691)
          .put(DIMENSION_008, 5555233)
          .put(DIMENSION_003, 7733676)
          .put(DIMENSION_033, 7579526)
          .put(USCENSUS2000, 11954)
          .put(WEATHER_SEPT_85, 24729002)
          .put(WIKILEAKS_NOQUOTES, 541893)
          .put(CENSUS_INCOME_SRT, 11257282)
          .put(CENSUS1881_SRT, 1360167)
          .put(WEATHER_SEPT_85_SRT, 30863347)
          .put(WIKILEAKS_NOQUOTES_SRT, 574463)
          .build();

  private static final Map<String, Integer> EXPECTED_RESULTS_NO_CARDINALITY =
      ImmutableMap.<String, Integer>builder()
          .put(CENSUS_INCOME, 20377)
          .put(CENSUS1881, 192344014)
          .put(DIMENSION_008, 60471647)
          .put(DIMENSION_003, -2080279977)
          .put(DIMENSION_033, 48993139)
          .put(USCENSUS2000, 1085687199)
          .put(WEATHER_SEPT_85, 164092)
          .put(WIKILEAKS_NOQUOTES, 43309741)
          .put(CENSUS_INCOME_SRT, 325103)
          .put(CENSUS1881_SRT, 113309680)
          .put(WEATHER_SEPT_85_SRT, 11922488)
          .put(WIKILEAKS_NOQUOTES_SRT, 28702307)
          .build();

  @Override
  protected void doTest(String dataset, String type, boolean immutable) {
    RealDataBenchmarkOr bench = new RealDataBenchmarkOr();
    assertEquals((int) EXPECTED_RESULTS.get(dataset), bench.pairwiseOr(bs));
    assertEquals(
        (int) EXPECTED_RESULTS_NO_CARDINALITY.get(dataset), bench.pairwiseOr_NoCardinality(bs));
  }
}
