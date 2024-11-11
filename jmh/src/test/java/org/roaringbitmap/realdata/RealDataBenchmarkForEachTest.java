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
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.EWAH;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.EWAH32;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.WAH;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class RealDataBenchmarkForEachTest extends RealDataBenchmarkSanityTest {

  private static final Map<String, Integer> EXPECTED_RESULTS =
      ImmutableMap.<String, Integer>builder()
          .put(CENSUS_INCOME, -942184551)
          .put(CENSUS1881, 246451066)
          .put(DIMENSION_008, -423436314)
          .put(DIMENSION_003, -1287135055)
          .put(DIMENSION_033, -1287135055)
          .put(USCENSUS2000, -1260727955)
          .put(WEATHER_SEPT_85, 644036874)
          .put(WIKILEAKS_NOQUOTES, 413846869)
          .put(CENSUS_INCOME_SRT, -679313956)
          .put(CENSUS1881_SRT, 445584405)
          .put(WEATHER_SEPT_85_SRT, 1132748056)
          .put(WIKILEAKS_NOQUOTES_SRT, 1921022163)
          .build();

  @Override
  protected void doTest(String dataset, String type, boolean immutable) {
    assumeFalse(type.equals(CONCISE));
    assumeFalse(type.equals(WAH));
    assumeFalse(type.equals(EWAH));
    assumeFalse(type.equals(EWAH32));
    int expected = EXPECTED_RESULTS.get(dataset);
    RealDataBenchmarkForEach bench = new RealDataBenchmarkForEach();
    assertEquals(expected, bench.forEach(bs));
  }
}
