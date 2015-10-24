package org.roaringbitmap.realdata;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.roaringbitmap.RealDataset.*;


public class RealDataBenchmarkWideOrNaiveTest extends RealDataBenchmarkSanityTest {

    private static final Map<String, Integer> EXPECTED_RESULTS =
            ImmutableMap.<String, Integer>builder()
                    .put(CENSUS_INCOME, 199523)
                    .put(CENSUS1881, 988653)
                    .put(DIMENSION_008, 148278)
                    .put(DIMENSION_003, 3866847)
                    .put(DIMENSION_033, 3866847)
                    .put(USCENSUS2000, 5985)
                    .put(WEATHER_SEPT_85, 1015367)
                    .put(WIKILEAKS_NOQUOTES, 242540)
                    .put(CENSUS_INCOME_SRT, 199523)
                    .put(CENSUS1881_SRT, 656346)
                    .put(WEATHER_SEPT_85_SRT, 1015367)
                    .put(WIKILEAKS_NOQUOTES_SRT, 236436)
            .build();

    @Test
    public void test() throws Exception {
        int expected = EXPECTED_RESULTS.get(dataset);
        RealDataBenchmarkWideOrNaive bench = new RealDataBenchmarkWideOrNaive();
        assertEquals(expected, bench.wideOr_naive(bs));
    }

}
