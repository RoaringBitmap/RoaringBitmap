package org.roaringbitmap.realdata;

import com.google.common.collect.ImmutableMap;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.roaringbitmap.RealDataset.*;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.CONCISE;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.WAH;


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

    @Before
    public void setup() throws Exception {
        Assume.assumeFalse(type.equals(CONCISE) && immutable);
        Assume.assumeFalse(type.equals(WAH) && immutable);
        super.setup();
    }

    @Test
    public void test() throws Exception {
        int expected = EXPECTED_RESULTS.get(dataset);
        RealDataBenchmarkAndNot bench = new RealDataBenchmarkAndNot();
        assertEquals(expected, bench.pairwiseAndNot(bs));
    }

}
