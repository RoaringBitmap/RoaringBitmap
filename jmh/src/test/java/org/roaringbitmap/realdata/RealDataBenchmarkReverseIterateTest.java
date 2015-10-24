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


public class RealDataBenchmarkReverseIterateTest extends RealDataBenchmarkSanityTest {

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

    @Before
    public void setup() throws Exception {
        Assume.assumeFalse(type.equals(CONCISE) && immutable);
        Assume.assumeFalse(type.equals(WAH) && immutable);
        super.setup();
    }

    @Test
    public void test() throws Exception {
        int expected = EXPECTED_RESULTS.get(dataset);
        RealDataBenchmarkReverseIterate bench = new RealDataBenchmarkReverseIterate();
        assertEquals(expected, bench.reverseIterate(bs));
    }

}
