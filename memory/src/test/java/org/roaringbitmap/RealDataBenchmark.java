package org.roaringbitmap;

import net.sourceforge.sizeof.SizeOf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class RealDataBenchmark {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                        { "census-income" },
                        { "census1881" },
                        { "dimension_008" },
                        { "dimension_003" },
                        { "dimension_033" },
                        { "uscensus2000" },
                        { "weather_sept_85" },
                        { "wikileaks-noquotes"}
                });
    }

    @Parameter
    public String dataset;

    @Test
    public void benchmark() throws Exception {
        ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);

        long basicSize = 0;
        long optiSize = 0;

        for (int[] data : dataRetriever.fetchBitPositions()) {
            RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
            RoaringBitmap opti = basic.clone();
            opti.runOptimize();

            basicSize += SizeOf.deepSizeOf(basic);
            optiSize += SizeOf.deepSizeOf(opti);
        }

        System.out.println();
        System.out.println("==============");
        System.out.println(dataset + " / Run size = " + SizeOf.humanReadable(optiSize) + " / normal size = " + SizeOf.humanReadable(basicSize));
        System.out.println("==============");
    }

}
