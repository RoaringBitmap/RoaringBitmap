package org.roaringbitmap;

import it.uniroma3.mat.extendedset.intset.ConciseSet;
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
                        { "wikileaks-noquotes"},                
                        {"census-income_srt"},
                        {"census1881_srt"},
                        {"weather_sept_85_srt"},
                        {"wikileaks-noquotes_srt"}

                });
    }

    @Parameter
    public String dataset;

    @Test
    public void benchmark() throws Exception {
        ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);

        long basicSize = 0;
        long optiSize = 0;
        long conciseSize = 0;
        long wahSize = 0;

        for (int[] data : dataRetriever.fetchBitPositions()) {
            RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
            RoaringBitmap opti = basic.clone();
            opti.runOptimize();
            ConciseSet concise = toConcise(data);
            ConciseSet w = toWAH(data);

            basicSize += SizeOf.deepSizeOf(basic);
            optiSize += SizeOf.deepSizeOf(opti);
            conciseSize += SizeOf.deepSizeOf(concise);
            wahSize += SizeOf.deepSizeOf(w);
        }

        System.out.println();
        System.out.println("==============");
        System.out.println(dataset + " / Run size = " + SizeOf.humanReadable(optiSize)
                                   + " / normal size = " + SizeOf.humanReadable(basicSize)
                                   + " / consize size = " + SizeOf.humanReadable(conciseSize)
                                   + " / WAH size = " + SizeOf.humanReadable(wahSize));
        System.out.println("==============");
    }

    private static ConciseSet toConcise(int[] dat) {
        ConciseSet ans = new ConciseSet();
        for (int i : dat) {
            ans.add(i);
        }
        return ans;
    }


    private static ConciseSet toWAH(int[] dat) {
        ConciseSet ans = new ConciseSet(true);
        for (int i : dat) {
            ans.add(i);
        }
        return ans;
    }


}
