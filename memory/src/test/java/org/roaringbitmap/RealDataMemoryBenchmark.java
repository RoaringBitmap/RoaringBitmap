package org.roaringbitmap;

import it.uniroma3.mat.extendedset.intset.ConciseSet;
import net.sourceforge.sizeof.SizeOf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.roaringbitmap.RealDataset.*;

@RunWith(Parameterized.class)
public class RealDataMemoryBenchmark {

    @Parameters(name = "{0}")
    public static Collection<Object[]> params() {
        return Arrays.asList(
                new Object[][] {
                        { CENSUS_INCOME },
                        { CENSUS1881 },
                        { DIMENSION_008 },
                        { DIMENSION_003 },
                        { DIMENSION_033 },
                        { USCENSUS2000 },
                        { WEATHER_SEPT_85 },
                        { WIKILEAKS_NOQUOTES },
                        { CENSUS_INCOME_SRT },
                        { CENSUS1881_SRT },
                        { WEATHER_SEPT_85_SRT },
                        { WIKILEAKS_NOQUOTES_SRT }
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
