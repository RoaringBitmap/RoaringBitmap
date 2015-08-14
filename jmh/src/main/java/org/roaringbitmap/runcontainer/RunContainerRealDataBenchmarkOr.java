package org.roaringbitmap.runcontainer;


import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RunContainerRealDataBenchmarkOr {

    static ConciseSet toConcise(int[] dat) {
        ConciseSet ans = new ConciseSet();
        for (int i : dat) {
            ans.add(i);
        }
        return ans;
    }


    static ConciseSet toWAH(int[] dat) {
        ConciseSet ans = new ConciseSet(true);
        for (int i : dat) {
            ans.add(i);
        }
        return ans;
    }


    @Benchmark
    public int pairwiseOr_RoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
            total += RoaringBitmap.or(benchmarkState.rc.get(k),benchmarkState.rc.get(k+1)).getCardinality();
        if(total != benchmarkState.totalor )
            throw new RuntimeException("bad pairwise or result");
        return total;
    }

    @Benchmark
    public int pairwiseOr_Roaring(BenchmarkState benchmarkState) {
        int total = 0;
        for(int k = 0; k + 1 < benchmarkState.ac.size(); ++k)
            total += RoaringBitmap.or(benchmarkState.ac.get(k),benchmarkState.ac.get(k+1)).getCardinality();
        if(total != benchmarkState.totalor )
            throw new RuntimeException("bad pairwise or result");
        return total;
    }




     @Benchmark
     public int pairwiseOr_Concise(BenchmarkState benchmarkState) {
         int total = 0;
         for(int k = 0; k + 1 < benchmarkState.cc.size(); ++k)
             total += benchmarkState.cc.get(k).union(benchmarkState.cc.get(k+1)).size();
         if(total != benchmarkState.totalor )
             throw new RuntimeException("bad pairwise or result");
         return total;
     }


     @Benchmark
     public int pairwiseOr_WAH(BenchmarkState benchmarkState) {
         int total = 0;
         for(int k = 0; k + 1 < benchmarkState.wah.size(); ++k)
             total += benchmarkState.wah.get(k).union(benchmarkState.wah.get(k+1)).size();
         if(total != benchmarkState.totalor )
             throw new RuntimeException("bad pairwise or result");
         return total;
     }

     @Benchmark
     public int pairwiseOr_EWAH(BenchmarkState benchmarkState) {
         int total = 0;
         for(int k = 0; k + 1 < benchmarkState.ewah.size(); ++k)
             total += benchmarkState.ewah.get(k).or(benchmarkState.ewah.get(k+1)).cardinality();
         if(total !=benchmarkState.totalor )
             throw new RuntimeException("bad pairwise and result");
         return total;
     }

     @Benchmark
     public int pairwiseOr_EWAH32(BenchmarkState benchmarkState) {
         int total = 0;
         for(int k = 0; k + 1 < benchmarkState.ewah32.size(); ++k)
             total += benchmarkState.ewah32.get(k).or(benchmarkState.ewah32.get(k+1)).cardinality();
         if(total !=benchmarkState.totalor )
             throw new RuntimeException("bad pairwise and result");
         return total;
     }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param ({// putting the data sets in alpha. order
            "census-income", "census1881",
            "dimension_008", "dimension_003", 
            "dimension_033", "uscensus2000", 
            "weather_sept_85", "wikileaks-noquotes"
                ,"census-income_srt","census1881_srt",
                "weather_sept_85_srt","wikileaks-noquotes_srt"
        })
        String dataset;

        int totalor = 0;

        ArrayList<RoaringBitmap> rc = new ArrayList<RoaringBitmap>();
        ArrayList<RoaringBitmap> ac = new ArrayList<RoaringBitmap>();
        ArrayList<ConciseSet> cc = new ArrayList<ConciseSet>();
        ArrayList<ConciseSet> wah = new ArrayList<ConciseSet>();
        ArrayList<EWAHCompressedBitmap> ewah = new ArrayList<EWAHCompressedBitmap>();
        ArrayList<EWAHCompressedBitmap32> ewah32 = new ArrayList<EWAHCompressedBitmap32>();



        public BenchmarkState() {
        }
                
        @Setup
        public void setup() throws Exception {
            ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
            System.out.println();
            System.out.println("Loading files from " + dataRetriever.getName());

            int normalsize = 0;
            int runsize = 0;
            int concisesize = 0;
            int wahsize = 0;
            int ewahsize = 0;
            int ewahsize32 = 0;
            long stupidarraysize = 0;
            long stupidbitmapsize = 0;
            int totalcount = 0;
            int numberofbitmaps = 0;
            int universesize = 0;

            for (int[] data : dataRetriever.fetchBitPositions()) {
                numberofbitmaps++;
                if(universesize < data[data.length - 1 ])
                    universesize = data[data.length - 1 ];
                stupidarraysize += 8 + data.length * 4L;
                stupidbitmapsize += 8 + (data[data.length - 1] + 63L) / 64 * 8;
                totalcount += data.length;
                EWAHCompressedBitmap ewahBitmap = EWAHCompressedBitmap.bitmapOf(data);
                ewahsize += ewahBitmap.serializedSizeInBytes();
                ewah.add(ewahBitmap);
                EWAHCompressedBitmap32 ewahBitmap32 = EWAHCompressedBitmap32.bitmapOf(data);
                ewahsize32 += ewahBitmap32.serializedSizeInBytes();
                ewah32.add(ewahBitmap32);

                RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
                RoaringBitmap opti = basic.clone();
                opti.runOptimize();
                ConciseSet concise = toConcise(data);
                ConciseSet w = toWAH(data);
                wah.add(w);
                wahsize += (int) (concise.size() * concise
                        .collectionCompressionRatio()) * 4;
                rc.add(opti);
                ac.add(basic);
                cc.add(concise);
                normalsize += basic.serializedSizeInBytes();
                runsize += opti.serializedSizeInBytes();
                concisesize += (int) (concise.size() * concise
                                      .collectionCompressionRatio()) * 4;
            }

            /***
             * This is a hack. JMH does not allow us to report
             * anything directly ourselves, so we do it forcefully.
             */
            DecimalFormat df = new DecimalFormat("0.0");
            System.out.println();
            System.out.println("==============");
            System.out.println("= data set "+dataset);
            System.out.println("Number of bitmaps = " + numberofbitmaps
                               + " total count = " + totalcount
                               + " universe size = "+universesize);
            System.out.println("Average bits per bitmap = "
                               + df.format(totalcount * 1.0 / numberofbitmaps));
            System.out.println("Run-roaring total     = "
                    + String.format("%1$10s", "" + runsize)
                    + "B, average per bitmap = "
                    + String.format("%1$10s",df.format(runsize * 1.0 / numberofbitmaps))
                    + "B, average bits per entry =  "
                    + String.format("%1$10s",df.format(runsize * 8.0 / totalcount)));
            System.out.println("Regular roaring total = "
                    + String.format("%1$10s", "" + normalsize)
                    + "B, average per bitmap = "
                    + String.format("%1$10s",df.format(normalsize * 1.0 / numberofbitmaps))
                    + "B, average bits per entry =  "
                    + String.format("%1$10s",df.format(normalsize * 8.0 / totalcount)));
            System.out.println("Concise total         = "
                    + String.format("%1$10s", "" + concisesize)
                    + "B, average per bitmap = "
                    + String.format("%1$10s",df.format(concisesize * 1.0 / numberofbitmaps))
                    + "B, average bits per entry =  "
                    + String.format("%1$10s",df.format(concisesize * 8.0 / totalcount)));
            System.out.println("WAH total         = "
                    + String.format("%1$10s", "" + wahsize)
                    + "B, average per bitmap = "
                    + String.format("%1$10s",df.format(wahsize * 1.0 / numberofbitmaps))
                    + "B, average bits per entry =  "
                    + String.format("%1$10s",df.format(wahsize * 8.0 / totalcount)));
            System.out.println("EWAH 64-bit total = "
                    + String.format("%1$10s", "" + ewahsize)
                    + "B, average per bitmap = "
                    + String.format("%1$10s",df.format(ewahsize * 1.0 / numberofbitmaps))
                    + "B, average bits per entry =  "
                    + String.format("%1$10s",df.format(ewahsize * 8.0 / totalcount)));
            System.out.println("EWAH 32-bit total = "
                    + String.format("%1$10s", "" + ewahsize32)
                    + "B, average per bitmap = "
                    + String.format("%1$10s",df.format(ewahsize32 * 1.0 / numberofbitmaps))
                    + "B, average bits per entry =  "
                    + String.format("%1$10s",df.format(ewahsize32 * 8.0 / totalcount)));

            System.out.println("Naive array total     = "
                    + String.format("%1$10s", "" + stupidarraysize)
                    + "B, average per bitmap = "
                    + String.format("%1$10s",df.format(stupidarraysize * 1.0 / numberofbitmaps))
                    + "B, average bits per entry =  "
                    + String.format("%1$10s",df.format(stupidarraysize * 8.0 / totalcount)));
            System.out.println("Naive bitmap total    = "
                    + String.format("%1$10s", "" + stupidbitmapsize)
                    + "B, average per bitmap = "
                    + String.format("%1$10s",df.format(stupidbitmapsize * 1.0 / numberofbitmaps))
                    + "B, average bits per entry =  "
                    + String.format("%1$10s",df.format(stupidbitmapsize * 8.0 / totalcount)));
            System.out.println("==============");
            System.out.println();
            // compute pairwise 
            for (int k = 0; k + 1 < rc.size(); ++k) {
                RoaringBitmap v1 = RoaringBitmap.or(rc.get(k), rc.get(k + 1));
                RoaringBitmap v2 = RoaringBitmap.or(ac.get(k), ac.get(k + 1));
                if(!v1.equals(v2)) throw new RuntimeException("bug");
                totalor += v1.getCardinality();
            }
        }

    }

}
