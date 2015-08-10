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
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.runcontainer.RunContainerRealDataBenchmarkAndNot.BenchmarkState;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RunContainerRealDataBenchmarkIterate {

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
     public int iterate_RoaringWithRun(BenchmarkState benchmarkState) {
         int total = 0;
         for (int k = 0; k < benchmarkState.rc.size(); ++k) {
             RoaringBitmap rb = benchmarkState.rc.get(k);
             org.roaringbitmap.IntIterator i = rb.getIntIterator();
             while(i.hasNext())
                 total += i.next();
         }
         return total;
     }


     @Benchmark
     public int iterate_Roaring(BenchmarkState benchmarkState) {
         int total = 0;
         for (int k = 0; k < benchmarkState.ac.size(); ++k) {
             RoaringBitmap rb = benchmarkState.ac.get(k);
             org.roaringbitmap.IntIterator i = rb.getIntIterator();
             while(i.hasNext())
                 total += i.next();
         }
         return total;

     }


     @Benchmark
     public int iterate_Concise(BenchmarkState benchmarkState) {
         int total = 0;
         for (int k = 0; k < benchmarkState.cc.size(); ++k) {
             ConciseSet cs = benchmarkState.cc.get(k);
             it.uniroma3.mat.extendedset.intset.IntSet.IntIterator i = cs.iterator();
             while(i.hasNext())
                 total += i.next();
         }
         return total;
     }


     @Benchmark
     public int iterate_WAH(BenchmarkState benchmarkState) {
         int total = 0;
         for (int k = 0; k < benchmarkState.wah.size(); ++k) {
             ConciseSet cs = benchmarkState.wah.get(k);
             it.uniroma3.mat.extendedset.intset.IntSet.IntIterator i = cs.iterator();
             while(i.hasNext())
                 total += i.next();
         }
         return total;
     }

     @Benchmark
     public int iterate_EWAH(BenchmarkState benchmarkState) {
         int total = 0;
         for(int k = 0; k < benchmarkState.ewah.size(); ++k) {
             com.googlecode.javaewah.IntIterator i = benchmarkState.ewah.get(k).intIterator();
             while(i.hasNext())
                 total += i.next();             
         }
         return total;
     }

     @Benchmark
     public int iterate_EWAH32(BenchmarkState benchmarkState) {
         int total = 0;
         for(int k = 0; k < benchmarkState.ewah32.size(); ++k) {
             com.googlecode.javaewah.IntIterator i = benchmarkState.ewah32.get(k).intIterator();
             while(i.hasNext())
                 total += i.next();             
         }
         return total;
     }

     
     
    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param ({// putting the data sets in alpha. order
            "census-income", "census1881",
            "dimension_008", "dimension_003", 
            "dimension_033", "uscensus2000", 
            "weather_sept_85", "wikileaks-noquotes",
            "census-income_srt","census1881_srt",
            "weather_sept_85_srt","wikileaks-noquotes_srt"
        })
        String dataset;

        int totalandnot = 0;
        int totaland = 0;
        int totalor = 0;
        int totalxor = 0;
        int horizontalor = 0;

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
            // compute pairwise AND and OR
            for (int k = 0; k + 1 < rc.size(); ++k) {
                totalandnot += RoaringBitmap.andNot(rc.get(k), rc.get(k + 1))
                               .getCardinality();
                totaland += RoaringBitmap.and(rc.get(k), rc.get(k + 1))
                            .getCardinality();
                totalor += RoaringBitmap.or(rc.get(k), rc.get(k + 1))
                           .getCardinality();
                totalxor += RoaringBitmap.xor(rc.get(k), rc.get(k + 1))
                            .getCardinality();
            }
            horizontalor = FastAggregation.naive_or(rc.iterator())
                    .getCardinality();
        }

    }

}
