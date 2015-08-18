package org.roaringbitmap.runcontainer;


import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
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

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@SuppressWarnings("rawtypes")
public class RunContainerRealDataBenchmarkWideOrNaive {

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
    
    // only include the first count items
    // Note: if you application is routinely aggregating
    // hundreds or thousands of bitmaps, you are maybe missing
    // optimization opportunities (e.g., one can precompute
    // some aggregates) so we mostly care for "moderate"
    // queries.
    protected static Iterator limit(final int count, final Iterator x) {
        
        return new Iterator(){
            int pos = 0;

            @Override
            public boolean hasNext() {
                return (pos < count) && (x.hasNext());
            }

            @Override
            public Object next() {
                pos++;
                return x.next();
            }
            
        };
    }
    

    // Concise does not provide a pq approach, we should provide it
    public static ConciseSet pq_or(final Iterator<ConciseSet> bitmaps) {
        PriorityQueue<ConciseSet> pq = new PriorityQueue<ConciseSet>(128,
                new Comparator<ConciseSet>() {
                    @Override
                    public int compare(ConciseSet a, ConciseSet b) {
                        return (int) (a.size() * a
                                .collectionCompressionRatio())  - (int) (b.size() * b
                                        .collectionCompressionRatio()) ;
                    }
                }
        );
        while(bitmaps.hasNext())
            pq.add(bitmaps.next());
        if(pq.isEmpty()) return new ConciseSet();
        while (pq.size() > 1) {
            ConciseSet x1 = pq.poll();
            ConciseSet x2 = pq.poll();
            pq.add(x1.union(x2));
        }
        return pq.poll();
    }

    
    @Benchmark
    public int Roaring(BenchmarkState benchmarkState) {
        int answer = RoaringBitmap.or(limit(benchmarkState.count,benchmarkState.ac.iterator()))
               .getCardinality();
        if(answer != benchmarkState.horizontalor)
            throw new RuntimeException("bug");
        return answer;
    }

    @Benchmark
    public int RoaringWithRun(BenchmarkState benchmarkState) {
        int answer = RoaringBitmap.or(limit(benchmarkState.count,benchmarkState.rc.iterator()))
               .getCardinality();
        if(answer != benchmarkState.horizontalor)
            throw new RuntimeException("bug");
        return answer;
    }

    @Benchmark
    public int Concise_naive(BenchmarkState benchmarkState) {
        ConciseSet bitmapor = benchmarkState.cc.get(0);
        for (int j = 1; j < Math.min(benchmarkState.count, benchmarkState.cc.size()) ; ++j) {
            bitmapor = bitmapor.union(benchmarkState.cc.get(j));
        }
        int answer = bitmapor.size();
        if(answer != benchmarkState.horizontalor)
            throw new RuntimeException("buggy horizontal or");
        return answer;
    }

    @Benchmark
    public int WAH_naive(BenchmarkState benchmarkState) {
        ConciseSet bitmapor = benchmarkState.wah.get(0);
        for (int j = 1; j < Math.min(benchmarkState.wah.size(),benchmarkState.count) ; ++j) {
            bitmapor = bitmapor.union(benchmarkState.cc.get(j));
        }
        int answer = bitmapor.size();
        if(answer != benchmarkState.horizontalor)
            throw new RuntimeException("buggy horizontal or");
        return answer;
    }
    

    @Benchmark
    public int EWAH_naive(BenchmarkState benchmarkState) {
        Iterator i = limit(benchmarkState.count,benchmarkState.ewah.iterator());
        EWAHCompressedBitmap bitmapor = (EWAHCompressedBitmap) i.next();
        while(i.hasNext())
            bitmapor = bitmapor.or((EWAHCompressedBitmap) i.next());
        int answer = bitmapor.cardinality();
        if(answer != benchmarkState.horizontalor)
            throw new RuntimeException("bug");
        return answer;
    }
    
    @Benchmark
    public int EWAH32_naive(BenchmarkState benchmarkState) {
        Iterator i = limit(benchmarkState.count,benchmarkState.ewah32.iterator());
        EWAHCompressedBitmap32 bitmapor = (EWAHCompressedBitmap32) i.next();
        while(i.hasNext())
            bitmapor = bitmapor.or((EWAHCompressedBitmap32) i.next());
        int answer = bitmapor.cardinality();
        if(answer != benchmarkState.horizontalor)
            throw new RuntimeException("bug");
        return answer;

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

        int horizontalor = 0;
        
        protected int count = 8;// arbitrary number but warning: when increasing this number 
        // check that reported timings increase monotonically, I found that as of ~12, they sharply decreased
        // for some schemes, suggesting that the benchmark was defeated.


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
            count = rc.size();
            System.out.println("# aggregating the first "+count+" bitmaps out of "+ac.size());

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
            horizontalor = FastAggregation.naive_or(limit(count,rc.iterator()))
                    .getCardinality();
        }

    }

}
