package org.roaringbitmap.needwork;


import java.util.ArrayList;
import java.util.Random;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RandomAccess {

    @Benchmark
     public int branchyRoaring(BenchmarkState benchmarkState) {
        int answer = 0;
        org.roaringbitmap.Util.USE_BRANCHLESS_BINSEARCH = false;
        for(int k : benchmarkState.queries) {
            for(RoaringBitmap rb : benchmarkState.ac) 
               if(rb.contains(k))
                 answer++;
        }
        return answer;
    }

    @Benchmark
    public int branchlessRoaring(BenchmarkState benchmarkState) {
        int answer = 0;
        org.roaringbitmap.Util.USE_BRANCHLESS_BINSEARCH = true;
        for(int k : benchmarkState.queries) {
            // on purpose we switch bitmaps between each contains to sabotage branchless
            for(RoaringBitmap rb : benchmarkState.ac) 
               if(rb.contains(k))
                 answer++;
        }
        return answer;
    }
    
    @Benchmark
    public int branchyRoaringWithRun(BenchmarkState benchmarkState) {
        int answer = 0;
        org.roaringbitmap.Util.USE_BRANCHLESS_BINSEARCH = false;
        for(int k : benchmarkState.queries) {
            for(RoaringBitmap rb : benchmarkState.rc) 
               if(rb.contains(k))
                 answer++;
        }
        return answer;
    }

    @Benchmark
    public int branchlessRoaringWithRun(BenchmarkState benchmarkState) {
        int answer = 0;
        org.roaringbitmap.Util.USE_BRANCHLESS_BINSEARCH = true;
        for(int k : benchmarkState.queries) {
            // on purpose we switch bitmaps between each contains to sabotage branchless
            for(RoaringBitmap rb : benchmarkState.rc) 
               if(rb.contains(k))
                 answer++;
        }
        return answer;
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
        
        int[] queries = new int[1024];
        

        ArrayList<RoaringBitmap> ac = new ArrayList<RoaringBitmap>();

        ArrayList<RoaringBitmap> rc = new ArrayList<RoaringBitmap>();

        public BenchmarkState() {
        }
                
        @Setup
        public void setup() throws Exception {
            ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
            System.out.println();
            System.out.println("Loading files from " + dataRetriever.getName());
            int universe = 0;
            

            for (int[] data : dataRetriever.fetchBitPositions()) {
                RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
                ac.add(basic.clone());
                int lv =  basic.getReverseIntIterator().next();
                if(lv > universe) universe = lv;

                basic.runOptimize();
                rc.add(basic);
            }
            Random rand = new Random(123);
            for(int k = 0; k < queries.length; ++k)
                queries[k] = rand.nextInt(universe+1);
            System.out.println("loaded "+rc.size()+" bitmaps");
        }

    }
}
