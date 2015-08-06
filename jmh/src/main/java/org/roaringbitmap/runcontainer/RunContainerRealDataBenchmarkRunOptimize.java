package org.roaringbitmap.runcontainer;


import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RunContainerRealDataBenchmarkRunOptimize {

    @Benchmark
    public int clone(BenchmarkState benchmarkState) {
        int total = 0;
        for (int i = 0; i < benchmarkState.ac.size(); i++) {
            RoaringBitmap bitmap = benchmarkState.ac.get(i).clone();
            total += bitmap.getCardinality();
        }
        return total;
    }

    @Benchmark
    public int runOptimize(BenchmarkState benchmarkState) {
        int total = 0;
        for (int i = 0; i < benchmarkState.ac.size(); i++) {
            RoaringBitmap bitmap = benchmarkState.ac.get(i).clone();
            bitmap.runOptimize();
            total += bitmap.getCardinality();
        }
        return total;
    }


    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param ({// putting the data sets in alpha. order
            "census-income", "census1881",
            "dimension_008", "dimension_003", 
            "dimension_033", "uscensus2000", 
            "weather_sept_85", "wikileaks-noquotes"
        })
        String dataset;

        ArrayList<RoaringBitmap> ac = new ArrayList<RoaringBitmap>();

        public BenchmarkState() {
        }
                
        @Setup
        public void setup() throws Exception {
            ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
            System.out.println();
            System.out.println("Loading files from " + dataRetriever.getName());

            for (int[] data : dataRetriever.fetchBitPositions()) {
                RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
                ac.add(basic);
            }
        }

    }

}
