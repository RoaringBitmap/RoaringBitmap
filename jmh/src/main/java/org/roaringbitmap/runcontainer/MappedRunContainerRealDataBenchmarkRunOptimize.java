package org.roaringbitmap.runcontainer;


import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MappedRunContainerRealDataBenchmarkRunOptimize {

    @Benchmark
    public int mutable_clone(BenchmarkState benchmarkState) {
        int total = 0;
        for (int i = 0; i < benchmarkState.mac.size(); i++) {
            MutableRoaringBitmap bitmap = benchmarkState.mac.get(i).clone();
            total += bitmap.getCardinality();
        }
        return total;
    }

    @Benchmark
    public int mutable_runOptimize(BenchmarkState benchmarkState) {
        int total = 0;
        for (int i = 0; i < benchmarkState.mac.size(); i++) {
            MutableRoaringBitmap bitmap = benchmarkState.mac.get(i).clone();
            bitmap.runOptimize();
            total += bitmap.getCardinality();
        }
        return total;
    }


    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param ({// putting the data sets in alpha. order
                "census-income_srt","census1881_srt",
                    "weather_sept_85_srt","wikileaks-noquotes_srt",

                "census-income", "census1881",
                "dimension_008", "dimension_003",
                "dimension_033", "uscensus2000",
                "weather_sept_85", "wikileaks-noquotes"
        })
        String dataset;

        List<MutableRoaringBitmap> mac = new ArrayList<MutableRoaringBitmap>();

        public BenchmarkState() {
        }

        @Setup
        public void setup() throws Exception {
            ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
            System.out.println();
            System.out.println("Loading files from " + dataRetriever.getName());

            for (int[] data : dataRetriever.fetchBitPositions()) {
                MutableRoaringBitmap mbasic = MutableRoaringBitmap.bitmapOf(data);
                mac.add(mbasic);
            }
        }

    }

}
