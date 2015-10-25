package org.roaringbitmap.needwork;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.Util;
import org.roaringbitmap.needwork.state.NeedWorkBenchmarkState;
import org.roaringbitmap.realdata.wrapper.Bitmap;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RandomAccess {

    @Benchmark
     public void binarySearch(BenchmarkState bs, Blackhole bh) {
        for(int k : bs.queries) {
            for (Bitmap bitmap : bs.bitmaps) {
                bh.consume(bitmap.contains(k));
            }
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState extends NeedWorkBenchmarkState {

        @Param({
                "false",
                "true"
        })
        public boolean hybrid;


        int[] queries = new int[1024];

        public BenchmarkState() {
        }

        @Setup
        public void setup() throws Exception {
            super.setup();

            int universe = 0;
            for (Bitmap bitmap : bitmaps) {
                int lv =  bitmap.last();
                if(lv > universe) universe = lv;
            }
            Random rand = new Random(123);
            for(int k = 0; k < queries.length; ++k)
                queries[k] = rand.nextInt(universe+1);

            Util.USE_HYBRID_BINSEARCH = hybrid;// this will not affect the buffer package
        }

    }

}
