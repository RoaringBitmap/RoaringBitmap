package org.roaringbitmap.aggregation.newand.identical;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.RoaringBitmap;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class RoaringBitmapBenchmark {

    private RoaringBitmap bitmap1;
    private RoaringBitmap bitmap2;

    @Setup
    public void setup() {
        bitmap1 = new RoaringBitmap();
        bitmap2 = new RoaringBitmap();
        int k = 1 << 16;
        for(int i = 0; i < 10000; ++i) {
            bitmap1.add(i * k);
            bitmap2.add(i * k);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public RoaringBitmap and() {
        return RoaringBitmap.and(bitmap1, bitmap2);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public RoaringBitmap inplace_and() {
        return bitmap1.clone().and(bitmap2);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public RoaringBitmap newand() {
        return RoaringBitmap.newand(bitmap1, bitmap2);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public RoaringBitmap inplace_newand() {
        return bitmap1.clone().newand(bitmap2);
    }

}
