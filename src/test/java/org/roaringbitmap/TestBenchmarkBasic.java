package org.roaringbitmap;

import static org.roaringbitmap.buffer.BenchmarkConsumers.CONSOLE_CONSUMER;
import static org.roaringbitmap.buffer.BenchmarkConsumers.H2_CONSUMER;


import java.io.IOException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;

public class TestBenchmarkBasic {
    

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule(CONSOLE_CONSUMER, H2_CONSUMER);

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void bigunion() throws Exception {
        for (int k = 1; k < N; k += 10) {
            RoaringBitmap bitmapor = FastAggregation.horizontal_or(Arrays.copyOf(ewah, k + 1));
            bogus += bitmapor.getCardinality();
        }
    }



    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void toarray() throws Exception {
        for (int k = 1; k < N * 100; ++k) {
            bogus += ewah[k % N].toArray().length;
        }
    }


    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void cardinality() throws Exception {
        for (int k = 1; k < N * 100; ++k) {
            bogus += ewah[k % N].getCardinality();
        }
    }


    @BeforeClass
    public static void prepare() throws IOException {
        for (int k = 0; k < N; ++k) {
            ewah[k] = new RoaringBitmap();
            for (int x = 0; x < M; ++x) {
                ewah[k].add(x * (N - k + 2));
            }
            ewah[k].trim();
        }
    }

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void createBitmapOrdered() {
        long besttime = Long.MAX_VALUE;
        RoaringBitmap r = new RoaringBitmap();
        long bef = System.nanoTime();
        for (int k = 0; k < 65536; k++) {
            r.add(k * 32);
        }
        long aft = System.nanoTime();
        if(besttime > aft - bef) besttime = aft-bef;
    }
    
    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void createBitmapUnordered() {
        long besttime = Long.MAX_VALUE;
        RoaringBitmap r = new RoaringBitmap();
        long bef = System.nanoTime();
        for (int k = 65536 - 1; k >= 0; k--) {
            r.add(k * 32);
        }
        long aft = System.nanoTime();
        if (besttime > aft - bef)
            besttime = aft - bef;
    }


    static int N = 1000;
    static int M = 1000;

    static RoaringBitmap[] ewah = new RoaringBitmap[N];

    public static int bogus = 0;

}
