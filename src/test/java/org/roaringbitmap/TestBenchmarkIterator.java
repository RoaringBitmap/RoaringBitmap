package org.roaringbitmap;

import static org.roaringbitmap.buffer.BenchmarkConsumers.CONSOLE_CONSUMER;
import static org.roaringbitmap.buffer.BenchmarkConsumers.H2_CONSUMER;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;

public class TestBenchmarkIterator {
    static RoaringBitmap bitmap;
    static IntIteratorFlyweight intIterator;
    static int result = 0;

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule(CONSOLE_CONSUMER,
            H2_CONSUMER);

    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 3)
    @Test
    public void testStandard() {
        IntIterator intIterator = bitmap.getIntIterator();
        while (intIterator.hasNext()) {
            result = intIterator.next();
        }
    }

    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 3)
    @Test
    public void testFlyweight() {
        intIterator.wrap(bitmap);
        while (intIterator.hasNext()) {
            result = intIterator.next();

        }
    }

    @BeforeClass
    public static void prepare() throws IOException {
        final Random source = new Random(0xcb000a2b9b5bdfb6l);
        final int[] data = takeSortedAndDistinct(source, 1000000);
        bitmap = RoaringBitmap.bitmapOf(data);
        intIterator = new IntIteratorFlyweight();
        
    }

    private static int[] takeSortedAndDistinct(Random source, int count) {
        HashSet<Integer> ints = new HashSet<Integer>(count);
        for (int size = 0; size < count; size++) {
            int next;
            do {
                next = Math.abs(source.nextInt());
            } while (!ints.add(next));
        }
        int[] unboxed = new int[ints.size()];
        int pos = 0;
        for (Integer i : ints)
            unboxed[pos++] = i;
        Arrays.sort(unboxed);
        return unboxed;
    }
}
