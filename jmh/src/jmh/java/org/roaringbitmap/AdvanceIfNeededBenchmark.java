package org.roaringbitmap;

import com.google.common.primitives.Ints;
import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 2000)
@Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS, time = 2000)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class AdvanceIfNeededBenchmark {
    public static MutableRoaringBitmap c2;
    public static PeekableIntIterator it;

    private static int[] takeSortedAndDistinct(Random source, int count) {
        LinkedHashSet<Integer> ints = new LinkedHashSet<>(count);
        for (int size = 0; size < count; size++) {
            int next;
            do {
                next = source.nextInt(1000000);
            } while (!ints.add(next));
        }
        // we add a range of continuous values
        for(int k = 1000; k < 10000; ++k) {
            ints.add(k);
        }
        int[] unboxed = Ints.toArray(ints);
        Arrays.sort(unboxed);
        return unboxed;
    }
    static {
        final Random source = new Random(0xcb000a2b9b5bdfb6L);
        final int[] data = takeSortedAndDistinct(source, 450000);
        c2 = MutableRoaringBitmap.bitmapOf(data);

    }
    @Setup(Level.Iteration)
    public void setup() {
        it = c2.getIntIterator();
        c2.first();
    }

    @Benchmark
    public MutableRoaringBitmap advanceIfNeeded() {
        it.advanceIfNeeded((char) (59 + 15 * 64));
        return c2;
    }
}
