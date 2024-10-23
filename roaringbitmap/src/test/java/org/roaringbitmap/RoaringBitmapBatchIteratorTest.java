package org.roaringbitmap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.roaringbitmap.RoaringBitmapWriter.writer;
import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

@Execution(ExecutionMode.CONCURRENT)
public class RoaringBitmapBatchIteratorTest {

    private static RoaringBitmap[] BITMAPS;
    private static final int[] SIZES = {
        128, 256, 1024, 8192, 5, 127, 1023
    };

    private static void initBitmaps() {
        BITMAPS = new RoaringBitmap[] {
            testCase().withArrayAt(0).withArrayAt(2).withArrayAt(4).withArrayAt((1 << 15) | (1 << 14)).build(),
            testCase().withRunAt(0).withRunAt(2).withRunAt(4).withRunAt((1 << 15) | (1 << 14)).build(),
            testCase().withBitmapAt(0).withRunAt(2).withBitmapAt(4).withBitmapAt((1 << 15) | (1 << 14)).build(),
            testCase().withArrayAt(0).withBitmapAt(2).withRunAt(4).withBitmapAt((1 << 15) | (1 << 14)).build(),
            testCase().withRunAt(0).withArrayAt(2).withBitmapAt(4).withRunAt((1 << 15) | (1 << 14)).build(),
            testCase().withBitmapAt(0).withRunAt(2).withArrayAt(4).withBitmapAt((1 << 15) | (1 << 14)).build(),
            testCase().withArrayAt(0).withBitmapAt(2).withRunAt(4).withArrayAt((1 << 15) | (1 << 14)).build(),
            testCase().withBitmapAt(0).withArrayAt(2).withBitmapAt(4).withRunAt((1 << 15) | (1 << 14)).build(),
            testCase().withRunAt((1 << 15) | (1 << 11)).withBitmapAt((1 << 15) | (1 << 12)).withArrayAt((1 << 15) | (1 << 13)).withBitmapAt((1 << 15) | (1 << 14)).build(),
            RoaringBitmap.bitmapOf(IntStream.range(1 << 10, 1 << 26).filter(i -> (i & 1) == 0).toArray()),
            RoaringBitmap.bitmapOf(IntStream.range(1 << 10, 1 << 25).filter(i -> ((i >>> 8) & 1) == 0).toArray()),
            RoaringBitmap.bitmapOf(IntStream.range(0,127).toArray()),
            RoaringBitmap.bitmapOf(IntStream.range(0,1024).toArray()),
            RoaringBitmap.bitmapOf(IntStream.concat(IntStream.range(0,256), IntStream.range(1 << 16, (1 << 16) | 256)).toArray()),
            RoaringBitmap.bitmapOf(8511),
            new RoaringBitmap()
        };
    }

    @BeforeAll
    public static void beforeAll() throws InterruptedException {
        int tryIndex = 0;
        int maxTryIndex = 3;
        while (++tryIndex < maxTryIndex) {
            try {
                initBitmaps();
            } catch (OutOfMemoryError e) {
                if (tryIndex == maxTryIndex) {
                    throw e;
                }
                e.printStackTrace();
                System.out.println("RoaringBitmapBatchIteratorTest.beforeAll Issue on try #" + tryIndex + ". Sleeping 5s for other tests to complete");
                TimeUnit.SECONDS.sleep(5);
            }
        }
    }

    @AfterAll
    public static void clear() {
        BITMAPS = null;
    }


    public static Stream<Arguments> params() {
        return Stream.of(BITMAPS)
            .flatMap(bitmap -> IntStream.of(SIZES).mapToObj(i -> Arguments.of(bitmap, i)));
    }

    @ParameterizedTest(name="offset={1}")
    @MethodSource("params")
    public void testBatchIteratorAsIntIterator(RoaringBitmap bitmap, int size) {
        IntIterator it = bitmap.getBatchIterator().asIntIterator(new int[size]);
        RoaringBitmapWriter<RoaringBitmap> w = writer().constantMemory()
                .initialCapacity(bitmap.highLowContainer.size).get();
        while (it.hasNext()) {
            w.add(it.next());
        }
        RoaringBitmap copy = w.get();
        assertEquals(bitmap, copy);
    }

    @ParameterizedTest(name="offset={1}")
    @MethodSource("params")
    public void test(RoaringBitmap bitmap, int batchSize) {
        int[] buffer = new int[batchSize];
        RoaringBitmap result = new RoaringBitmap();
        RoaringBatchIterator it = bitmap.getBatchIterator();
        int cardinality = 0;
        while (it.hasNext()) {
            int batch = it.nextBatch(buffer);
            for (int i = 0; i < batch; ++i) {
                result.add(buffer[i]);
            }
            cardinality += batch;
        }
        assertEquals(bitmap, result);
        assertEquals(bitmap.getCardinality(), cardinality);
    }

    @ParameterizedTest(name="offset={1}")
    @MethodSource("params")
    public void testBatchIteratorAdvancedIfNeeded(RoaringBitmap bitmap, int batchSize) {
        final int cardinality = bitmap.getCardinality();
        if (cardinality < 2) {
            return;
        }
        int midpoint = bitmap.select(cardinality / 2);
        int[] buffer = new int[batchSize];
        RoaringBitmap result = new RoaringBitmap();
        RoaringBatchIterator it = bitmap.getBatchIterator();
        it.advanceIfNeeded(midpoint);
        int consumed = 0;
        while (it.hasNext()) {
            int batch = it.nextBatch(buffer);
            for (int i = 0; i < batch; ++i) {
                result.add(buffer[i]);
            }
            consumed += batch;
        }
        RoaringBitmap expected = bitmap.clone();
        expected.remove(0, midpoint & 0xFFFFFFFFL);
        assertEquals(expected, result);
        assertEquals(expected.getCardinality(), consumed);
    }

    @ParameterizedTest(name="offset={1}")
    @MethodSource("params")
    public void testBatchIteratorAdvancedIfNeededToAbsentValue(RoaringBitmap bitmap, int batchSize) {
        long firstAbsent = bitmap.nextAbsentValue(0);
        int[] buffer = new int[batchSize];
        RoaringBitmap result = new RoaringBitmap();
        BatchIterator it = bitmap.getBatchIterator();
        it.advanceIfNeeded((int) firstAbsent);
        int consumed = 0;
        while (it.hasNext()) {
            int batch = it.nextBatch(buffer);
            for (int i = 0; i < batch; ++i) {
                result.add(buffer[i]);
            }
            consumed += batch;
        }
        RoaringBitmap expected = bitmap.clone();
        expected.remove(0, firstAbsent & 0xFFFFFFFFL);
        assertEquals(expected, result);
        assertEquals(expected.getCardinality(), consumed);
    }

    @ParameterizedTest(name="offset={1}")
    @MethodSource("params")
    public void testBatchIteratorAdvancedIfNeededBeyondLastValue(RoaringBitmap bitmap, int batchSize) {
        long advanceTo = bitmap.isEmpty() ? 0 : bitmap.last() + 1;
        int[] buffer = new int[batchSize];
        RoaringBitmap result = new RoaringBitmap();
        BatchIterator it = bitmap.getBatchIterator();
        it.advanceIfNeeded((int) advanceTo);
        int consumed = 0;
        while (it.hasNext()) {
            int batch = it.nextBatch(buffer);
            for (int i = 0; i < batch; ++i) {
                result.add(buffer[i]);
            }
            consumed += batch;
        }
        assertEquals(0, consumed);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testTimelyTermination() {
        RoaringBitmap bm = RoaringBitmap.bitmapOf(8511);
        BatchIterator bi = bm.getBatchIterator();
        int[] batch = new int[10];
        assertTrue(bi.hasNext());
        int n = bi.nextBatch(batch);
        assertEquals(n, 1);
        assertEquals(batch[0], 8511);
        assertFalse(bi.hasNext());
    }

    @Test
    public void testTimelyTerminationAfterAdvanceIfNeeded() {
        RoaringBitmap bm = RoaringBitmap.bitmapOf(8511);
        BatchIterator bi = bm.getBatchIterator();
        assertTrue(bi.hasNext());
        bi.advanceIfNeeded(8512);
        assertFalse(bi.hasNext());
    }

    @Test
    public void testBatchIteratorWithAdvanceIfNeeded() {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(3 << 16, (3 << 16) + 5, (3 << 16) + 10);
        BatchIterator it = bitmap.getBatchIterator();
        it.advanceIfNeeded(6);
        assertTrue(it.hasNext());
        int[] batch = new int[10];
        int n = it.nextBatch(batch);
        assertEquals(n, 3);
        assertEquals(batch[0], 3 << 16);
        assertEquals(batch[1], (3 << 16) + 5);
        assertEquals(batch[2], (3 << 16) + 10);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 11, 12, 13, 14, 15, 18, 20, 21, 23, 24})
    public void testBatchIteratorWithAdvancedIfNeededWithZeroLengthRun(int number) {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(10, 11, 12, 13, 14, 15, 18, 20, 21, 22, 23, 24);
        bitmap.runOptimize();
        BatchIterator it = bitmap.getBatchIterator();
        it.advanceIfNeeded(number);
        assertTrue(it.hasNext());
        int[] batch = new int[10];
        int n = it.nextBatch(batch);
        int i = Arrays.binarySearch(batch, 0, n, number);
        assertTrue(i >= 0, "key " + number + " not found");
        assertEquals(batch[i], number);
    }

    @Test
    public void testBatchIteratorFillsBufferAcrossContainers() {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(3 << 4, 3 << 8, 3 << 12, 3 << 16, 3 << 20, 3 << 24, 3 << 28);
        assertEquals(5, bitmap.highLowContainer.size());
        BatchIterator it = bitmap.getBatchIterator();
        int[] batch = new int[3];
        int n = it.nextBatch(batch);
        assertEquals(3, n);
        assertArrayEquals(new int[]{3 << 4, 3 << 8, 3 << 12}, batch);
        n = it.nextBatch(batch);
        assertEquals(3, n);
        assertArrayEquals(new int[]{3 << 16, 3 << 20, 3 << 24}, batch);
        n = it.nextBatch(batch);
        assertEquals(1, n);
        assertArrayEquals(new int[]{3 << 28}, Arrays.copyOfRange(batch, 0, 1));
        n = it.nextBatch(batch);
        assertEquals(0, n);
    }
}
