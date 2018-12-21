package org.roaringbitmap;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.roaringbitmap.RoaringBitmapWriter.writer;
import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

@RunWith(Parameterized.class)
public class RoaringBitmapBatchIteratorTest {

    @Parameterized.Parameters
    public static Object[][] params() {
        return new Object[][] {
                {testCase().withArrayAt(0).withArrayAt(2).withArrayAt(4).withArrayAt((1 << 15) | (1 << 14)).build()},
                {testCase().withRunAt(0).withRunAt(2).withRunAt(4).withRunAt((1 << 15) | (1 << 14)).build()},
                {testCase().withBitmapAt(0).withRunAt(2).withBitmapAt(4).withBitmapAt((1 << 15) | (1 << 14)).build()},
                {testCase().withArrayAt(0).withBitmapAt(2).withRunAt(4).withBitmapAt((1 << 15) | (1 << 14)).build()},
                {testCase().withRunAt(0).withArrayAt(2).withBitmapAt(4).withRunAt((1 << 15) | (1 << 14)).build()},
                {testCase().withBitmapAt(0).withRunAt(2).withArrayAt(4).withBitmapAt((1 << 15) | (1 << 14)).build()},
                {testCase().withArrayAt(0).withBitmapAt(2).withRunAt(4).withArrayAt((1 << 15) | (1 << 14)).build()},
                {testCase().withBitmapAt(0).withArrayAt(2).withBitmapAt(4).withRunAt((1 << 15) | (1 << 14)).build()},
                {testCase().withRunAt((1 << 15) | (1 << 11)).withBitmapAt((1 << 15) | (1 << 12)).withArrayAt((1 << 15) | (1 << 13)).withBitmapAt((1 << 15) | (1 << 14)).build()},
                {RoaringBitmap.bitmapOf(IntStream.range(1 << 10, 1 << 26).filter(i -> (i & 1) == 0).toArray())},
                {RoaringBitmap.bitmapOf(IntStream.range(1 << 10, 1 << 25).filter(i -> ((i >>> 8) & 1) == 0).toArray())},
                {RoaringBitmap.bitmapOf(IntStream.range(0,127).toArray())},
                {RoaringBitmap.bitmapOf(IntStream.range(0,1024).toArray())},
                {RoaringBitmap.bitmapOf(IntStream.concat(IntStream.range(0,256), IntStream.range(1 << 16, (1 << 16) | 256)).toArray())},
                {new RoaringBitmap()}
        };
    }

    private final RoaringBitmap bitmap;

    public RoaringBitmapBatchIteratorTest(RoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    @Test
    public void testBatchIterator256() {
        test(256);
    }


    @Test
    public void testBatchIterator1024() {
        test(1024);
    }


    @Test
    public void testBatchIterator65536() {
        test(65536);
    }


    @Test
    public void testBatchIterator8192() {
        test(8192);
    }

    @Test
    public void testBatchIteratorRandom() {
        IntStream.range(0, 10).map(i -> ThreadLocalRandom.current().nextInt(0, 1 << 16))
                .forEach(this::test);
    }

    @Test
    public void testBatchIteratorAsIntIterator() {
        IntIterator it = bitmap.getBatchIterator().asIntIterator(new int[128]);
        RoaringBitmapWriter<RoaringBitmap> w = writer().constantMemory()
                .initialCapacity(bitmap.highLowContainer.size).get();
        while (it.hasNext()) {
            w.add(it.next());
        }
        RoaringBitmap copy = w.get();
        assertEquals(bitmap, copy);
    }

    private void test(int batchSize) {
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

}
