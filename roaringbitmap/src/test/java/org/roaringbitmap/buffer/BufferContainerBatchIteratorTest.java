package org.roaringbitmap.buffer;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.ContainerBatchIterator;
import org.roaringbitmap.RandomisedTestData;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static java.util.Arrays.copyOfRange;


@RunWith(Parameterized.class)
public class BufferContainerBatchIteratorTest {


    @Parameterized.Parameters
    public static Object[][] params() {
        return new Object[][] {
                {IntStream.range(0, 20000).toArray()},
                {IntStream.range(0, 1 << 16).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> i < 500 || i > 2000).filter(i -> i < (1 << 15) || i > ((1 << 15) | (1 << 8))).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 12) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 11) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 10) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 9) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 8) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 7) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 6) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 5) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 4) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 3) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 2) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> ((i >>> 1) & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> (i & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> (i & 1) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> (i % 3) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> (i % 5) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> (i % 7) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> (i % 9) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> (i % 271) == 0).toArray()},
                {IntStream.range(0, 1 << 16).filter(i -> (i % 1000) == 0).toArray()},
                {IntStream.empty().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.sparseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.denseRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
                {RandomisedTestData.rleRegion().toArray()},
        };
    }


    private final int[] expectedValues;

    public BufferContainerBatchIteratorTest(int[] expectedValues) {
        this.expectedValues = expectedValues;
    }

    @Test
    public void shouldRecoverValues512() {
        test(512);
    }

    @Test
    public void shouldRecoverValues1024() {
        test(1024);
    }

    @Test
    public void shouldRecoverValues2048() {
        test(2048);
    }

    @Test
    public void shouldRecoverValues4096() {
        test(4096);
    }

    @Test
    public void shouldRecoverValues8192() {
        test(8192);
    }

    @Test
    public void shouldRecoverValues65536() {
        test(65536);
    }


    @Test
    public void shouldRecoverValuesRandomBatchSizes() {
        IntStream.range(0, 100)
                .forEach(i -> test(ThreadLocalRandom.current().nextInt(1, 65536)));
    }


    private void test(int batchSize) {
        int[] buffer = new int[batchSize];
        MappeableContainer container = createContainer();
        ContainerBatchIterator it = container.getBatchIterator();
        int cardinality = 0;
        while (it.hasNext()) {
            int from = cardinality;
            cardinality += it.next(0, buffer);
            Assert.assertArrayEquals("Failure with batch size " + batchSize,
                    copyOfRange(expectedValues, from, cardinality), copyOfRange(buffer, 0, cardinality - from));
        }
        Assert.assertEquals(expectedValues.length, cardinality);
    }

    private MappeableContainer createContainer() {
        MappeableContainer container = new MappeableArrayContainer();
        for (int value : expectedValues) {
            container = container.add((short) value);
        }
        return container.runOptimize();
    }
}