package org.roaringbitmap.buffer;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class TestBufferRangeCardinality {

    private int[] elements;
    private int begin;
    private int end;
    private int expected;

    @Parameterized.Parameters(name = "{index}: cardinalityInBitmapRange({0},{1},{2})={3}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {new int[]{1, 3, 5, 7, 9}, 3, 8, 3},
                {new int[]{1, 3, 5, 7, 9}, 2, 8, 3},
                {new int[]{1, 3, 5, 7, 9}, 3, 7, 2},
                {new int[]{1, 3, 5, 7, 9}, 0, 7, 3},
                {new int[]{1, 3, 5, 7, 9}, 0, 6, 3},
                {new int[]{1, 3, 5, 7, 9, Short.MAX_VALUE}, 0, Short.MAX_VALUE + 1, 6},
                {new int[]{1, 10000, 25000, Short.MAX_VALUE - 1}, 0, Short.MAX_VALUE, 4},
                {new int[]{1 << 3, 1 << 8, 511,512,513, 1 << 12, 1 << 14}, 0, Short.MAX_VALUE, 7}
        });
    }

    public TestBufferRangeCardinality(int[] elements, int begin, int end, int expected) {
        this.elements = elements;
        this.begin = begin;
        this.end = end;
        this.expected = expected;
    }

    @Test
    public void testCardinalityInBitmapWordRange() {
        LongBuffer array = ByteBuffer.allocateDirect(MappeableBitmapContainer.MAX_CAPACITY / 8).asLongBuffer();
        MappeableBitmapContainer bc = new MappeableBitmapContainer(array, 0);
        for (int e : elements) {
            bc.add((short) e);
        }
        Assert.assertEquals(false, bc.isArrayBacked());
        Assert.assertEquals(expected, BufferUtil.cardinalityInBitmapRange(bc.bitmap, begin, end));
    }
}
