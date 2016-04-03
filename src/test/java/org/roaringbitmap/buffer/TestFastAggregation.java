package org.roaringbitmap.buffer;

import org.junit.Assert;
import org.junit.Test;

public class TestFastAggregation {

    @Test
    public void testNaiveAnd() {
        int[] array1 = {39173, 39174};
        int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
        int[] array3 = {39173, 39174};
        int[] array4 = {};
        MutableRoaringBitmap data1 = MutableRoaringBitmap.bitmapOf(array1);
        MutableRoaringBitmap data2 = MutableRoaringBitmap.bitmapOf(array2);
        MutableRoaringBitmap data3 = MutableRoaringBitmap.bitmapOf(array3);
        MutableRoaringBitmap data4 = MutableRoaringBitmap.bitmapOf(array4);
        Assert.assertEquals(data3, BufferFastAggregation.naive_and(data1, data2));
        Assert.assertEquals(new MutableRoaringBitmap(), BufferFastAggregation.naive_and(data4));
    }
}
