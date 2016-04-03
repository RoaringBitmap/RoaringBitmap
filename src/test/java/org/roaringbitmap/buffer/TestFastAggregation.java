package org.roaringbitmap.buffer;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

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

    @Test
    public void testPriorityQueueOr() {
        int[] array1 = {1232, 3324, 123,43243, 1322, 7897, 8767};
        int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
        int[] array3 = {1232, 3324, 123,43243, 1322, 7897, 8767, 39173,
                39174, 39175, 39176, 39177, 39178, 39179};
        int[] array4 = {};
        ArrayList<MutableRoaringBitmap> data5 = new ArrayList<>();
        ArrayList<MutableRoaringBitmap> data6 = new ArrayList<>();
        MutableRoaringBitmap data1 = MutableRoaringBitmap.bitmapOf(array1);
        MutableRoaringBitmap data2 = MutableRoaringBitmap.bitmapOf(array2);
        MutableRoaringBitmap data3 = MutableRoaringBitmap.bitmapOf(array3);
        MutableRoaringBitmap data4 = MutableRoaringBitmap.bitmapOf(array4);
        data5.add(data1);
        data5.add(data2);
        Assert.assertEquals(data3, BufferFastAggregation.priorityqueue_or(data1, data2));
        Assert.assertEquals(data1, BufferFastAggregation.priorityqueue_or(data1));
        Assert.assertEquals(data1, BufferFastAggregation.priorityqueue_or(data1, data4));
        Assert.assertEquals(data3, BufferFastAggregation.priorityqueue_or(data5.iterator()));
        Assert.assertEquals(new MutableRoaringBitmap(), BufferFastAggregation.priorityqueue_or(data6.iterator()));
        data6.add(data1);
        Assert.assertEquals(data1, BufferFastAggregation.priorityqueue_or(data6.iterator()));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testPriorityQueueXor() {
        int[] array1 = {1232, 3324, 123,43243, 1322, 7897, 8767};
        int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
        int[] array3 = {1232, 3324, 123,43243, 1322, 7897, 8767, 39173,
                39174, 39175, 39176, 39177, 39178, 39179};
        MutableRoaringBitmap data1 = MutableRoaringBitmap.bitmapOf(array1);
        MutableRoaringBitmap data2 = MutableRoaringBitmap.bitmapOf(array2);
        MutableRoaringBitmap data3 = MutableRoaringBitmap.bitmapOf(array3);
        Assert.assertEquals(data3, BufferFastAggregation.priorityqueue_xor(data1, data2));
        BufferFastAggregation.priorityqueue_xor(data1);
    }
}
