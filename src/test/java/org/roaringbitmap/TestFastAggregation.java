package org.roaringbitmap;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestFastAggregation {

    @Test
    public void horizontal_or() {
        RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
        RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
        RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1<<16, 2<<16);
        RoaringBitmap result = FastAggregation.horizontal_or(Arrays.asList(rb1, rb2, rb3));
        RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 2, 5, 6, 1<<16, 2<<16);
        assertEquals(expected, result);
    }

    @Test
    public void horizontal_or2() {
        RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
        RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
        RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1<<16, 2<<16);
        RoaringBitmap result = FastAggregation.horizontal_or(rb1, rb2, rb3);
        RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 2, 5, 6, 1<<16, 2<<16);
        assertEquals(expected, result);
    }

    @Test
    public void priorityqueue_or() {
        RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
        RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
        RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1<<16, 2<<16);
        RoaringBitmap result = FastAggregation.priorityqueue_or(Arrays.asList(rb1, rb2, rb3).iterator());
        RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 2, 5, 6, 1<<16, 2<<16);
        assertEquals(expected, result);
    }

    @Test
    public void priorityqueue_or2() {
        RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
        RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
        RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1<<16, 2<<16);
        RoaringBitmap result = FastAggregation.priorityqueue_or(rb1, rb2, rb3);
        RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 2, 5, 6, 1<<16, 2<<16);
        assertEquals(expected, result);
    }

}
