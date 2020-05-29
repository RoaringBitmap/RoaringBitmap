package org.roaringbitmap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;


@Execution(ExecutionMode.CONCURRENT)
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
    public void or() {
        RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
        RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
        RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1<<16, 2<<16);
        RoaringBitmap result = FastAggregation.or(rb1, rb2, rb3);
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

    private static class ExtendedRoaringBitmap extends RoaringBitmap {}


    @Test
    public void testWorkShyAnd() {
        final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2, 0x10001, 0x20001, 0x30001);
        final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3, 0x20002, 0x30001);
        final RoaringBitmap bResult = FastAggregation.workShyAnd(b1, b2);
        assertFalse(bResult.contains(1));
        assertTrue(bResult.contains(2));
        assertFalse(bResult.contains(3));
    }

    @Test
    public void testAndWithIterator() {
        final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
        final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
        final RoaringBitmap bResult = FastAggregation.and(Arrays.asList(b1, b2).iterator());
        assertFalse(bResult.contains(1));
        assertTrue(bResult.contains(2));
        assertFalse(bResult.contains(3));

        final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
        eb1.add(1);
        eb1.add(2);
        final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
        eb2.add(2);
        eb2.add(3);
        final RoaringBitmap ebResult = FastAggregation.and(Arrays.asList(b1, b2).iterator());
        assertFalse(ebResult.contains(1));
        assertTrue(ebResult.contains(2));
        assertFalse(ebResult.contains(3));
    }

    @Test
    public void testNaiveAndWithIterator() {
        final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
        final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
        final RoaringBitmap bResult = FastAggregation.naive_and(Arrays.asList(b1, b2).iterator());
        assertFalse(bResult.contains(1));
        assertTrue(bResult.contains(2));
        assertFalse(bResult.contains(3));

        final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
        eb1.add(1);
        eb1.add(2);
        final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
        eb2.add(2);
        eb2.add(3);
        final RoaringBitmap ebResult = FastAggregation.naive_and(Arrays.asList(b1, b2).iterator());
        assertFalse(ebResult.contains(1));
        assertTrue(ebResult.contains(2));
        assertFalse(ebResult.contains(3));
    }

    @Test
    public void testOrWithIterator() {
        final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
        final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
        final RoaringBitmap bItResult = FastAggregation.or(Arrays.asList(b1, b2).iterator());
        assertTrue(bItResult.contains(1));
        assertTrue(bItResult.contains(2));
        assertTrue(bItResult.contains(3));

        final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
        eb1.add(1);
        eb1.add(2);
        final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
        eb2.add(2);
        eb2.add(3);
        final RoaringBitmap ebItResult = FastAggregation.or(Arrays.asList(b1, b2).iterator());
        assertTrue(ebItResult.contains(1));
        assertTrue(ebItResult.contains(2));
        assertTrue(ebItResult.contains(3));
    }

    @Test
    public void testNaiveOrWithIterator() {
        final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
        final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
        final RoaringBitmap bResult = FastAggregation.naive_or(Arrays.asList(b1, b2).iterator());
        assertTrue(bResult.contains(1));
        assertTrue(bResult.contains(2));
        assertTrue(bResult.contains(3));

        final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
        eb1.add(1);
        eb1.add(2);
        final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
        eb2.add(2);
        eb2.add(3);
        final RoaringBitmap ebResult = FastAggregation.naive_or(Arrays.asList(b1, b2).iterator());
        assertTrue(ebResult.contains(1));
        assertTrue(ebResult.contains(2));
        assertTrue(ebResult.contains(3));
    }

    @Test
    public void testNaiveXorWithIterator() {
        final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
        final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
        final RoaringBitmap bResult = FastAggregation.naive_xor(Arrays.asList(b1, b2).iterator());
        assertTrue(bResult.contains(1));
        assertFalse(bResult.contains(2));
        assertTrue(bResult.contains(3));

        final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
        eb1.add(1);
        eb1.add(2);
        final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
        eb2.add(2);
        eb2.add(3);
        final RoaringBitmap ebResult = FastAggregation.naive_xor(Arrays.asList(b1, b2).iterator());
        assertTrue(ebResult.contains(1));
        assertFalse(ebResult.contains(2));
        assertTrue(ebResult.contains(3));
    }

}
