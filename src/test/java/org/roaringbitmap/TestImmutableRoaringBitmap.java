package org.roaringbitmap;

import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import static org.junit.Assert.assertEquals;

public class TestImmutableRoaringBitmap {

    @Test
    public void xor() {
        ImmutableRoaringBitmap a = ImmutableRoaringBitmap.bitmapOf(1, 73647, 83469);
        ImmutableRoaringBitmap b = ImmutableRoaringBitmap.bitmapOf(1, 5, 10<<16);
        ImmutableRoaringBitmap xor = ImmutableRoaringBitmap.xor(a, b);
        ImmutableRoaringBitmap expected = ImmutableRoaringBitmap.bitmapOf(5, 73647, 83469, 10<<16);
        assertEquals(expected, xor);
    }

    @Test
    public void or() {
        ImmutableRoaringBitmap a = ImmutableRoaringBitmap.bitmapOf(1, 73647, 83469);
        ImmutableRoaringBitmap b = ImmutableRoaringBitmap.bitmapOf(1, 5, 10<<16);
        ImmutableRoaringBitmap expected = ImmutableRoaringBitmap.bitmapOf(1, 5, 73647, 83469, 10<<16);
        assertEquals(expected, ImmutableRoaringBitmap.or(a, b));
        assertEquals(expected, ImmutableRoaringBitmap.or(b, a));
    }

    @Test
    public void andNot() {
        ImmutableRoaringBitmap a = ImmutableRoaringBitmap.bitmapOf(1<<16, 2<<16);
        ImmutableRoaringBitmap b = ImmutableRoaringBitmap.bitmapOf(11, 12, 13, 2<<16);
        ImmutableRoaringBitmap andNot = ImmutableRoaringBitmap.andNot(a, b);
        ImmutableRoaringBitmap expected = ImmutableRoaringBitmap.bitmapOf(1<<16);
        assertEquals(expected, andNot);
    }

    @Test(expected = RuntimeException.class)
    public void flipInvalidRange() {
        ImmutableRoaringBitmap a = ImmutableRoaringBitmap.bitmapOf(1, 5, 7, 13);
        ImmutableRoaringBitmap.flip(a, 7L, 5L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flipInvalidRange2() {
        ImmutableRoaringBitmap a = ImmutableRoaringBitmap.bitmapOf(1, 5, 7, 13);
        ImmutableRoaringBitmap.flip(a, 1L<<32, 1L<<33);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flipInvalidRange3() {
        ImmutableRoaringBitmap a = ImmutableRoaringBitmap.bitmapOf(1, 5, 7, 13);
        ImmutableRoaringBitmap.flip(a, 1L, 1L<<33);
    }



}
