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

}
