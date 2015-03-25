/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import org.junit.Test;

public class TestBitmapContainer {
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testPreviousTooSmall() {
        emptyContainer().prevSetBit(-1);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testPreviousTooLarge() {
        emptyContainer().prevSetBit(Short.MAX_VALUE + 1);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testNextTooSmall() {
        emptyContainer().nextSetBit(-1);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testNextTooLarge() {
        emptyContainer().nextSetBit(Short.MAX_VALUE + 1);
    }

    private static BitmapContainer emptyContainer() {
        return new BitmapContainer(new long[1], 0);
    }

}
