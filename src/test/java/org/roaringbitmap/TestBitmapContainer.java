/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

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


    @Test
    public void numberOfRunsLowerBound1() {
        System.out.println("numberOfRunsLowerBound1");
        Random r = new Random(12345);
        
        for (double density = 0.001; density < 0.8; density *= 2)  {

            ArrayList<Integer> values = new ArrayList<Integer>();
            for (int i = 0; i < 65536; ++i)
                if (r.nextDouble() < density)  values.add(i);
            Integer [] positions = values.toArray( new Integer[0]);
            BitmapContainer bc = new BitmapContainer();

            for (int position : positions) {
                bc.add((short) position);
            }

            assertTrue(bc.numberOfRunsLowerBound(1) > 1);
            assertTrue(bc.numberOfRunsLowerBound(100) <= bc.numberOfRuns());

            assertTrue(bc.numberOfRunsLowerBound(100000) <= bc.numberOfRuns());
            assertEquals(bc.numberOfRuns(),
                         bc.numberOfRunsLowerBound(100000)+bc.numberOfRunsAdjustment());



            assertTrue(bc.numberOfRunsLowerBoundUnrolled2(1) > 1);
            assertTrue(bc.numberOfRunsLowerBoundUnrolled2(100) <= bc.numberOfRuns());

            assertTrue(bc.numberOfRunsLowerBound(100000) <= bc.numberOfRuns());
            assertEquals(bc.numberOfRunsLowerBound(100000),
                         bc.numberOfRunsLowerBoundUnrolled2(100000));
        }

    }



}
