/*
 * (c) Seth Pellegrino
 * Licensed under the Apache License, Version 2.0.
 */


package org.roaringbitmap;


import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class TestIterators {
    @Test
    public void testIteration() {
        final Random source = new Random(0xcb000a2b9b5bdfb6l);
        final int[] data = takeSortedAndDistinct(source, 450000);
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
        IntIterator i = bitmap.getIntIterator();
        int counter = 0;
        while(i.hasNext()) {
            i.next();
            ++counter;
        }
        Assert.assertEquals(bitmap.getCardinality(), counter);
        Assert.assertArrayEquals(data, copyOf(bitmap.iterator()));
        Assert.assertArrayEquals(data, copyOf(bitmap.getIntIterator()));
    }

    private int[] takeSortedAndDistinct(Random source, int count) {
        LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);
        for (int size = 0; size < count; size++) {
            int next;
            do {
                next = Math.abs(source.nextInt());
            } while (!ints.add(next));
        }
        int[] unboxed = copyOf(ints.iterator());
        Arrays.sort(unboxed);
        return unboxed;
    }

    private static int[] copyOf(Iterator<Integer> ints) {
        class IntIteratorAdapter implements IntIterator {
            private final Iterator<Integer> ints;

            IntIteratorAdapter(Iterator<Integer> ints) {
                this.ints = ints;
            }

            @Override
            public boolean hasNext() {
                return ints.hasNext();
            }

            @Override
            public int next() {
                return ints.next();
            }

            @Override
            public IntIterator clone() {
                throw new UnsupportedOperationException();
            }
        }
        return copyOf(new IntIteratorAdapter(ints));
    }

    private static int[] copyOf(IntIterator ints) {
        int[] values = new int[10];
        int size = 0;
        while (ints.hasNext()) {
            if (!(size < values.length)) {
                values = java.util.Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = ints.next();
        }
        return Arrays.copyOf(values, size);
    }

}
