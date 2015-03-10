/*
 * (c) Seth Pellegrino
 * Licensed under the Apache License, Version 2.0.
 */


package org.roaringbitmap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

public class TestIntIteratorFlyweight {
    @Test
    public void testEmptyIteration() {
        IntIteratorFlyweight iter = new IntIteratorFlyweight();
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        iter.wrap(bitmap);
        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testSmallIteration() {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 2, 3);
        IntIteratorFlyweight iter = new IntIteratorFlyweight();
        iter.wrap(bitmap);
        final List<Integer> intIteratorCopy = asList(iter);
        Assert.assertEquals(ImmutableList.of(1, 2, 3), intIteratorCopy);

    }



    @Test
    public void testIteration() {
        final Random source = new Random(0xcb000a2b9b5bdfb6l);
        final int[] data = takeSortedAndDistinct(source, 450000);
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
        IntIteratorFlyweight iter = new IntIteratorFlyweight();
        iter.wrap(bitmap);

        final List<Integer> intIteratorCopy = asList(iter);

        Assert.assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
        Assert.assertEquals(Ints.asList(data), intIteratorCopy);

    }

    private static int[] takeSortedAndDistinct(Random source, int count) {
        LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);
        for (int size = 0; size < count; size++) {
            int next;
            do {
                next = Math.abs(source.nextInt());
            } while (!ints.add(next));
        }
        int[] unboxed = Ints.toArray(ints);
        Arrays.sort(unboxed);
        return unboxed;
    }

    private static List<Integer> asList(IntIterator ints) {
        int[] values = new int[10];
        int size = 0;
        while (ints.hasNext()) {
            if (!(size < values.length)) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = ints.next();
        }
        return Ints.asList(Arrays.copyOf(values, size));
    }

    private static List<Integer> asList(final ShortIterator shorts) {
        return asList(new IntIterator() {
            @Override
            public boolean hasNext() {
                return shorts.hasNext();
            }

            @Override
            public int next() {
                return shorts.next();
            }

            @SuppressWarnings("CloneDoesntCallSuperClone")
            @Override
            public IntIterator clone() {
                throw new UnsupportedOperationException();
            }
        });
    }
}
