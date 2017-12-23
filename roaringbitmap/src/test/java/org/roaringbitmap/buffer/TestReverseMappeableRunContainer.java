package org.roaringbitmap.buffer;

import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.ShortIterator;

import java.util.Arrays;
import java.util.List;

public class TestReverseMappeableRunContainer {

    private static List<Integer> asList(ShortIterator ints) {
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

    @Test
    public void testClone() {
        MappeableRunContainer mappeableRunContainer = new MappeableRunContainer();
        for (short i = 10; i < 20; ++i) {
            mappeableRunContainer.add(i);
        }
        ReverseMappeableRunContainerShortIterator rmr = new ReverseMappeableRunContainerShortIterator(mappeableRunContainer);
        ShortIterator rmrClone = rmr.clone();
        final List<Integer> rmrList = asList(rmr);
        final List<Integer> rmrCloneList = asList(rmrClone);
        Assert.assertTrue(rmrList.equals(rmrCloneList));
    }

    @Test
    public void testNextAsInt() {
        MappeableRunContainer mappeableRunContainer = new MappeableRunContainer();
        for (short i = 10; i < 15; ++i) {
            mappeableRunContainer.add(i);
        }
        ReverseMappeableRunContainerShortIterator rmr = new ReverseMappeableRunContainerShortIterator(mappeableRunContainer);
        Assert.assertEquals(14, rmr.nextAsInt());
        rmr.next();
        rmr.next();
        rmr.next();
        rmr.next();
        rmr.next();
        rmr.nextAsInt();
        rmr.nextAsInt();
        rmr.nextAsInt();
        rmr.nextAsInt();
        rmr.nextAsInt();
        Assert.assertEquals(13, rmr.nextAsInt());
    }
}


