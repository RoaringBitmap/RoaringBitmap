package org.roaringbitmap.buffer;

import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.PeekableShortIterator;

import java.util.Arrays;
import java.util.List;

import static org.roaringbitmap.buffer.MappeableArrayContainer.DEFAULT_MAX_SIZE;

public class TestMappeableBitmapContainerShortIterator {

    private static List<Integer> asList(PeekableShortIterator ints) {
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
        MappeableBitmapContainer mappeableBitmapContainer = new MappeableBitmapContainer();
        for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
            mappeableBitmapContainer.add((short) (k * 10));
        }
        MappeableBitmapContainerShortIterator tmbc = new MappeableBitmapContainerShortIterator(mappeableBitmapContainer);
        PeekableShortIterator tmbcClone = tmbc.clone();
        final List<Integer> tmbcList = asList(tmbc);
        final List<Integer> tmbcCloneList = asList(tmbcClone);
        Assert.assertEquals(tmbcList, tmbcCloneList);
    }
}
