package org.roaringbitmap;

import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestArrayContainer {

    @Test
    public void testConst() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        short[] data = {5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
        ArrayContainer ac2 = new ArrayContainer(data);
        Assert.assertEquals(ac1, ac2);
    }

    @Test
    public void testRemove() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        ac1.remove((short)14);
        ArrayContainer ac2 = new ArrayContainer(5, 14);
        Assert.assertEquals(ac1, ac2);
    }

    @Test
    public void testToString() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        Assert.assertEquals("{5,6,7,8,9,10,11,12,13,14}", ac1.toString());
    }

    @Test
    public void testIandNot() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        ArrayContainer ac2 = new ArrayContainer(10, 15);
        BitmapContainer bc = new BitmapContainer(5, 10);
        ArrayContainer ac3 = ac1.iandNot(bc);
        Assert.assertEquals(ac2, ac3);
    }

    @Test
    public void testReverseArrayContainerShortIterator() {
        //Test Clone
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        ReverseArrayContainerShortIterator rac1 = new ReverseArrayContainerShortIterator(ac1);
        ShortIterator rac2 = rac1.clone();
        Assert.assertEquals(asList(rac1), asList(rac2));
    }

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

}
