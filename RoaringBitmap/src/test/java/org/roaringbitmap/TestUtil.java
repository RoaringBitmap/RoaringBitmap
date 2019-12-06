package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestUtil {

    @Test
    public void testUtilUnsignedIntersection() {
        char data1[] = fromShorts(new short[]{-19, -17, -15, -13, -11, -9, -7, -5, -3, -1});
        char data2[] = fromShorts(new short[]{-18, -16, -14, -12, -10, -8, -1});
        Assert.assertTrue(Util.unsignedIntersects(data1, data1.length, data2, data2.length));
        char data3[] = fromShorts(new short[]{-19, -17, -15, -13, -11, -9, -7});
        char data4[] = fromShorts(new short[]{-18, -16, -14, -12, -10, -8, -6, -4, -2, 0});
        Assert.assertFalse(Util.unsignedIntersects(data3, data3.length, data4, data4.length));
        char data5[] = {};
        char data6[] = {};
        Assert.assertFalse(Util.unsignedIntersects(data5, data5.length, data6, data6.length));
    }

    @Test
    public void testBranchyUnsignedBinarySearch() {
        char data1[] = fromShorts(new short[]{-19, -17, -15, -13, -11, -9, -7, -5, -3});
        Assert.assertEquals(8, Util.branchyUnsignedBinarySearch(data1, 0, data1.length, data1[8]));
        Assert.assertEquals(0, Util.branchyUnsignedBinarySearch(data1, 0, data1.length, data1[0]));
        Assert.assertEquals(data1.length-1, Util.branchyUnsignedBinarySearch(data1, data1.length-1, data1.length, data1[data1.length-1]));
        Assert.assertEquals(-1, Util.branchyUnsignedBinarySearch(data1, 0, 0, (char)0));
        Assert.assertEquals(-10, Util.branchyUnsignedBinarySearch(data1, 0, data1.length, (char) -1));
    }

    @Test
    public void testCompare() {
        Assert.assertTrue(Util.compareUnsigned((char)1,(char)2) < 0);
        Assert.assertTrue(Util.compareUnsigned((char)-32333,(char)2) > 0);
        Assert.assertTrue(Util.compareUnsigned((char)2, (char)-32333) < 0);
        Assert.assertTrue(Util.compareUnsigned((char)0,(char)0) ==0);

    }

    @Test
    public void testPartialRadixSortEmpty() {
        int[] data = new int[] {};
        int[] test = Arrays.copyOf(data, data.length);
        Util.partialRadixSort(test);
        Assert.assertArrayEquals(data, test);
    }

    @Test
    public void testPartialRadixSortIsStableInSameKey() {
        int[] data = new int[] {25, 1, 0, 10};
        int[] test = Arrays.copyOf(data, data.length);
        Util.partialRadixSort(test);
        Assert.assertArrayEquals(data, test);
    }

    @Test
    public void testPartialRadixSortSortsKeysCorrectly() {
        int key1 = 1 << 16;
        int key2 = 1 << 17;
        int[] data = new int[] {key2 | 25, key1 | 1, 0, key2 | 10, 25, key1 | 10, key1, 10};
        // sort by keys, leave values stable
        int[] expected = new int[] {0, 25, 10, key1 | 1, key1 | 10, key1, key2 | 25, key2 | 10};
        int[] test = Arrays.copyOf(data, data.length);
        Util.partialRadixSort(test);
        Assert.assertArrayEquals(expected, test);
    }

    @Test
    public void testPartialRadixSortSortsKeysCorrectlyWithDuplicates() {
        int key1 = 1 << 16;
        int key2 = 1 << 17;
        int[] data = new int[] {key2 | 25, key1 | 1, 0, key2 | 10, 25, key1 | 10, key1, 10,
                                key2 | 25, key1 | 1, 0, key2 | 10, 25, key1 | 10, key1, 10};
        // sort by keys, leave values stable
        int[] expected = new int[] {0, 25, 10, 0, 25, 10, key1 | 1, key1 | 10,  key1, key1 | 1, key1 | 10,  key1,
                                    key2 | 25, key2 | 10, key2 | 25, key2 | 10};
        int[] test = Arrays.copyOf(data, data.length);
        Util.partialRadixSort(test);
        Assert.assertArrayEquals(expected, test);
    }

    @Test
    public void testAdvanceUntil() {
        char data[] = fromShorts(new short[]{0, 3, 16, 18, 21, 29, 30,-342});
        Assert.assertEquals(1, Util.advanceUntil(data, -1, data.length, (char) 3));
        Assert.assertEquals(5, Util.advanceUntil(data, -1, data.length, (char) 28));
        Assert.assertEquals(5, Util.advanceUntil(data, -1, data.length, (char) 29));
        Assert.assertEquals(7, Util.advanceUntil(data, -1, data.length, (char) -342));
    }

    @Test
    public void testIterateUntil() {
        char data[] = fromShorts(new short[]{0, 3, 16, 18, 21, 29, 30,-342});
        Assert.assertEquals(1, Util.iterateUntil(data, 0, data.length, Util.toIntUnsigned((char) 3)));
        Assert.assertEquals(5, Util.iterateUntil(data, 0, data.length, Util.toIntUnsigned((char) 28)));
        Assert.assertEquals(5, Util.iterateUntil(data, 0, data.length, Util.toIntUnsigned((char) 29)));
        Assert.assertEquals(7, Util.iterateUntil(data, 0, data.length, Util.toIntUnsigned((char) -342)));
    }

    @Test
    public void testToUnsigned() {
        Assert.assertEquals(0, Util.toIntUnsigned((char) 0));
        Assert.assertEquals(128, Util.toIntUnsigned((char) 128));
        Assert.assertEquals(32767, Util.toIntUnsigned((char)Short.MAX_VALUE));
        Assert.assertEquals(32768, Util.toIntUnsigned((char)Short.MIN_VALUE));
        Assert.assertEquals(65535, Util.toIntUnsigned((char) -1));
    }

    @Test
    public void testReverseToUnsigned() {
        Assert.assertEquals((char) 0,  (char) 0);
        Assert.assertEquals((char) 128,  (char) 128);
        Assert.assertEquals((char)Short.MAX_VALUE,  (char) 32767);
        Assert.assertEquals((char)Short.MIN_VALUE,  (char) 32768);
        Assert.assertEquals((char) -1,  (char) 65535);
    }

    static char[] fromShorts(short[] array) {
        char[] result = new char[array.length];
        for (int i = 0 ; i < array.length; ++i) {
            result[i] = (char)(array[i] & 0xFFFF);
        }
        return result;
    }

}
