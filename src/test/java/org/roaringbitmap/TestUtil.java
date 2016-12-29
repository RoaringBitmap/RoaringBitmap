package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

public class TestUtil {

    @Test
    public void testUtilUnsignedIntersection() {
        short data1[] = {-19, -17, -15, -13, -11, -9, -7, -5, -3, -1};
        short data2[] = {-18, -16, -14, -12, -10, -8, -1};
        Assert.assertTrue(Util.unsignedIntersects(data1, data1.length, data2, data2.length));
        short data3[] = {-19, -17, -15, -13, -11, -9, -7};
        short data4[] = {-18, -16, -14, -12, -10, -8, -6, -4, -2, 0};
        Assert.assertFalse(Util.unsignedIntersects(data3, data3.length, data4, data4.length));
        short data5[] = {};
        short data6[] = {};
        Assert.assertFalse(Util.unsignedIntersects(data5, data5.length, data6, data6.length));
    }

    @Test
    public void testBranchyUnsignedBinarySearch() {
        short data1[] = {-19, -17, -15, -13, -11, -9, -7, -5, -3};
        Assert.assertEquals(8, Util.branchyUnsignedBinarySearch(data1, 0, data1.length, data1[8]));
        Assert.assertEquals(0, Util.branchyUnsignedBinarySearch(data1, 0, data1.length, data1[0]));
        Assert.assertEquals(data1.length-1, Util.branchyUnsignedBinarySearch(data1, data1.length-1, data1.length, data1[data1.length-1]));
        Assert.assertEquals(-1, Util.branchyUnsignedBinarySearch(data1, 0, 0, (short)0));
        Assert.assertEquals(-10, Util.branchyUnsignedBinarySearch(data1, 0, data1.length, (short) -1));
    }

    @Test
    public void testCardinalityInBitmapWordRange() {
        BitmapContainer bc = new BitmapContainer();
        bc.add((short) 1);
        bc.add((short) 2);
        bc.add((short) 31);

        int result = Util.cardinalityInBitmapWordRange(bc.bitmap, 7, 37);

        Assert.assertEquals(1, result);
    }
}
