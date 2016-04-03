package org.roaringbitmap.buffer;


import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;

public class TestUtil {

    @Test
    public void testFillArrayANDNOT() {
        LongBuffer data1 = LongBuffer.wrap(new long[]{1, 2, 4, 8, 16});
        LongBuffer data2 = LongBuffer.wrap(new long[]{2, 1, 3, 7, 15});
        short[] content = new short[5];
        short[] result = {0, 65, 130, 195, 260};
        BufferUtil.fillArrayANDNOT(content, data1, data2);
        Assert.assertTrue(Arrays.equals(content, result));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFillArrayANDNOTException() {
        LongBuffer data1 = LongBuffer.wrap(new long[]{1, 2, 4, 8, 16});
        LongBuffer data2 = LongBuffer.wrap(new long[]{2, 1, 3, 7});
        short[] content = new short[5];
        BufferUtil.fillArrayANDNOT(content, data1, data2);
    }

    @Test
    public void testHybridUnsignedBinarySearch() {
        ShortBuffer data = ShortBuffer.wrap(new short[]{-100, -99, -98, -97, -96, -95, -94, -93,
                -92, -91, -90, -89, -88, -87, -86, -85, -84, -83, -82, -81, -80, -79, -78, -77,
                -76, -75, -74, -73, -72, -71, -70, -69, -68, -67, -66, -65, -64, -63, -62, -61,
                -19, -17, -15, -13, -11, -9, -7, -5, -4});
        Assert.assertEquals(0, BufferUtil.hybridUnsignedBinarySearch(data, 0, data.limit(), data.get(0)));
        Assert.assertEquals(30, BufferUtil.hybridUnsignedBinarySearch(data, 0, data.limit(), data.get(30)));
        Assert.assertEquals(3, BufferUtil.hybridUnsignedBinarySearch(data, 0, data.limit(), data.get(3)));
        Assert.assertEquals(25, BufferUtil.hybridUnsignedBinarySearch(data, 0, data.limit(), data.get(25)));
        Assert.assertEquals(-data.limit() - 1, BufferUtil.hybridUnsignedBinarySearch(data, 0, data.limit(), (short) -3));
    }

    @Test
    public void testUnsignedIntersects() {
        ShortBuffer data1 = ShortBuffer.wrap(new short[]{-100, -98, -96, -94, -92, -90, -88, -86, -84, -82, -80});
        ShortBuffer data2 = ShortBuffer.wrap(new short[]{-99, -97, -95, -93, -91, -89, -87, -85, -83, -81, -79});
        ShortBuffer data3 = ShortBuffer.wrap(new short[]{-99, -97, -95, -93, -91, -89, -87, -85, -83, -81, -80});
        ShortBuffer data4 = ShortBuffer.wrap(new short[]{});
        ShortBuffer data5 = ShortBuffer.wrap(new short[]{});
        Assert.assertFalse(BufferUtil.unsignedIntersects(data1, data1.limit(), data2, data2.limit()));
        Assert.assertTrue(BufferUtil.unsignedIntersects(data1, data1.limit(), data3, data3.limit()));
        Assert.assertFalse(BufferUtil.unsignedIntersects(data4, data4.limit(), data5, data5.limit()));
    }

}
