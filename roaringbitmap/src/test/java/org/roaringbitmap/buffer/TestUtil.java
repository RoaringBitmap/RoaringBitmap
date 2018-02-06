package org.roaringbitmap.buffer;


import org.junit.Assert;
import org.junit.Test;

import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;

public class TestUtil {
  
    @Test
    public void testCopy() {
      ShortBuffer sb = ShortBuffer.allocate(64);
      sb.position(32);
      ShortBuffer slice = sb.slice();
      ShortBuffer dest = ShortBuffer.allocate(64);
      for(int k = 0; k < 32; ++k)
        slice.put(k, (short) k);
      BufferUtil.arraycopy(slice, 16, dest, 16, 16);
      for(int k = 16; k < 32; ++k)
        Assert.assertEquals((short)k,dest.get(k));
      BufferUtil.arraycopy(slice, 0, dest, 16, 16);
      for(int k = 16; k < 32; ++k)
        Assert.assertEquals((short)(k-16),dest.get(k));
      BufferUtil.arraycopy(slice, 16, dest, 0, 16);
      for(int k = 0; k < 16; ++k)
        Assert.assertEquals((short)(k+16),dest.get(k));

    }

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

  @Test
    public void testAdvanceUntil() {
        ShortBuffer data = ShortBuffer.wrap(new short[] {0, 3, 16, 18, 21, 29, 30, -342});
        Assert.assertEquals(1, BufferUtil.advanceUntil(data, -1, data.limit(), (short) 3));
        Assert.assertEquals(5, BufferUtil.advanceUntil(data, -1, data.limit(), (short) 28));
        Assert.assertEquals(5, BufferUtil.advanceUntil(data, -1, data.limit(), (short) 29));
        Assert.assertEquals(7, BufferUtil.advanceUntil(data, -1, data.limit(), (short) -342));
    }

    @Test
    public void testIterateUntil() {
        ShortBuffer data = ShortBuffer.wrap(new short[] {0, 3, 16, 18, 21, 29, 30, -342});
        Assert.assertEquals(1, BufferUtil.iterateUntil(data, 0, data.limit(), BufferUtil.toIntUnsigned((short) 3)));
        Assert.assertEquals(5, BufferUtil.iterateUntil(data, 0, data.limit(), BufferUtil.toIntUnsigned((short) 28)));
        Assert.assertEquals(5, BufferUtil.iterateUntil(data, 0, data.limit(), BufferUtil.toIntUnsigned((short) 29)));
        Assert.assertEquals(7, BufferUtil.iterateUntil(data, 0, data.limit(), BufferUtil.toIntUnsigned((short) -342)));
    }

    @Test
    public void testToUnsigned() {
        Assert.assertEquals(0, BufferUtil.toIntUnsigned((short) 0));
        Assert.assertEquals(128, BufferUtil.toIntUnsigned((short) 128));
        Assert.assertEquals(32767, BufferUtil.toIntUnsigned(Short.MAX_VALUE));
        Assert.assertEquals(32768, BufferUtil.toIntUnsigned(Short.MIN_VALUE));
        Assert.assertEquals(65535, BufferUtil.toIntUnsigned((short) -1));
    }

    @Test
    public void testReverseToUnsigned() {
        Assert.assertEquals((short) 0, (short) 0);
        Assert.assertEquals((short) 128, (short) 128);
        Assert.assertEquals(Short.MAX_VALUE, (short) 32767);
        Assert.assertEquals(Short.MIN_VALUE, (short) 32768);
        Assert.assertEquals((short) -1, (short) 65535);
    }

}
