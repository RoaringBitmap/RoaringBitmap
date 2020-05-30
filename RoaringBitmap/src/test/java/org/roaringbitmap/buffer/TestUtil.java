package org.roaringbitmap.buffer;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.CONCURRENT)
public class TestUtil {
  
    @Test
    public void testCopy() {
      CharBuffer sb = CharBuffer.allocate(64);
      sb.position(32);
      CharBuffer slice = sb.slice();
      CharBuffer dest = CharBuffer.allocate(64);
      for(int k = 0; k < 32; ++k)
        slice.put(k, (char) k);
      BufferUtil.arraycopy(slice, 16, dest, 16, 16);
      for(int k = 16; k < 32; ++k)
        assertEquals((char)k,dest.get(k));
      BufferUtil.arraycopy(slice, 0, dest, 16, 16);
      for(int k = 16; k < 32; ++k)
        assertEquals((char)(k-16),dest.get(k));
      BufferUtil.arraycopy(slice, 16, dest, 0, 16);
      for(int k = 0; k < 16; ++k)
        assertEquals((char)(k+16),dest.get(k));

    }

    @Test
    public void testFillArrayANDNOT() {
        LongBuffer data1 = LongBuffer.wrap(new long[]{1, 2, 4, 8, 16});
        LongBuffer data2 = LongBuffer.wrap(new long[]{2, 1, 3, 7, 15});
        char[] content = new char[5];
        char[] result = {0, 65, 130, 195, 260};
        BufferUtil.fillArrayANDNOT(content, data1, data2);
        assertTrue(Arrays.equals(content, result));
    }

    @Test
    public void testFillArrayANDNOTException() {
        assertThrows(IllegalArgumentException.class, () -> {
            LongBuffer data1 = LongBuffer.wrap(new long[]{1, 2, 4, 8, 16});
            LongBuffer data2 = LongBuffer.wrap(new long[]{2, 1, 3, 7});
            char[] content = new char[5];
            BufferUtil.fillArrayANDNOT(content, data1, data2);
        });
    }



    @Test
    public void testUnsignedIntersects() {
        CharBuffer data1 = CharBuffer.wrap(fromShorts(new short[]{-100, -98, -96, -94, -92, -90, -88, -86, -84, -82, -80}));
        CharBuffer data2 = CharBuffer.wrap(fromShorts(new short[]{-99, -97, -95, -93, -91, -89, -87, -85, -83, -81, -79}));
        CharBuffer data3 = CharBuffer.wrap(fromShorts(new short[]{-99, -97, -95, -93, -91, -89, -87, -85, -83, -81, -80}));
        CharBuffer data4 = CharBuffer.wrap(new char[]{});
        CharBuffer data5 = CharBuffer.wrap(new char[]{});
        assertFalse(BufferUtil.unsignedIntersects(data1, data1.limit(), data2, data2.limit()));
        assertTrue(BufferUtil.unsignedIntersects(data1, data1.limit(), data3, data3.limit()));
        assertFalse(BufferUtil.unsignedIntersects(data4, data4.limit(), data5, data5.limit()));
    }

  @Test
    public void testAdvanceUntil() {
        CharBuffer data = CharBuffer.wrap(fromShorts(new short[] {0, 3, 16, 18, 21, 29, 30, -342}));
        assertEquals(1, BufferUtil.advanceUntil(data, -1, data.limit(), (char) 3));
        assertEquals(5, BufferUtil.advanceUntil(data, -1, data.limit(), (char) 28));
        assertEquals(5, BufferUtil.advanceUntil(data, -1, data.limit(), (char) 29));
        assertEquals(7, BufferUtil.advanceUntil(data, -1, data.limit(), (char) -342));
    }

    @Test
    public void testIterateUntil() {
        CharBuffer data = CharBuffer.wrap(fromShorts(new short[] {0, 3, 16, 18, 21, 29, 30, -342}));
        assertEquals(1, BufferUtil.iterateUntil(data, 0, data.limit(), ((char) 3)));
        assertEquals(5, BufferUtil.iterateUntil(data, 0, data.limit(), ((char) 28)));
        assertEquals(5, BufferUtil.iterateUntil(data, 0, data.limit(), ((char) 29)));
        assertEquals(7, BufferUtil.iterateUntil(data, 0, data.limit(), ((char) -342)));
    }

  static char[] fromShorts(short[] array) {
    char[] result = new char[array.length];
    for (int i = 0 ; i < array.length; ++i) {
      result[i] = (char)(array[i] & 0xFFFF);
    }
    return result;
  }
}
