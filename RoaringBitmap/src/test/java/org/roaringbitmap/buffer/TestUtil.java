package org.roaringbitmap.buffer;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.Util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.roaringbitmap.SeededTestData.*;
import static org.roaringbitmap.SeededTestData.sparseRegion;

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

  public static Stream<Arguments> sets() {
      return Stream.of(true, false)
              .flatMap(direct -> Stream.of(
              Arguments.of(direct, rleRegion().toArray(), rleRegion().toArray()),
              Arguments.of(direct, denseRegion().toArray(), rleRegion().toArray()),
              Arguments.of(direct, sparseRegion().toArray(), rleRegion().toArray()),
              Arguments.of(direct, rleRegion().toArray(), denseRegion().toArray()),
              Arguments.of(direct, denseRegion().toArray(), denseRegion().toArray()),
              Arguments.of(direct, sparseRegion().toArray(), denseRegion().toArray()),
              Arguments.of(direct, rleRegion().toArray(), sparseRegion().toArray()),
              Arguments.of(direct, denseRegion().toArray(), sparseRegion().toArray()),
              Arguments.of(direct, sparseRegion().toArray(), sparseRegion().toArray())));
  }


  @MethodSource("sets")
  @ParameterizedTest(name = "direct={0}")
  public void testIntersectBitmapWithArray(boolean direct, int[] set1, int[] set2) {
    LongBuffer bitmap = direct ? ByteBuffer.allocateDirect(8192).asLongBuffer() : LongBuffer.allocate(1024);
    for (int i : set1) {
      bitmap.put(i >>> 6, bitmap.get(i >>> 6) | (1L << i));
    }
    LongBuffer referenceBitmap = direct ? ByteBuffer.allocateDirect(8192).asLongBuffer() : LongBuffer.allocate(1024);
    CharBuffer array = direct ? ByteBuffer.allocateDirect(2 * set2.length).asCharBuffer() : CharBuffer.allocate(set2.length);
    int pos = 0;
    for (int i : set2) {
      referenceBitmap.put(i >>> 6, referenceBitmap.get(i >>> 6) | (1L << i));
      array.put(pos++, (char)i);
    }
    int expectedCardinality = 0;
    for (int i = 0; i < 1024; ++i) {
      referenceBitmap.put(i, referenceBitmap.get(i) & bitmap.get(i));
      expectedCardinality += Long.bitCount(referenceBitmap.get(i));
    }
    int cardinality = BufferUtil.intersectArrayIntoBitmap(bitmap, array, set2.length);
    assertEquals(expectedCardinality, cardinality);
    for (int i = 0; i < bitmap.limit(); ++i) {
      assertEquals(bitmap.get(i), referenceBitmap.get(i), "mismatch at " + i);
    }
  }

  @Test
  public void bitmapOfRange() {
    assertBitmapRange(0, 10);// begin of first container
    assertBitmapRange(0, 1 << 16 - 1);// early full container
    assertBitmapRange(0, 1 << 16);// full first container
    assertBitmapRange(0, 1 << 16 + 1);// full first container + one value the second
    assertBitmapRange(10, 1 << 16);// without first several integers
    assertBitmapRange(1 << 16, (1 << 16) * 2);// full second container
    assertBitmapRange(10, 100);// some integers inside interval
    assertBitmapRange((1 << 16) - 5, (1 << 16) + 7); // first to second container
    assertBitmapRange(0, 100_000); // more than one container
    assertBitmapRange(100_000, 200_000);// second to third container
    assertBitmapRange(200_000, 400_000);// more containers inside
  }

  private static void assertBitmapRange(int start, int end) {
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOfRange(start, end);
    assertEquals(end - start, bitmap.getCardinality());
    assertEquals(start, bitmap.first());
    assertEquals(end - 1, bitmap.last());
  }
}
