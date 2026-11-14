package org.roaringbitmap.longlong;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigInteger;
import java.util.Random;
import java.util.TreeSet;

public class TestRoaring64Offset {
  private static final BigInteger LIMIT = BigInteger.ONE.shiftLeft(64);
  private static final long[] OFFSETS = {
    0,
    1,
    -1,
    63,
    -63,
    64,
    -64,
    65535,
    -65535,
    65536,
    -65536,
    65537,
    -65537,
    Long.MAX_VALUE,
    Long.MIN_VALUE
  };

  private void check(Roaring64Bitmap source, long offset) {
    long[] before = source.toArray();
    TreeSet<Long> expected = new TreeSet<>(Long::compareUnsigned);
    for (long value : before) {
      BigInteger shifted =
          new BigInteger(Long.toUnsignedString(value)).add(BigInteger.valueOf(offset));
      if (shifted.signum() >= 0 && shifted.compareTo(LIMIT) < 0) {
        expected.add(shifted.longValue());
      }
    }
    Roaring64Bitmap result = Roaring64Bitmap.addOffset(source, offset);
    assertArrayEquals(
        expected.stream().mapToLong(Long::longValue).toArray(),
        result.toArray(),
        "offset " + offset);
    assertTrue(result.validate());
    assertArrayEquals(before, source.toArray());
    if (!result.isEmpty()) {
      long first = result.first();
      result.removeLong(first);
      assertArrayEquals(before, source.toArray(), "result mutation changed source");
    }
  }

  @Test
  public void boundaryValues() {
    Roaring64Bitmap source = new Roaring64Bitmap();
    for (long value :
        new long[] {
          0,
          1,
          65534,
          65535,
          65536,
          65537,
          Long.MAX_VALUE - 1,
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          Long.MIN_VALUE + 1,
          -65537,
          -65536,
          -65535,
          -2,
          -1
        }) {
      source.addLong(value);
    }
    for (long offset : OFFSETS) {
      check(source, offset);
      check(new Roaring64Bitmap(), offset);
    }
  }

  @Test
  public void allContainerEncodingsAndAdjacentCarries() {
    for (int encoding = 0; encoding < 3; encoding++) {
      Roaring64Bitmap source = new Roaring64Bitmap();
      for (long base : new long[] {0, 65536, Long.MAX_VALUE - 65535, Long.MIN_VALUE, -65536}) {
        if (encoding == 2) {
          // A full block; base + 65536 would wrap to 0 for the last block, so add its last
          // value separately.
          source.addRange(base, base + 65535);
          source.addLong(base + 65535);
        } else {
          int count = encoding == 0 ? 20 : 10000;
          for (int i = 0; i < count; i++) {
            source.addLong(base + (i * 31L % 65536));
          }
        }
      }
      if (encoding == 2) {
        source.runOptimize();
      }
      for (long offset : OFFSETS) {
        check(source, offset);
      }
    }
  }

  @Test
  public void randomizedUnsignedOracle() {
    Random random = new Random(709);
    for (int trial = 0; trial < 30; trial++) {
      Roaring64Bitmap source = new Roaring64Bitmap();
      for (int i = 0; i < 250; i++) {
        source.addLong(random.nextLong());
      }
      check(source, random.nextLong());
    }
  }

  @Test
  public void copiesAreIndependentAndSerialize() throws IOException {
    for (long offset : new long[] {0, 65536, -65536, 1, -1}) {
      Roaring64Bitmap source = new Roaring64Bitmap();
      source.addRange(100000, 300000);
      source.runOptimize();
      Roaring64Bitmap shifted = Roaring64Bitmap.addOffset(source, offset);
      long[] expected = shifted.toArray();
      source.removeLong(150000);
      source.addLong(400000);
      assertArrayEquals(expected, shifted.toArray());
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      shifted.serializePortable(new DataOutputStream(bytes));
      Roaring64Bitmap restored = new Roaring64Bitmap();
      restored.deserializePortable(
          new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
      assertTrue(restored.validate());
      assertArrayEquals(expected, restored.toArray());
    }
  }

  @Test
  public void lastUnsignedContainerDropsOnlyOverflowingCarry() {
    Roaring64Bitmap source = Roaring64Bitmap.bitmapOf(-65536L, -65535L, -2L, -1L);
    Roaring64Bitmap result = Roaring64Bitmap.addOffset(source, 1);
    assertArrayEquals(new long[] {-65535L, -65534L, -1L}, result.toArray());
    assertFalse(result.contains(0));
    assertEquals(3, result.getLongCardinality());
    assertTrue(result.validate());
    assertArrayEquals(new long[] {-65536L, -65535L, -2L, -1L}, source.toArray());
  }

  @Test
  public void partialDropsAtBothEnds() {
    Roaring64Bitmap source = new Roaring64Bitmap();
    for (long k = 0; k < 8; k++) {
      source.addLong((k << 16) + 7);
      source.addLong((k << 16) + 65530);
      source.addLong(-(k << 16) - 1);
      source.addLong(-(k << 16) - 65530);
    }
    for (long blocks = 0; blocks < 10; blocks++) {
      for (long low : new long[] {0, 1, 6, 7, 8, 65529, 65535}) {
        check(source, (blocks << 16) + low);
        check(source, -(blocks << 16) - low);
      }
    }
  }

  @Test
  public void splitRunsUseTheMostCompactEncoding() throws IOException {
    for (long base : new long[] {0, 65536 * 5, Long.MIN_VALUE, -65536 * 2}) {
      Roaring64Bitmap source = new Roaring64Bitmap();
      source.addRange(base, base + 65535);
      source.addLong(base + 65535);
      source.addRange(base + 65536 + 10, base + 65536 + 20);
      source.runOptimize();
      for (long offset : new long[] {1, 2, 32768, 65535, 65536 + 1, -1, -65535}) {
        Roaring64Bitmap result = Roaring64Bitmap.addOffset(source, offset);
        assertTrue(result.validate());
        long size = result.serializedSizeInBytes();
        result.runOptimize();
        assertEquals(
            size, result.serializedSizeInBytes(), "base " + base + " offset " + offset);
      }
    }
  }

  @Test
  public void hugeCompressedRange() {
    Roaring64Bitmap source = new Roaring64Bitmap();
    source.addRange(0, 1L << 32);
    Roaring64Bitmap shifted = Roaring64Bitmap.addOffset(source, 65535);
    assertEquals(1L << 32, shifted.getLongCardinality());
    assertEquals(65535, shifted.first());
    assertEquals((1L << 32) + 65534, shifted.last());
    assertTrue(shifted.validate());
    assertEquals(0, source.first());
  }
}
