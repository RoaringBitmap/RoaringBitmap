package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Random;

public class TestBitSetUtil {
  private static BitSet appendRandomBitset(
      final Random random, final int offset, final BitSet bitset, final int nbits) {
    for (int i = 0; i < nbits; i++) {
      final boolean b = random.nextBoolean();
      bitset.set(offset + i, b);
    }
    return bitset;
  }

  private static BitSet randomBitset(final Random random, final int offset, final int length) {
    final BitSet bitset = new BitSet();
    return appendRandomBitset(random, offset, bitset, length);
  }

  private void assertEqualBitsets(final BitSet bitset, final RoaringBitmap bitmap) {
    assertTrue(BitSetUtil.equals(bitset, bitmap), "bitset and bitmap do not match");
    assertEquals(bitset, BitSetUtil.bitsetOf(bitmap), "bitsetOf doesn't match");
    assertEquals(
        bitset, BitSetUtil.bitsetOfWithoutCopy(bitmap), "bitsetOfWithoutCopy doesn't match");
    assertArrayEquals(
        bitset.toByteArray(), BitSetUtil.toByteArray(bitmap), "toByteArray bytes don't match");
    assertEquals(
        bitset, BitSet.valueOf(BitSetUtil.toByteArray(bitmap)), "toByteArray doesn't match");
    assertArrayEquals(
        bitset.toLongArray(), BitSetUtil.toLongArray(bitmap), "toLongArray doesn't match");

    RoaringBitmap runBitmap = bitmap.clone();
    runBitmap.runOptimize();
    assertArrayEquals(
        bitset.toByteArray(),
        BitSetUtil.toByteArray(runBitmap),
        "toByteArray bytes don't match for run optimized bitmap");
    assertEquals(
        bitset, BitSetUtil.bitsetOf(runBitmap), "bitsetOf doesn't match for run optimized bitmap");
    assertArrayEquals(
        bitset.toLongArray(),
        BitSetUtil.toLongArray(runBitmap),
        "toLongArray doesn't match for run optimized bitmap");
  }

  @Test
  public void testEmptyBitSet() {
    final BitSet bitset = new BitSet();
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testFlipFlapBetweenRandomFullAndEmptyBitSet() {
    final Random random = new Random(1234);
    final int nbitsPerBlock = 1024 * Long.SIZE;
    final int blocks = 50;
    final BitSet bitset = new BitSet(nbitsPerBlock * blocks);

    // i want a mix of empty blocks, randomly filled blocks and full blocks
    for (int block = 0; block < blocks * nbitsPerBlock; block += nbitsPerBlock) {
      int type = random.nextInt(3);
      switch (type) {
        case 0:
          // a block with random set bits
          appendRandomBitset(random, block, bitset, nbitsPerBlock);
          break;
        case 1:
          // a full block
          bitset.set(block, block + nbitsPerBlock);
          break;
        default:
          // and an empty block;
          break;
      }
    }
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testFullBitSet() {
    final BitSet bitset = new BitSet();
    final int nbits = 1024 * Long.SIZE * 50;
    bitset.set(0, nbits);
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testGapBitmap() {
    for (int gap = 1; gap <= 4096; gap *= 2) {
      for (int offset = 300; offset < 3000; offset += 10) {
        BitSet bitset = new BitSet();
        for (int k = 0; k < 100000; k += gap) {
          bitset.set(k + offset);
        }
        final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
        assertEqualBitsets(bitset, bitmap);
      }
    }
  }

  @Test
  public void testRandomBitmap() {
    final Random random = new Random(1235);
    final int runs = 50;
    final int maxNbits = 500000;
    for (int i = 0; i < runs; i++) {
      final BitSet bitset = randomBitset(random, 0, random.nextInt(maxNbits));
      final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
      assertEqualBitsets(bitset, bitmap);
    }
  }

  @Test
  public void testRandomBitmap_extended() {
    final Random random = new Random(1245);
    final int runs = 50;
    final int maxNbits = 500000;
    for (int i = 0; i < runs; i++) {
      final BitSet bitset = randomBitset(random, 100000, random.nextInt(maxNbits));
      final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
      assertEqualBitsets(bitset, bitmap);
    }
  }

  @Test
  public void testSmallBitSet1() {
    final BitSet bitset = new BitSet();
    bitset.set(1);
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testSmallBitSet1_10000000() {
    final BitSet bitset = new BitSet();
    bitset.set(1);
    bitset.set(10000000);
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testLastValueAtWordBoundaries() {
    for (int value : new int[] {0, 1, 62, 63, 64, 65, 127, 128, 65535, 65536, 65599, 65600}) {
      final BitSet single = new BitSet();
      single.set(value);
      assertEqualBitsets(single, RoaringBitmap.bitmapOf(value));

      final BitSet range = new BitSet();
      range.set(0, value + 1);
      assertEqualBitsets(range, RoaringBitmap.bitmapOfRange(0, value + 1));
    }
  }

  @Test
  public void testSmallBitSet10000000() {
    final BitSet bitset = new BitSet();
    bitset.set(10000000);
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testLastValueAtByteBoundaries() {
    for (int value :
        new int[] {0, 1, 7, 8, 9, 55, 56, 63, 64, 65, 127, 128, 65535, 65536, 65543, 65544}) {
      final BitSet single = new BitSet();
      single.set(value);
      assertEqualBitsets(single, RoaringBitmap.bitmapOf(value));

      final BitSet range = new BitSet();
      range.set(0, value + 1);
      assertEqualBitsets(range, RoaringBitmap.bitmapOfRange(0, value + 1));
    }
  }

  @Test
  public void testToByteArrayMixedContainers() {
    final Random random = new Random(8570);
    for (int trial = 0; trial < 50; trial++) {
      final BitSet bitset = new BitSet();
      final RoaringBitmap bitmap = new RoaringBitmap();
      // containers of every type, with gaps, and the last one ending at an arbitrary bit
      final int containers = 1 + random.nextInt(6);
      for (int c = 0; c < containers; c++) {
        final int base = (2 * c + random.nextInt(2)) << 16;
        switch (random.nextInt(3)) {
          case 0: // array
            for (int k = 0; k < 100; k++) {
              final int value = base + random.nextInt(1 << 16);
              bitset.set(value);
              bitmap.add(value);
            }
            break;
          case 1: // bitmap
            for (int k = 0; k < 10000; k++) {
              final int value = base + random.nextInt(1 << 16);
              bitset.set(value);
              bitmap.add(value);
            }
            break;
          default: // run
            final int start = base + random.nextInt(1 << 15);
            final int end = start + 1 + random.nextInt(1 << 15);
            bitset.set(start, end);
            bitmap.add((long) start, (long) end);
            break;
        }
      }
      bitmap.runOptimize();
      assertArrayEquals(bitset.toByteArray(), BitSetUtil.toByteArray(bitmap));
    }
  }

  /*
   The ByteBuffer->RoaringBitmap just replicate similar tests written for BitSet/long[]->RoaringBitmap
  */

  @Test
  public void testEmptyByteBuffer() {
    final BitSet bitset = new BitSet();
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(toByteBuffer(bitset), false);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testFlipFlapBetweenRandomFullAndEmptyByteBuffer() {
    final Random random = new Random(1234);
    final int nbitsPerBlock = 1024 * Long.SIZE;
    final int blocks = 50;
    final BitSet bitset = new BitSet(nbitsPerBlock * blocks);

    // i want a mix of empty blocks, randomly filled blocks and full blocks
    for (int block = 0; block < blocks * nbitsPerBlock; block += nbitsPerBlock) {
      int type = random.nextInt(3);
      switch (type) {
        case 0:
          // a block with random set bits
          appendRandomBitset(random, block, bitset, nbitsPerBlock);
          break;
        case 1:
          // a full block
          bitset.set(block, block + nbitsPerBlock);
          break;
        default:
          // and an empty block;
          break;
      }
    }
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(toByteBuffer(bitset), false);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testFullByteBuffer() {
    final BitSet bitset = new BitSet();
    final int nbits = 1024 * Long.SIZE * 50;
    bitset.set(0, nbits);
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(toByteBuffer(bitset), false);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testGapByteBuffer() {
    for (int gap = 1; gap <= 4096; gap *= 2) {
      for (int offset = 300; offset < 3000; offset += 10) {
        BitSet bitset = new BitSet();
        for (int k = 0; k < 100000; k += gap) {
          bitset.set(k + offset);
        }
        final RoaringBitmap bitmap = BitSetUtil.bitmapOf(toByteBuffer(bitset), false);
        assertEqualBitsets(bitset, bitmap);
      }
    }
  }

  @Test
  public void testRandomByteBuffer() {
    final Random random = new Random(8934);
    final int runs = 100;
    final int maxNbits = 500000;
    for (int i = 0; i < runs; ++i) {
      final int offset = random.nextInt(maxNbits) & Integer.MAX_VALUE;
      final BitSet bitset = randomBitset(random, offset, random.nextInt(maxNbits));
      final RoaringBitmap bitmap = BitSetUtil.bitmapOf(toByteBuffer(bitset), false);
      assertEqualBitsets(bitset, bitmap);
    }
  }

  @Test
  public void testByteArrayWithOnly10000000thBitSet() {
    final BitSet bitset = new BitSet();
    bitset.set(10000000);
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(toByteBuffer(bitset), false);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testByteArrayWithOnly1And10000000thBitSet() {
    final BitSet bitset = new BitSet();
    bitset.set(1);
    bitset.set(10000000);
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(toByteBuffer(bitset), false);
    assertEqualBitsets(bitset, bitmap);
  }

  @Test
  public void testByteArrayWithFastRank() {
    final BitSet bitset = randomBitset(new Random(238), 0, 50);
    final RoaringBitmap bitmap = BitSetUtil.bitmapOf(toByteBuffer(bitset), true);
    Assertions.assertTrue(bitmap instanceof FastRankRoaringBitmap);
  }

  @Test
  public void testBitmapOfNegative() {
    final RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(-1);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              BitSetUtil.bitsetOf(bitmap);
            });
    assertEquals("bitmap has negative bits set", exception.getMessage());
  }

  private static ByteBuffer toByteBuffer(BitSet bitset) {
    return ByteBuffer.wrap(bitset.toByteArray());
  }
}
