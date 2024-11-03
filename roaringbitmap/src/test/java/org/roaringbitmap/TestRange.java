package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

public class TestRange {
  @Test
  public void flip64() {
    RoaringBitmap rb = new RoaringBitmap();
    rb.add(0);
    rb.flip(1L, 2L);
    IntIterator i = rb.getIntIterator();
    assertEquals(0, i.next());
    assertEquals(1, i.next());
    assertFalse(i.hasNext());
  }

  private static int fillWithRandomBits(
      final RoaringBitmap bitmap, final BitSet bitset, final int bits) {
    int added = 0;
    Random r = new Random(1011);
    for (int j = 0; j < bits; j++) {
      if (r.nextBoolean()) {
        added++;
        bitmap.add(j);
        bitset.set(j);
      }
    }
    return added;
  }

  @Test
  public void doubleadd() {
    RoaringBitmap rb = new RoaringBitmap();
    rb.add(65533L, 65536L);
    rb.add(65530L, 65536L);
    BitSet bs = new BitSet();
    bs.set(65530, 65536);
    assertTrue(TestRoaringBitmap.equals(bs, rb));
    rb.remove(65530L, 65536L);
    assertEquals(0, rb.getCardinality());
  }

  @Test
  public void rangeAddRemoveBig() {
    final int numCases = 5000;
    RoaringBitmap rbstatic = new RoaringBitmap();
    final RoaringBitmap rbinplace = new RoaringBitmap();
    final BitSet bs = new BitSet();
    final Random r = new Random(3333);
    long start, end;
    for (int i = 0; i < numCases; ++i) {
      start = r.nextInt(65536 * 20);
      end = r.nextInt(65536 * 20);
      if (start > end) {
        long tmp = start;
        start = end;
        end = tmp;
      }
      rbinplace.add(start, end);
      rbstatic = RoaringBitmap.add(rbstatic, start, end);
      bs.set((int) start, (int) end);

      //
      start = r.nextInt(65536 * 20);
      end = r.nextInt(65536 * 20);
      if (start > end) {
        long tmp = start;
        start = end;
        end = tmp;
      }
      rbinplace.remove(start, end);
      rbstatic = RoaringBitmap.remove(rbstatic, start, end);
      bs.clear((int) start, (int) end);

      //
      start = r.nextInt(20) * 65536;
      end = r.nextInt(65536 * 20);
      if (start > end) {
        long tmp = start;
        start = end;
        end = tmp;
      }
      rbinplace.add(start, end);
      rbstatic = RoaringBitmap.add(rbstatic, start, end);
      bs.set((int) start, (int) end);

      //
      start = r.nextInt(65536 * 20);
      end = r.nextInt(20) * 65536;
      if (start > end) {
        long tmp = start;
        start = end;
        end = tmp;
      }
      rbinplace.add(start, end);
      rbstatic = RoaringBitmap.add(rbstatic, start, end);
      bs.set((int) start, (int) end);
      //
      start = r.nextInt(20) * 65536;
      end = r.nextInt(65536 * 20);
      if (start > end) {
        long tmp = start;
        start = end;
        end = tmp;
      }
      rbinplace.remove(start, end);
      rbstatic = RoaringBitmap.remove(rbstatic, start, end);
      bs.clear((int) start, (int) end);

      //
      start = r.nextInt(65536 * 20);
      end = r.nextInt(20) * 65536;
      if (start > end) {
        long tmp = start;
        start = end;
        end = tmp;
      }
      rbinplace.remove(start, end);
      rbstatic = RoaringBitmap.remove(rbstatic, start, end);
      bs.clear((int) start, (int) end);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rbstatic));
    assertTrue(TestRoaringBitmap.equals(bs, rbinplace));
    assertEquals(rbinplace, rbstatic);
  }

  @Test
  public void setRemoveTest1() {
    final BitSet bs = new BitSet();
    RoaringBitmap rb = new RoaringBitmap();
    bs.set(0, 1000000);
    rb.add(0L, 1000000L);
    rb.remove(43022L, 392542L);
    bs.clear(43022, 392542);
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setRemoveTest2() {
    final BitSet bs = new BitSet();
    RoaringBitmap rb = new RoaringBitmap();
    bs.set(43022, 392542);
    rb.add(43022L, 392542L);
    rb.remove(43022L, 392542L);
    bs.clear(43022, 392542);
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTest1() {
    final RoaringBitmap rb = new RoaringBitmap();

    rb.add(100000L, 200000L); // in-place on empty bitmap
    final int rbcard = rb.getCardinality();
    assertEquals(100000, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 100000; i < 200000; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTest1A() {
    final RoaringBitmap rb = new RoaringBitmap();

    final RoaringBitmap rb1 = RoaringBitmap.add(rb, 100000L, 200000L);
    final int rbcard = rb1.getCardinality();
    assertEquals(100000, rbcard);
    assertEquals(0, rb.getCardinality());

    final BitSet bs = new BitSet();
    assertTrue(TestRoaringBitmap.equals(bs, rb)); // still empty?
    for (int i = 100000; i < 200000; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb1));
  }

  @Test
  public void setTest2() {
    final RoaringBitmap rb = new RoaringBitmap();

    rb.add(100000L, 100000L);
    final int rbcard = rb.getCardinality();
    assertEquals(0, rbcard);

    final BitSet bs = new BitSet();
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTest2A() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb1 = RoaringBitmap.add(rb, 100000L, 100000L);
    rb.add(1); // will not affect rb1 (no shared container)
    final int rbcard = rb1.getCardinality();
    assertEquals(0, rbcard);
    assertEquals(1, rb.getCardinality());

    final BitSet bs = new BitSet();
    assertTrue(TestRoaringBitmap.equals(bs, rb1));
    bs.set(1);
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTest3() {
    final RoaringBitmap rb = new RoaringBitmap();

    rb.add(0L, 65536L);
    final int rbcard = rb.getCardinality();

    assertEquals(65536, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 0; i < 65536; ++i) {
      bs.set(i);
    }

    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTest3A() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb1 = RoaringBitmap.add(rb, 100000L, 200000L);
    final RoaringBitmap rb2 = RoaringBitmap.add(rb1, 500000L, 600000L);
    final int rbcard = rb2.getCardinality();

    assertEquals(200000, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 100000; i < 200000; ++i) {
      bs.set(i);
    }
    for (int i = 500000; i < 600000; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb2));
  }

  @Test
  public void setTest4() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(100000L, 200000L);
    rb.add(65536L, 4 * 65536L);
    final int rbcard = rb.getCardinality();

    assertEquals(196608, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 65536; i < 4 * 65536; ++i) {
      bs.set(i);
    }

    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTest4A() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb1 = RoaringBitmap.add(rb, 100000L, 200000L);
    final RoaringBitmap rb2 = RoaringBitmap.add(rb1, 65536L, 4 * 65536L);
    final int rbcard = rb2.getCardinality();

    assertEquals(196608, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 65536; i < 4 * 65536; ++i) {
      bs.set(i);
    }

    assertTrue(TestRoaringBitmap.equals(bs, rb2));
  }

  @Test
  public void setTest5() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(500L, 65536L * 3 + 500);
    rb.add(65536L, 65536L * 3);

    final int rbcard = rb.getCardinality();

    assertEquals(196608, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 500; i < 65536 * 3 + 500; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTest5A() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb1 = RoaringBitmap.add(rb, 100000L, 500000L);
    final RoaringBitmap rb2 = RoaringBitmap.add(rb1, 65536L, 120000L);
    final int rbcard = rb2.getCardinality();

    assertEquals(434464, rbcard);

    BitSet bs = new BitSet();
    for (int i = 65536; i < 500000; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb2));
  }

  @Test
  public void setTest6() { // fits evenly on big end, multiple containers
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(100000L, 132000L);
    rb.add(3 * 65536L, 4 * 65536L);
    final int rbcard = rb.getCardinality();

    assertEquals(97536, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 100000; i < 132000; ++i) {
      bs.set(i);
    }
    for (int i = 3 * 65536; i < 4 * 65536; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTest6A() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb1 = RoaringBitmap.add(rb, 100000L, 132000L);
    final RoaringBitmap rb2 = RoaringBitmap.add(rb1, 3 * 65536L, 4 * 65536L);
    final int rbcard = rb2.getCardinality();

    assertEquals(97536, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 100000; i < 132000; ++i) {
      bs.set(i);
    }
    for (int i = 3 * 65536; i < 4 * 65536; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb2));
  }

  @Test
  public void setTest7() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(10L, 50L);
    rb.add(1L, 9L);
    rb.add(130L, 185L);
    rb.add(6407L, 6460L);
    rb.add(325L, 380L);
    rb.add((65536L * 3) + 3, (65536L * 3) + 60);
    rb.add(65536L * 3 + 195, 65536L * 3 + 245);
    final int rbcard = rb.getCardinality();

    assertEquals(318, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 10; i < 50; ++i) {
      bs.set(i);
    }
    for (int i = 1; i < 9; ++i) {
      bs.set(i);
    }
    for (int i = 130; i < 185; ++i) {
      bs.set(i);
    }
    for (int i = 325; i < 380; ++i) {
      bs.set(i);
    }
    for (int i = 6407; i < 6460; ++i) {
      bs.set(i);
    }
    for (int i = 65536 * 3 + 3; i < 65536 * 3 + 60; ++i) {
      bs.set(i);
    }
    for (int i = 65536 * 3 + 195; i < 65536 * 3 + 245; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTest7A() {
    final RoaringBitmap rb = new RoaringBitmap();
    final BitSet bs = new BitSet();
    assertTrue(TestRoaringBitmap.equals(bs, rb));

    final RoaringBitmap rb1 = RoaringBitmap.add(rb, 10L, 50L);
    bs.set(10, 50);
    rb.add(10L, 50L);
    assertEquals(rb1, rb);
    assertTrue(TestRoaringBitmap.equals(bs, rb1));

    RoaringBitmap rb2 = RoaringBitmap.add(rb1, 130L, 185L);
    bs.set(130, 185);
    rb.add(130L, 185L);
    assertEquals(rb2, rb);
    assertTrue(TestRoaringBitmap.equals(bs, rb2));

    RoaringBitmap rb3 = RoaringBitmap.add(rb2, 6407L, 6460L);
    bs.set(6407, 6460);
    assertTrue(TestRoaringBitmap.equals(bs, rb3));
    rb2.add(6407L, 6460L);
    assertEquals(rb2, rb3);

    rb3 = RoaringBitmap.add(rb3, (65536L * 3) + 3, (65536L * 3) + 60);
    rb2.add((65536L * 3) + 3, (65536L * 3) + 60);
    bs.set((65536 * 3) + 3, (65536 * 3) + 60);
    assertEquals(rb2, rb3);
    assertTrue(TestRoaringBitmap.equals(bs, rb3));

    rb3 = RoaringBitmap.add(rb3, 65536L * 3 + 195, 65536L * 3 + 245);
    bs.set(65536 * 3 + 195, 65536 * 3 + 245);
    rb2.add(65536L * 3 + 195, 65536L * 3 + 245);
    assertEquals(rb2, rb3);
    assertTrue(TestRoaringBitmap.equals(bs, rb3));

    final int rbcard = rb3.getCardinality();

    assertEquals(255, rbcard);

    // now removing

    rb3 = RoaringBitmap.remove(rb3, 65536L * 3 + 195, 65536L * 3 + 245);
    bs.clear(65536 * 3 + 195, 65536 * 3 + 245);
    rb2.remove(65536L * 3 + 195, 65536L * 3 + 245);

    assertEquals(rb2, rb3);
    assertTrue(TestRoaringBitmap.equals(bs, rb3));

    rb3 = RoaringBitmap.remove(rb3, (65536L * 3) + 3, (65536L * 3) + 60);
    bs.clear((65536 * 3) + 3, (65536 * 3) + 60);
    rb2.remove((65536L * 3) + 3, (65536L * 3) + 60);

    assertEquals(rb2, rb3);
    assertTrue(TestRoaringBitmap.equals(bs, rb3));

    rb3 = RoaringBitmap.remove(rb3, 6407L, 6460L);
    bs.clear(6407, 6460);
    rb2.remove(6407L, 6460L);

    assertEquals(rb2, rb3);
    assertTrue(TestRoaringBitmap.equals(bs, rb3));

    rb2 = RoaringBitmap.remove(rb1, 130L, 185L);
    bs.clear(130, 185);
    rb.remove(130L, 185L);
    assertEquals(rb2, rb);
    assertTrue(TestRoaringBitmap.equals(bs, rb2));
  }

  @Test
  public void setTest8() {
    final RoaringBitmap rb = new RoaringBitmap();
    for (int i = 0; i < 5; i++) {
      for (long j = 0; j < 1024; j++) {
        rb.add(i * (1 << 16) + j * 64 + 2, i * (1 << 16) + j * 64 + 63);
      }
    }

    final int rbcard = rb.getCardinality();

    assertEquals(312320, rbcard);

    final BitSet bs = new BitSet();
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 1024; j++) {
        bs.set(i * (1 << 16) + j * 64 + 2, i * (1 << 16) + j * 64 + 63);
      }
    }

    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTestArrayContainer() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(500L, 3000L);
    rb.add(65536L, 66000L);
    final int rbcard = rb.getCardinality();

    assertEquals(2964, rbcard);

    BitSet bs = new BitSet();
    for (int i = 500; i < 3000; ++i) {
      bs.set(i);
    }
    for (int i = 65536; i < 66000; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTestArrayContainerA() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb1 = RoaringBitmap.add(rb, 500L, 3000L);
    final RoaringBitmap rb2 = RoaringBitmap.add(rb1, 65536L, 66000L);
    final int rbcard = rb2.getCardinality();

    assertEquals(2964, rbcard);

    BitSet bs = new BitSet();
    for (int i = 500; i < 3000; ++i) {
      bs.set(i);
    }
    for (int i = 65536; i < 66000; ++i) {
      bs.set(i);
    }
    assertTrue(TestRoaringBitmap.equals(bs, rb2));
  }

  @Test
  public void setTestSinglePonits() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(500L, 501L);
    rb.add(65536L, 65537L);
    final int rbcard = rb.getCardinality();

    assertEquals(2, rbcard);

    BitSet bs = new BitSet();
    bs.set(500);
    bs.set(65536);
    assertTrue(TestRoaringBitmap.equals(bs, rb));
  }

  @Test
  public void setTestSinglePonitsA() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb1 = RoaringBitmap.add(rb, 500L, 501L);
    final RoaringBitmap rb2 = RoaringBitmap.add(rb1, 65536L, 65537L);
    final int rbcard = rb2.getCardinality();

    assertEquals(2, rbcard);

    BitSet bs = new BitSet();
    bs.set(500);
    bs.set(65536);
    assertTrue(TestRoaringBitmap.equals(bs, rb2));
  }

  @Test
  public void testClearRanges() {
    long N = 16;
    for (long end = 1; end < N; ++end) {
      for (long start = 0; start < end; ++start) {
        RoaringBitmap bs1 = new RoaringBitmap();
        bs1.add(0L, N);
        for (int k = (int) start; k < end; ++k) {
          bs1.remove(k);
        }
        RoaringBitmap bs2 = new RoaringBitmap();
        bs2.add(0L, N);
        bs2.remove(start, end);
        assertEquals(bs1, bs2);
      }
    }
  }

  @Test
  public void testFlipRanges() {
    int N = 256;
    for (long end = 1; end < N; ++end) {
      for (long start = 0; start < end; ++start) {
        RoaringBitmap bs1 = new RoaringBitmap();
        for (int k = (int) start; k < end; ++k) {
          bs1.flip(k);
        }
        RoaringBitmap bs2 = new RoaringBitmap();
        bs2.flip(start, end);
        assertEquals(bs2.getCardinality(), end - start);
        assertEquals(bs1, bs2);
      }
    }
  }

  @Test
  public void testRangeRemoval() {
    final RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(1);
    assertTrue((bitmap.getCardinality() == 1) && bitmap.contains(1));
    bitmap.runOptimize();
    assertTrue((bitmap.getCardinality() == 1) && bitmap.contains(1));
    bitmap.removeRunCompression();
    assertTrue((bitmap.getCardinality() == 1) && bitmap.contains(1));
    bitmap.remove(0L, 1L); // should do nothing
    assertTrue((bitmap.getCardinality() == 1) && bitmap.contains(1));
    bitmap.remove(1L, 2L);
    bitmap.remove(1L, 2L); // should clear [1,2)
    assertTrue(bitmap.isEmpty());
  }

  @Test
  public void testRangeRemovalWithEightBitsAndRunlengthEncoding1() {
    final RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(1); // index 1
    bitmap.add(2);
    bitmap.add(3);
    bitmap.add(4);
    bitmap.add(5);
    bitmap.add(7); // index 7

    bitmap.runOptimize();
    // should remove from index 0 to 7
    bitmap.remove(0L, 8L);

    assertTrue(bitmap.isEmpty());
  }

  @Test
  public void testRangeRemovalWithEightBitsAndRunlengthEncoding2() {
    final RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(1); // index 1
    bitmap.add(2);
    bitmap.add(3);
    bitmap.add(4);
    bitmap.add(5);
    bitmap.add(7); // index 7

    bitmap.runOptimize();
    bitmap.removeRunCompression();
    assertFalse(bitmap.hasRunCompression());
    // should remove from index 0 to 7
    bitmap.remove(0L, 8L);
    assertTrue(bitmap.isEmpty());
  }

  private void testRangeRemovalWithRandomBits(boolean withRunCompression) {
    final int iterations = 4096;
    final int bits = 8;

    for (int i = 0; i < iterations; i++) {
      final BitSet bitset = new BitSet(bits);
      final RoaringBitmap bitmap = new RoaringBitmap();

      // check, if empty
      assertTrue(bitset.isEmpty());
      assertTrue(bitmap.isEmpty());

      // fill with random bits
      final int added = fillWithRandomBits(bitmap, bitset, bits);

      // check for cardinalities and if not empty
      assertTrue(added > 0 ? !bitmap.isEmpty() : bitmap.isEmpty());
      assertTrue(added > 0 ? !bitset.isEmpty() : bitset.isEmpty());
      assertEquals(added, bitmap.getCardinality());
      assertEquals(added, bitset.cardinality());

      // apply runtime compression or not
      if (withRunCompression) {
        bitmap.runOptimize();
        bitmap.removeRunCompression();
      }

      // clear and check bitmap, if really empty
      bitmap.remove(0L, (long) bits);
      assertEquals(0, bitmap.getCardinality(), "fails with bits: " + bitset);
      assertTrue(bitmap.isEmpty());

      // clear java's bitset
      bitset.clear(0, bits);
      assertEquals(0, bitset.cardinality());
      assertTrue(bitset.isEmpty());
    }
  }

  @Test
  public void testRemovalWithAddedAndRemovedRunOptimization() {
    // with runlength encoding
    testRangeRemovalWithRandomBits(true);
  }

  @Test
  public void testRemovalWithoutAddedAndRemovedRunOptimization() {
    // without runlength encoding
    testRangeRemovalWithRandomBits(false);
  }

  @Test
  public void testSetRanges() {
    int N = 256;
    for (long end = 1; end < N; ++end) {
      for (long start = 0; start < end; ++start) {
        RoaringBitmap bs1 = new RoaringBitmap();
        for (int k = (int) start; k < end; ++k) {
          bs1.add(k);
        }
        RoaringBitmap bs2 = new RoaringBitmap();
        bs2.add(start, end);
        assertEquals(bs1, bs2);
      }
    }
  }

  @Test
  public void testStaticClearRanges() {
    int N = 16;
    for (int end = 1; end < N; ++end) {
      for (int start = 0; start < end; ++start) {
        RoaringBitmap bs1 = new RoaringBitmap();
        bs1.add(0L, (long) N);
        for (int k = start; k < end; ++k) {
          bs1.remove(k);
        }
        RoaringBitmap bs2 = new RoaringBitmap();
        bs2 = RoaringBitmap.add(bs2, 0L, (long) N);
        bs2 = RoaringBitmap.remove(bs2, (long) start, (long) end);
        assertEquals(bs1, bs2);
      }
    }
  }

  @Test
  public void testStaticSetRanges() {
    int N = 256;
    for (int end = 1; end < N; ++end) {
      for (int start = 0; start < end; ++start) {
        RoaringBitmap bs1 = new RoaringBitmap();
        for (int k = start; k < end; ++k) {
          bs1.add(k);
        }
        RoaringBitmap bs2 = new RoaringBitmap();
        bs2 = RoaringBitmap.add(bs2, (long) start, (long) end);
        assertEquals(bs1, bs2);
      }
    }
  }

  /* nb on 20 April 2016, all unit tests above [then] were switched from
   * the original int-range functions to the wrapper int-range functions
   * without test errors.  Then all code above switched use longs for
   * range endpoints, again without test errors.
   *
   * Below, check the deprecated versions for undocumented behaviours that
   * hopefully don't rely on...but might have
   */

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedStaticAdd() {
    RoaringBitmap rb1 = RoaringBitmap.add(new RoaringBitmap(), 300000, 500000);
    RoaringBitmap rb2 = RoaringBitmap.add(new RoaringBitmap(), 300000L, 500000L);
    assertEquals(rb1, rb2);
    rb1 = RoaringBitmap.add(rb1, Integer.MAX_VALUE + 300000, Integer.MAX_VALUE + 500000);
    rb2 = RoaringBitmap.add(rb2, Integer.MAX_VALUE + 300000L, Integer.MAX_VALUE + 500000L);
    assertEquals(rb1, rb2);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedStaticFlip() {
    RoaringBitmap rb1 = RoaringBitmap.flip(new RoaringBitmap(), 300000, 500000);
    RoaringBitmap rb2 = RoaringBitmap.flip(new RoaringBitmap(), 300000L, 500000L);
    assertEquals(rb1, rb2);
    rb1 = RoaringBitmap.flip(rb1, Integer.MAX_VALUE + 300000, Integer.MAX_VALUE + 500000);
    rb2 = RoaringBitmap.flip(rb2, Integer.MAX_VALUE + 300000L, Integer.MAX_VALUE + 500000L);
    assertEquals(rb1, rb2);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedMemberFlip() {
    RoaringBitmap rb1 = new RoaringBitmap();
    rb1.flip(300000, 500000);
    RoaringBitmap rb2 = new RoaringBitmap();
    rb2.flip(300000L, 500000L);
    assertEquals(rb1, rb2);
    rb1.flip(Integer.MAX_VALUE + 300000, Integer.MAX_VALUE + 500000);
    rb2.flip(Integer.MAX_VALUE + 300000L, Integer.MAX_VALUE + 500000L);
    assertEquals(rb1, rb2);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedStaticRemove() {
    RoaringBitmap rb1 = RoaringBitmap.add(new RoaringBitmap(), 200000L, 400000L);
    rb1 = RoaringBitmap.remove(rb1, 300000, 500000);
    RoaringBitmap rb2 = RoaringBitmap.add(new RoaringBitmap(), 200000L, 400000L);
    rb2 = RoaringBitmap.remove(rb2, 300000L, 500000L);
    assertEquals(rb1, rb2);

    rb1 = RoaringBitmap.add(rb1, Integer.MAX_VALUE + 200000L, Integer.MAX_VALUE + 400000L);
    rb2 = RoaringBitmap.add(rb2, Integer.MAX_VALUE + 200000L, Integer.MAX_VALUE + 400000L);
    rb1 = RoaringBitmap.remove(rb1, Integer.MAX_VALUE + 300000, Integer.MAX_VALUE + 500000);
    rb2 = RoaringBitmap.remove(rb2, Integer.MAX_VALUE + 300000L, Integer.MAX_VALUE + 500000L);
    assertEquals(rb1, rb2);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedAdd() {
    RoaringBitmap rb1 = new RoaringBitmap();
    rb1.add(300000, 500000);
    RoaringBitmap rb2 = new RoaringBitmap();
    rb2.add(300000L, 500000L);
    assertEquals(rb1, rb2);
    rb1.add(Integer.MAX_VALUE + 300000, Integer.MAX_VALUE + 500000);
    rb2.add(Integer.MAX_VALUE + 300000L, Integer.MAX_VALUE + 500000L);
    assertEquals(rb1, rb2);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedRemove() {
    RoaringBitmap rb1 = new RoaringBitmap();
    rb1.add(200000L, 400000L);
    rb1.remove(300000, 500000);
    RoaringBitmap rb2 = new RoaringBitmap();
    rb2.add(200000L, 400000L);
    rb2.remove(300000L, 500000L);
    assertEquals(rb1, rb2);

    rb1.add(Integer.MAX_VALUE + 200000L, Integer.MAX_VALUE + 400000L);
    rb2.add(Integer.MAX_VALUE + 200000L, Integer.MAX_VALUE + 400000L);
    rb1.remove(Integer.MAX_VALUE + 300000, Integer.MAX_VALUE + 500000);
    rb2.remove(Integer.MAX_VALUE + 300000L, Integer.MAX_VALUE + 500000L);
    assertEquals(rb1, rb2);
  }

  // the other tests for ranged AND are in TestRoaringBitmap; the
  // range-with-longs is assumed to be okay and only lightly checked here
  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedIteratorAnd() {

    RoaringBitmap rb1 = new RoaringBitmap();
    RoaringBitmap rb2 = new RoaringBitmap();

    List<RoaringBitmap> list = new ArrayList<>();
    list.add(rb1);
    list.add(rb2);

    rb1.add(200000L, 400000L); // two normal positive ranges
    rb2.add(300000L, 500000L); // full overlap is on 300000 to 399999

    RoaringBitmap result = RoaringBitmap.and(list.iterator(), 350000L, 450000L);
    RoaringBitmap resultInt = RoaringBitmap.and(list.iterator(), 350000, 450000);

    assertEquals(result, resultInt);
    assertEquals(50000, result.getCardinality());

    // empty ranges get empty result
    resultInt = RoaringBitmap.and(list.iterator(), 300000, 200000);
    result = RoaringBitmap.and(list.iterator(), 300000L, 200000L);
    assertEquals(result, resultInt);
    assertEquals(0, resultInt.getCardinality());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedIteratorOr() {

    RoaringBitmap rb1 = new RoaringBitmap();
    RoaringBitmap rb2 = new RoaringBitmap();

    List<RoaringBitmap> list = new ArrayList<>();
    list.add(rb1);
    list.add(rb2);

    rb1.add(200000L, 400000L); // two normal positive ranges
    rb2.add(300000L, 500000L); // full union is 200000 to 499999

    RoaringBitmap result = RoaringBitmap.or(list.iterator(), 250000L, 550000L);
    RoaringBitmap resultInt = RoaringBitmap.or(list.iterator(), 250000, 550000);

    assertEquals(result, resultInt);
    assertEquals(250000, result.getCardinality());

    // empty ranges get empty result
    resultInt = RoaringBitmap.or(list.iterator(), 300000, 200000);
    result = RoaringBitmap.or(list.iterator(), 300000L, 200000L);
    assertEquals(result, resultInt);
    assertEquals(0, resultInt.getCardinality());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedIteratorAndNot() {

    RoaringBitmap rb1 = new RoaringBitmap();
    RoaringBitmap rb2 = new RoaringBitmap();

    List<RoaringBitmap> list = new ArrayList<>();
    list.add(rb1);
    list.add(rb2);

    rb1.add(200000L, 400000L); // two normal positive ranges
    rb2.add(300000L, 500000L); // full andNOToverlap is on 200000 to 299999

    RoaringBitmap result = RoaringBitmap.andNot(rb1, rb2, 250000L, 450000L);
    RoaringBitmap resultInt = RoaringBitmap.andNot(rb1, rb2, 250000, 450000);

    assertEquals(result, resultInt);
    assertEquals(50000, result.getCardinality());

    // empty ranges get empty result
    resultInt = RoaringBitmap.andNot(rb1, rb2, 300000, 200000);
    result = RoaringBitmap.andNot(rb1, rb2, 300000L, 200000L);
    assertEquals(result, resultInt);
    assertEquals(0, resultInt.getCardinality());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedIteratorXor() {

    RoaringBitmap rb1 = new RoaringBitmap();
    RoaringBitmap rb2 = new RoaringBitmap();

    List<RoaringBitmap> list = new ArrayList<>();
    list.add(rb1);
    list.add(rb2);

    rb1.add(200000L, 400000L); // two normal positive ranges
    rb2.add(300000L, 500000L); // full XOR is 200000 to 299999, 400000-4999999

    RoaringBitmap result = RoaringBitmap.xor(list.iterator(), 250000L, 450000L);
    RoaringBitmap resultInt = RoaringBitmap.xor(list.iterator(), 250000, 450000);

    assertEquals(result, resultInt);
    assertEquals(100000, result.getCardinality());

    // empty ranges get empty result
    resultInt = RoaringBitmap.xor(list.iterator(), 300000, 200000);
    result = RoaringBitmap.xor(list.iterator(), 300000L, 200000L);
    assertEquals(result, resultInt);
    assertEquals(0, resultInt.getCardinality());
  }
}
