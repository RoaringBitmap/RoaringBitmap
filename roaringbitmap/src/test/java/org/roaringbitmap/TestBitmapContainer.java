/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.ValidationRangeConsumer.Value.ABSENT;
import static org.roaringbitmap.ValidationRangeConsumer.Value.PRESENT;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class TestBitmapContainer {
  private static BitmapContainer emptyContainer() {
    return new BitmapContainer(new long[1], 0);
  }

  static BitmapContainer generateContainer(char min, char max, int sample) {
    BitmapContainer bc = new BitmapContainer();
    for (int i = min; i < max; i++) {
      if (i % sample != 0) bc.add((char) i);
    }
    return bc;
  }

  @Test
  public void testToString() {
    BitmapContainer bc2 = new BitmapContainer(5, 15);
    bc2.add((char) -19);
    bc2.add((char) -3);
    assertEquals("{5,6,7,8,9,10,11,12,13,14,65517,65533}", bc2.toString());
  }

  @Test
  public void testXOR() {
    BitmapContainer bc = new BitmapContainer(100, 10000);
    BitmapContainer bc2 = new BitmapContainer();
    BitmapContainer bc3 = new BitmapContainer();

    for (int i = 100; i < 10000; ++i) {
      if ((i % 2) == 0) {
        bc2.add((char) i);
      } else {
        bc3.add((char) i);
      }
    }
    bc = (BitmapContainer) bc.ixor(bc2);
    assertEquals(0, bc.ixor(bc3).getCardinality());
  }

  @Test
  public void testANDNOT() {
    BitmapContainer bc = new BitmapContainer(100, 10000);
    BitmapContainer bc2 = new BitmapContainer();
    BitmapContainer bc3 = new BitmapContainer();

    for (int i = 100; i < 10000; ++i) {
      if ((i % 2) == 0) {
        bc2.add((char) i);
      } else {
        bc3.add((char) i);
      }
    }
    RunContainer rc = new RunContainer();
    rc.iadd(0, 1 << 16);
    bc = (BitmapContainer) bc.iand(rc);
    bc = (BitmapContainer) bc.iandNot(bc2);
    assertEquals(bc, bc3);
    assertEquals(bc.hashCode(), bc3.hashCode());
    assertEquals(0, bc.iandNot(bc3).getCardinality());
    bc3.clear();
    assertEquals(0, bc3.getCardinality());
  }

  @Test
  public void testAND() {
    BitmapContainer bc = new BitmapContainer(100, 10000);
    BitmapContainer bc2 = new BitmapContainer();
    BitmapContainer bc3 = new BitmapContainer();

    for (int i = 100; i < 10000; ++i) {
      if ((i % 2) == 0) {
        bc2.add((char) i);
      } else {
        bc3.add((char) i);
      }
    }
    bc = (BitmapContainer) bc.iand(bc2);
    assertEquals(bc, bc2);
    assertEquals(0, bc.iand(bc3).getCardinality());
  }

  @Test
  public void testOR() {
    BitmapContainer bc = new BitmapContainer(100, 10000);
    BitmapContainer bc2 = new BitmapContainer();
    BitmapContainer bc3 = new BitmapContainer();

    for (int i = 100; i < 10000; ++i) {
      if ((i % 2) == 0) {
        bc2.add((char) i);
      } else {
        bc3.add((char) i);
      }
    }
    bc2 = (BitmapContainer) bc2.ior(bc3);
    assertEquals(bc, bc2);
    bc2 = (BitmapContainer) bc2.ior(bc);
    assertEquals(bc, bc2);
    RunContainer rc = new RunContainer();
    rc.iadd(0, 1 << 16);
    assertEquals(0, bc.iandNot(rc).getCardinality());
  }

  @Test
  public void testLazyORFull() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    BitmapContainer bc2 = new BitmapContainer(3210, 1 << 16);
    Container result = bc.lazyor(bc2);
    Container iresult = bc.ilazyor(bc2);
    assertEquals(-1, result.getCardinality());
    assertEquals(-1, iresult.getCardinality());
    Container repaired = result.repairAfterLazy();
    Container irepaired = iresult.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertEquals(1 << 16, irepaired.getCardinality());
    assertTrue(repaired instanceof RunContainer);
    assertTrue(irepaired instanceof RunContainer);
  }

  @Test
  public void testLazyORFull2() {
    BitmapContainer bc = new BitmapContainer((1 << 10) - 200, 1 << 16);
    ArrayContainer ac = new ArrayContainer(0, 1 << 10);
    Container result = bc.lazyor(ac);
    Container iresult = bc.ilazyor(ac);
    assertEquals(-1, result.getCardinality());
    assertEquals(-1, iresult.getCardinality());
    Container repaired = result.repairAfterLazy();
    Container irepaired = iresult.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertEquals(1 << 16, irepaired.getCardinality());
    assertTrue(repaired instanceof RunContainer);
    assertTrue(irepaired instanceof RunContainer);
  }

  @Test
  public void testLazyORFull3() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    Container rc = Container.rangeOfOnes(1 << 15, 1 << 16);
    Container result = bc.lazyor((RunContainer) rc);
    Container iresult = bc.ilazyor((RunContainer) rc);
    assertEquals(-1, result.getCardinality());
    assertEquals(-1, iresult.getCardinality());
    Container repaired = result.repairAfterLazy();
    Container irepaired = iresult.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertEquals(1 << 16, irepaired.getCardinality());
    assertTrue(repaired instanceof RunContainer);
    assertTrue(irepaired instanceof RunContainer);
  }

  @Test
  public void runConstructorForBitmap() {
    System.out.println("runConstructorForBitmap");
    for (int start = 0; start <= (1 << 16); start += 4096) {
      for (int end = start; end <= (1 << 16); end += 4096) {
        BitmapContainer bc = new BitmapContainer(start, end);
        BitmapContainer bc2 = new BitmapContainer();
        BitmapContainer bc3 = (BitmapContainer) bc2.add(start, end);
        bc2.iadd(start, end);
        assertEquals(bc.getCardinality(), end - start);
        assertEquals(bc2.getCardinality(), end - start);
        assertEquals(bc, bc2);
        assertEquals(bc, bc3);
        assertEquals(0, bc2.remove(start, end).getCardinality());
        assertEquals(bc2.getCardinality(), end - start);
        assertEquals(0, bc2.not(start, end).getCardinality());
      }
    }
  }

  @Test
  public void runConstructorForBitmap2() {
    System.out.println("runConstructorForBitmap2");
    for (int start = 0; start <= (1 << 16); start += 63) {
      for (int end = start; end <= (1 << 16); end += 63) {
        BitmapContainer bc = new BitmapContainer(start, end);
        BitmapContainer bc2 = new BitmapContainer();
        BitmapContainer bc3 = (BitmapContainer) bc2.add(start, end);
        bc2.iadd(start, end);
        assertEquals(bc.getCardinality(), end - start);
        assertEquals(bc2.getCardinality(), end - start);
        assertEquals(bc, bc2);
        assertEquals(bc, bc3);
        assertEquals(0, bc2.remove(start, end).getCardinality());
        assertEquals(bc2.getCardinality(), end - start);
        assertEquals(0, bc2.not(start, end).getCardinality());
      }
    }
  }

  @Test
  public void testRangeCardinality() {
    BitmapContainer bc = generateContainer((char) 100, (char) 10000, 5);
    bc = (BitmapContainer) bc.add(200, 2000);
    assertEquals(8280, bc.cardinality);
  }

  @Test
  public void testRangeCardinality2() {
    BitmapContainer bc = generateContainer((char) 100, (char) 10000, 5);
    bc.iadd(200, 2000);
    assertEquals(8280, bc.cardinality);
  }

  @Test
  public void testRangeCardinality3() {
    BitmapContainer bc = generateContainer((char) 100, (char) 10000, 5);
    RunContainer rc = new RunContainer(new char[] {7, 300, 400, 900, 1400, 2200}, 3);
    bc.ior(rc);
    assertEquals(8677, bc.cardinality);
  }

  @Test
  public void testRangeCardinality4() {
    BitmapContainer bc = generateContainer((char) 100, (char) 10000, 5);
    RunContainer rc = new RunContainer(new char[] {7, 300, 400, 900, 1400, 2200}, 3);
    bc = (BitmapContainer) bc.andNot(rc);
    assertEquals(5274, bc.cardinality);
  }

  @Test
  public void testRangeCardinality5() {
    BitmapContainer bc = generateContainer((char) 100, (char) 10000, 5);
    RunContainer rc = new RunContainer(new char[] {7, 300, 400, 900, 1400, 2200}, 3);
    bc.iandNot(rc);
    assertEquals(5274, bc.cardinality);
  }

  @Test
  public void testRangeCardinality6() {
    BitmapContainer bc = generateContainer((char) 100, (char) 10000, 5);
    RunContainer rc = new RunContainer(new char[] {7, 300, 400, 900, 1400, 5200}, 3);
    bc = (BitmapContainer) bc.iand(rc);
    assertEquals(5046, bc.cardinality);
  }

  @Test
  public void testRangeCardinality7() {
    BitmapContainer bc = generateContainer((char) 100, (char) 10000, 5);
    RunContainer rc = new RunContainer(new char[] {7, 300, 400, 900, 1400, 2200}, 3);
    bc.ixor(rc);
    assertEquals(6031, bc.cardinality);
  }

  @Test
  public void numberOfRunsLowerBound1() {
    System.out.println("numberOfRunsLowerBound1");
    Random r = new Random(12345);

    for (double density = 0.001; density < 0.8; density *= 2) {

      ArrayList<Integer> values = new ArrayList<Integer>();
      for (int i = 0; i < 65536; ++i) {
        if (r.nextDouble() < density) {
          values.add(i);
        }
      }
      Integer[] positions = values.toArray(new Integer[0]);
      BitmapContainer bc = new BitmapContainer();

      for (int position : positions) {
        bc.add((char) position);
      }

      assertTrue(bc.numberOfRunsLowerBound(1) > 1);
      assertTrue(bc.numberOfRunsLowerBound(100) <= bc.numberOfRuns());

      // a big parameter like 100000 ensures that the full lower bound
      // is taken

      assertTrue(bc.numberOfRunsLowerBound(100000) <= bc.numberOfRuns());
      assertEquals(
          bc.numberOfRuns(), bc.numberOfRunsLowerBound(100000) + bc.numberOfRunsAdjustment());

      /*
       * the unrolled guys are commented out, did not help performance and slated for removal
       * soon...
       *
       * assertTrue(bc.numberOfRunsLowerBoundUnrolled2(1) > 1);
       * assertTrue(bc.numberOfRunsLowerBoundUnrolled2(100) <= bc.numberOfRuns());
       *
       * assertEquals(bc.numberOfRunsLowerBound(100000),
       * bc.numberOfRunsLowerBoundUnrolled2(100000));
       */
    }
  }

  @Test
  public void testNextTooLarge() {
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> emptyContainer().nextSetBit(Short.MAX_VALUE + 1));
  }

  @Test
  public void testNextTooSmall() {
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> emptyContainer().nextSetBit(-1));
  }

  @Test
  public void testPreviousTooLarge() {
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> emptyContainer().prevSetBit(Short.MAX_VALUE + 1));
  }

  @Test
  public void testPreviousTooSmall() {
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> emptyContainer().prevSetBit(-1));
  }

  @Test
  public void addInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Container bc = new BitmapContainer();
          bc.add(13, 1);
        });
  }

  @Test
  public void iaddInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Container bc = new BitmapContainer();
          bc.iadd(13, 1);
        });
  }

  @Test
  public void roundtrip() throws Exception {
    Container bc = new BitmapContainer();
    bc = bc.add(1, 5);
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oo = new ObjectOutputStream(bos)) {
      bc.writeExternal(oo);
    }
    Container bc2 = new BitmapContainer();
    final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    bc2.readExternal(new ObjectInputStream(bis));

    assertEquals(4, bc2.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(bc2.contains((char) i));
    }
  }

  @Test
  public void iorNot() {
    Container rc1 = new BitmapContainer();
    Container rc2 = new ArrayContainer();

    rc1.iadd(257, 258);
    rc2.iadd(128, 256);
    rc1 = rc1.iorNot(rc2, 258);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(257, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void iorNot2() {
    Container rc1 = new BitmapContainer();
    Container rc2 = new ArrayContainer();
    rc2.iadd(128, 256).iadd(257, 260);
    rc1 = rc1.iorNot(rc2, 261);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(260, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void iorNot3() {
    Container rc1 = new BitmapContainer();
    Container rc2 = new BitmapContainer();

    rc1.iadd(257, 258);
    rc2.iadd(128, 256);
    rc1 = rc1.iorNot(rc2, 258);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(257, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void iorNot4() {
    Container rc1 = new BitmapContainer();
    Container rc2 = new RunContainer();

    rc1.iadd(257, 258);
    rc2.iadd(128, 256);
    rc1 = rc1.iorNot(rc2, 258);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(257, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot() {
    final Container rc1 = new BitmapContainer();

    {
      Container rc2 = new ArrayContainer();
      rc2.iadd(128, 256);
      Container res = rc1.orNot(rc2, 257);
      assertEquals(129, res.getCardinality());

      PeekableCharIterator iterator = res.getCharIterator();
      for (int i = 0; i < 128; i++) {
        assertTrue(iterator.hasNext());
        assertEquals(i, iterator.next());
      }
      assertTrue(iterator.hasNext());
      assertEquals(256, iterator.next());

      assertFalse(iterator.hasNext());
    }

    {
      Container rc2 = new BitmapContainer();
      rc2.iadd(128, 256);
      Container res = rc1.orNot(rc2, 257);
      assertEquals(129, res.getCardinality());

      PeekableCharIterator iterator = res.getCharIterator();
      for (int i = 0; i < 128; i++) {
        assertTrue(iterator.hasNext());
        assertEquals(i, iterator.next());
      }
      assertTrue(iterator.hasNext());
      assertEquals(256, iterator.next());

      assertFalse(iterator.hasNext());
    }

    {
      Container rc2 = new RunContainer();
      rc2.iadd(128, 256);
      Container res = rc1.orNot(rc2, 257);
      assertEquals(129, res.getCardinality());

      PeekableCharIterator iterator = res.getCharIterator();
      for (int i = 0; i < 128; i++) {
        assertTrue(iterator.hasNext());
        assertEquals(i, iterator.next());
      }
      assertTrue(iterator.hasNext());
      assertEquals(256, iterator.next());

      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void orNot2() {
    Container rc1 = new BitmapContainer();
    Container rc2 = new ArrayContainer();
    rc2.iadd(128, 256).iadd(257, 260);
    rc1 = rc1.orNot(rc2, 261);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(260, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void iorRun() {
    Container bc = new BitmapContainer();
    bc = bc.add(1, 5);
    Container rc = new RunContainer();
    rc = rc.add(4, 10);
    bc.ior(rc);
    assertEquals(9, bc.getCardinality());
    for (int i = 1; i < 10; i++) {
      assertTrue(bc.contains((char) i));
    }
  }

  @Test
  public void orFullToRunContainer() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    BitmapContainer half = new BitmapContainer(1 << 15, 1 << 16);
    Container result = bc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof RunContainer);
  }

  @Test
  public void orFullToRunContainer2() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    ArrayContainer half = new ArrayContainer(1 << 15, 1 << 16);
    Container result = bc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof RunContainer);
  }

  @Test
  public void orFullToRunContainer3() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    BitmapContainer bc2 = new BitmapContainer(3210, 1 << 16);
    Container result = bc.or(bc2);
    Container iresult = bc.ior(bc2);
    assertEquals(1 << 16, result.getCardinality());
    assertEquals(1 << 16, iresult.getCardinality());
    assertTrue(result instanceof RunContainer);
    assertTrue(iresult instanceof RunContainer);
  }

  @Test
  public void orFullToRunContainer4() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    Container bc2 = Container.rangeOfOnes(3210, 1 << 16);
    Container iresult = bc.ior(bc2);
    assertEquals(1 << 16, iresult.getCardinality());
    assertTrue(iresult instanceof RunContainer);
  }

  @Test
  public void iremoveEmptyRange() {
    Container bc = new BitmapContainer();
    bc = bc.iremove(1, 1);
    assertEquals(0, bc.getCardinality());
  }

  @Test
  public void iremoveInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Container ac = new BitmapContainer();
          ac.iremove(13, 1);
        });
  }

  @Test
  public void iremove() {
    Container bc = new BitmapContainer();
    bc = bc.add(1, 10);
    bc = bc.iremove(5, 10);
    assertEquals(4, bc.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(bc.contains((char) i));
    }
  }

  @Test
  public void iremove2() {
    Container bc = new BitmapContainer();
    bc = bc.add(1, 8092);
    bc = bc.iremove(1, 10);
    assertEquals(8082, bc.getCardinality());
    for (int i = 10; i < 8092; i++) {
      assertTrue(bc.contains((char) i));
    }
  }

  @Test
  public void ixorRun() {
    Container bc = new BitmapContainer();
    bc = bc.add(1, 10);
    Container rc = new RunContainer();
    rc = rc.add(5, 15);
    bc = bc.ixor(rc);
    assertEquals(9, bc.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(bc.contains((char) i));
    }
    for (int i = 10; i < 15; i++) {
      assertTrue(bc.contains((char) i));
    }
  }

  @Test
  public void ixorRun2() {
    Container bc = new BitmapContainer();
    bc = bc.add(1, 8092);
    Container rc = new RunContainer();
    rc = rc.add(1, 10);
    bc = bc.ixor(rc);
    assertEquals(8082, bc.getCardinality());
    for (int i = 10; i < 8092; i++) {
      assertTrue(bc.contains((char) i));
    }
  }

  @Test
  public void selectInvalidPosition() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Container bc = new BitmapContainer();
          bc = bc.add(1, 13);
          bc.select(100);
        });
  }

  @Test
  public void removeInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Container ac = new BitmapContainer();
          ac.remove(13, 1);
        });
  }

  @Test
  public void remove() {
    Container bc = new BitmapContainer();
    bc = bc.add(1, 8092);
    bc = bc.remove(1, 10);
    assertEquals(8082, bc.getCardinality());
    for (int i = 10; i < 8092; i++) {
      assertTrue(bc.contains((char) i));
    }
  }

  @Test
  public void iandRun() {
    Container bc = new BitmapContainer();
    bc = bc.add(0, 8092);
    Container rc = new RunContainer();
    rc = rc.add(1, 10);
    bc = bc.iand(rc);
    assertEquals(9, bc.getCardinality());
    for (int i = 1; i < 10; i++) {
      assertTrue(bc.contains((char) i));
    }
  }

  @Test
  public void testFirst_Empty() {
    assertThrows(NoSuchElementException.class, () -> new BitmapContainer().first());
  }

  @Test
  public void testLast_Empty() {
    assertThrows(NoSuchElementException.class, () -> new BitmapContainer().last());
  }

  @Test
  public void testFirstLast() {
    Container rc = new ArrayContainer();
    final int firstInclusive = 1;
    int lastExclusive = firstInclusive;
    for (int i = 0; i < 1 << 16 - 10; ++i) {
      int newLastExclusive = lastExclusive + 10;
      rc = rc.add(lastExclusive, newLastExclusive);
      assertEquals(firstInclusive, rc.first());
      assertEquals(newLastExclusive - 1, rc.last());
      lastExclusive = newLastExclusive;
    }
  }

  @Test
  public void testContainsBitmapContainer_EmptyContainsEmpty() {
    Container bc = new BitmapContainer();
    Container subset = new BitmapContainer();
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_IncludeProperSubset() {
    Container bc = new BitmapContainer().add(0, 10);
    Container subset = new BitmapContainer().add(0, 9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_IncludeSelf() {
    Container bc = new BitmapContainer().add(0, 10);
    Container subset = new BitmapContainer().add(0, 10);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeSuperSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container superset = new BitmapContainer().add(0, 20);
    assertFalse(bc.contains(superset));
  }

  @Test
  public void testContainsBitmapContainer_IncludeProperSubsetDifferentStart() {
    Container bc = new BitmapContainer().add(0, 10);
    Container subset = new RunContainer().add(2, 9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeShiftedSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container shifted = new BitmapContainer().add(2, 12);
    assertFalse(bc.contains(shifted));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeDisJointSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container disjoint = new BitmapContainer().add(20, 40);
    assertFalse(bc.contains(disjoint));
    assertFalse(disjoint.contains(bc));
  }

  @Test
  public void testContainsRunContainer_EmptyContainsEmpty() {
    Container bc = new BitmapContainer();
    Container subset = new BitmapContainer();
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_IncludeProperSubset() {
    Container bc = new BitmapContainer().add(0, 10);
    Container subset = new RunContainer().add(0, 9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_IncludeSelf() {
    Container bc = new BitmapContainer().add(0, 10);
    Container subset = new RunContainer().add(0, 10);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeSuperSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container superset = new RunContainer().add(0, 20);
    assertFalse(bc.contains(superset));
  }

  @Test
  public void testContainsRunContainer_IncludeProperSubsetDifferentStart() {
    Container bc = new BitmapContainer().add(0, 10);
    Container subset = new RunContainer().add(2, 9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeShiftedSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container shifted = new RunContainer().add(2, 12);
    assertFalse(bc.contains(shifted));
  }

  @Test
  public void testContainsRunContainer_ExcludeDisJointSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container disjoint = new RunContainer().add(20, 40);
    assertFalse(bc.contains(disjoint));
    assertFalse(disjoint.contains(bc));
  }

  @Test
  public void testContainsArrayContainer_EmptyContainsEmpty() {
    Container bc = new BitmapContainer();
    Container subset = new ArrayContainer();
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_IncludeProperSubset() {
    Container bc = new BitmapContainer().add(0, 10);
    Container subset = new ArrayContainer().add(0, 9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_IncludeSelf() {
    Container bc = new BitmapContainer().add(0, 10);
    Container subset = new ArrayContainer().add(0, 10);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeSuperSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container superset = new ArrayContainer().add(0, 20);
    assertFalse(bc.contains(superset));
  }

  @Test
  public void testContainsArrayContainer_IncludeProperSubsetDifferentStart() {
    Container bc = new BitmapContainer().add(0, 10);
    Container subset = new ArrayContainer().add(2, 9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeShiftedSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container shifted = new ArrayContainer().add(2, 12);
    assertFalse(bc.contains(shifted));
  }

  @Test
  public void testContainsArrayContainer_ExcludeDisJointSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container disjoint = new ArrayContainer().add(20, 40);
    assertFalse(bc.contains(disjoint));
    assertFalse(disjoint.contains(bc));
  }

  @Test
  public void testIntersectsWithRange() {
    Container container = new BitmapContainer().add(0, 10);
    assertTrue(container.intersects(0, 1));
    assertTrue(container.intersects(0, 101));
    assertTrue(container.intersects(0, 1 << 16));
    assertFalse(container.intersects(11, lower16Bits(-1)));
  }

  public static Stream<Arguments> bitmapsForRangeIntersection() {
    return Stream.of(
        Arguments.of(new BitmapContainer().add((char) 60), 0, 61, true),
        Arguments.of(new BitmapContainer().add((char) 60), 0, 60, false),
        Arguments.of(new BitmapContainer().add((char) 1000), 0, 1001, true),
        Arguments.of(new BitmapContainer().add((char) 1000), 0, 1000, false),
        Arguments.of(new BitmapContainer().add((char) 1000), 0, 10000, true));
  }

  @ParameterizedTest
  @MethodSource("bitmapsForRangeIntersection")
  public void testIntersectsWithRangeUpperBoundaries(
      Container container, int min, int sup, boolean intersects) {
    assertEquals(intersects, container.intersects(min, sup));
  }

  @Test
  public void testIntersectsWithRangeHitScan() {
    Container container =
        new BitmapContainer().add(0, 10).add(500, 512).add(lower16Bits(-50), lower16Bits(-10));
    assertTrue(container.intersects(0, 1));
    assertTrue(container.intersects(0, 101));
    assertTrue(container.intersects(0, 1 << 16));
    assertTrue(container.intersects(11, 1 << 16));
    assertTrue(container.intersects(501, 511));
  }

  @Test
  public void testIntersectsWithRangeUnsigned() {
    Container container = new BitmapContainer().add(lower16Bits(-50), lower16Bits(-10));
    assertFalse(container.intersects(0, 1));
    assertTrue(container.intersects(0, lower16Bits(-40)));
    assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
    assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
    // assertTrue(container.intersects(11, (char)-1)); // forbidden
  }

  @Test
  public void testIntersectsAtEndWord() {
    Container container = new BitmapContainer().add(lower16Bits(-500), lower16Bits(-10));
    assertTrue(container.intersects(lower16Bits(-50), lower16Bits(-10)));
    assertTrue(container.intersects(lower16Bits(-400), lower16Bits(-11)));
    assertTrue(container.intersects(lower16Bits(-11), lower16Bits(-1)));
    assertFalse(container.intersects(lower16Bits(-10), lower16Bits(-1)));
  }

  @Test
  public void testIntersectsAtEndWord2() {
    Container container = new BitmapContainer().add(lower16Bits(500), lower16Bits(-500));
    assertTrue(container.intersects(lower16Bits(-650), lower16Bits(-500)));
    assertTrue(container.intersects(lower16Bits(-501), lower16Bits(-1)));
    assertFalse(container.intersects(lower16Bits(-500), lower16Bits(-1)));
    assertFalse(container.intersects(lower16Bits(-499), 1 << 16));
  }

  @Test
  public void testContainsRangeSingleWord() {
    long[] bitmap = evenBits();
    bitmap[10] = -1L;
    int cardinality = 32 + 1 << 15;
    BitmapContainer container = new BitmapContainer(bitmap, cardinality);
    assertTrue(container.contains(0, 1));
    assertTrue(container.contains(64 * 10, 64 * 11));
    assertFalse(container.contains(64 * 10, 2 + 64 * 11));
    assertTrue(container.contains(1 + 64 * 10, (64 * 11) - 1));
  }

  @Test
  public void testContainsRangeMultiWord() {
    long[] bitmap = evenBits();
    bitmap[10] = -1L;
    bitmap[11] = -1L;
    bitmap[12] |= ((1L << 32) - 1);
    int cardinality = 32 + 32 + 16 + 1 << 15;
    BitmapContainer container = new BitmapContainer(bitmap, cardinality);
    assertTrue(container.contains(0, 1));
    assertFalse(container.contains(64 * 10, (64 * 13) - 30));
    assertTrue(container.contains(64 * 10, (64 * 13) - 31));
    assertTrue(container.contains(1 + 64 * 10, (64 * 13) - 32));
    assertTrue(container.contains(64 * 10, 64 * 12));
    assertFalse(container.contains(64 * 10, 2 + 64 * 13));
  }

  @Test
  public void testContainsRangeSubWord() {
    long[] bitmap = evenBits();
    bitmap[bitmap.length - 1] = ~((1L << 63) | 1L);
    int cardinality = 32 + 32 + 16 + 1 << 15;
    BitmapContainer container = new BitmapContainer(bitmap, cardinality);
    assertFalse(container.contains(64 * 1023, 64 * 1024));
    assertFalse(container.contains(64 * 1023, 64 * 1024 - 1));
    assertTrue(container.contains(1 + 64 * 1023, 64 * 1024 - 1));
    assertTrue(container.contains(1 + 64 * 1023, 64 * 1024 - 2));
    assertFalse(container.contains(64 * 1023, 64 * 1023 + 2));
    assertTrue(container.contains(64 * 1023 + 1, 64 * 1023 + 2));
  }

  @Test
  public void testNextSetBit() {
    BitmapContainer container = new BitmapContainer(evenBits(), 1 << 15);
    assertEquals(0, container.nextSetBit(0));
    assertEquals(2, container.nextSetBit(1));
    assertEquals(2, container.nextSetBit(2));
    assertEquals(4, container.nextSetBit(3));
  }

  @Test
  public void testNextSetBitAfterEnd() {
    BitmapContainer container = new BitmapContainer(evenBits(), 1 << 15);
    container.bitmap[1023] = 0L;
    container.cardinality -= 32;
    assertEquals(-1, container.nextSetBit((64 * 1023) + 5));
  }

  @Test
  public void testNextSetBitBeforeStart() {
    BitmapContainer container = new BitmapContainer(evenBits(), 1 << 15);
    container.bitmap[0] = 0L;
    container.cardinality -= 32;
    assertEquals(64, container.nextSetBit(1));
  }

  @Test
  public void testNextValue() {
    BitmapContainer container = new ArrayContainer(new char[] {10, 20, 30}).toBitmapContainer();
    assertEquals(10, container.nextValue((char) 10));
    assertEquals(20, container.nextValue((char) 11));
    assertEquals(30, container.nextValue((char) 30));
  }

  @Test
  public void testNextValueAfterEnd() {
    BitmapContainer container = new ArrayContainer(new char[] {10, 20, 30}).toBitmapContainer();
    assertEquals(-1, container.nextValue((char) 31));
  }

  @Test
  public void testNextValue2() {
    BitmapContainer container = new BitmapContainer().iadd(64, 129).toBitmapContainer();
    assertEquals(64, container.nextValue((char) 0));
    assertEquals(64, container.nextValue((char) 64));
    assertEquals(65, container.nextValue((char) 65));
    assertEquals(128, container.nextValue((char) 128));
    assertEquals(-1, container.nextValue((char) 129));
    assertEquals(-1, container.nextValue((char) 5000));
  }

  @Test
  public void testNextValueBetweenRuns() {
    BitmapContainer container =
        new BitmapContainer().iadd(64, 129).iadd(256, 321).toBitmapContainer();
    assertEquals(64, container.nextValue((char) 0));
    assertEquals(64, container.nextValue((char) 64));
    assertEquals(65, container.nextValue((char) 65));
    assertEquals(128, container.nextValue((char) 128));
    assertEquals(256, container.nextValue((char) 129));
    assertEquals(-1, container.nextValue((char) 512));
  }

  @Test
  public void testNextValue3() {
    BitmapContainer container =
        new ArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201).toBitmapContainer();
    assertEquals(64, container.nextValue((char) 0));
    assertEquals(64, container.nextValue((char) 63));
    assertEquals(64, container.nextValue((char) 64));
    assertEquals(65, container.nextValue((char) 65));
    assertEquals(128, container.nextValue((char) 128));
    assertEquals(200, container.nextValue((char) 129));
    assertEquals(200, container.nextValue((char) 199));
    assertEquals(200, container.nextValue((char) 200));
    assertEquals(250, container.nextValue((char) 250));
    assertEquals(5000, container.nextValue((char) 2500));
    assertEquals(5000, container.nextValue((char) 5000));
    assertEquals(5200, container.nextValue((char) 5200));
    assertEquals(-1, container.nextValue((char) 5201));
  }

  @Test
  public void testNextValueUnsigned() {
    BitmapContainer container =
        new ArrayContainer(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)})
            .toBitmapContainer();
    assertEquals(((1 << 15) | 5), container.nextValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 5), container.nextValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 7), container.nextValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 7), container.nextValue((char) ((1 << 15) | 7)));
    assertEquals(-1, container.nextValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testPreviousValue1() {
    BitmapContainer container = new ArrayContainer().iadd(64, 129).toBitmapContainer();
    assertEquals(-1, container.previousValue((char) 0));
    assertEquals(-1, container.previousValue((char) 63));
    assertEquals(64, container.previousValue((char) 64));
    assertEquals(65, container.previousValue((char) 65));
    assertEquals(128, container.previousValue((char) 128));
    assertEquals(128, container.previousValue((char) 129));
  }

  @Test
  public void testPreviousValue2() {
    BitmapContainer container =
        new ArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201).toBitmapContainer();
    assertEquals(-1, container.previousValue((char) 0));
    assertEquals(-1, container.previousValue((char) 63));
    assertEquals(64, container.previousValue((char) 64));
    assertEquals(65, container.previousValue((char) 65));
    assertEquals(128, container.previousValue((char) 128));
    assertEquals(128, container.previousValue((char) 129));
    assertEquals(128, container.previousValue((char) 199));
    assertEquals(200, container.previousValue((char) 200));
    assertEquals(250, container.previousValue((char) 250));
    assertEquals(500, container.previousValue((char) 2500));
    assertEquals(5000, container.previousValue((char) 5000));
    assertEquals(5200, container.previousValue((char) 5200));
  }

  @Test
  public void testPreviousValueBeforeStart() {
    BitmapContainer container = new ArrayContainer(new char[] {10, 20, 30}).toBitmapContainer();
    assertEquals(-1, container.previousValue((char) 5));
  }

  @Test
  public void testPreviousValueSparse() {
    BitmapContainer container = new ArrayContainer(new char[] {10, 20, 30}).toBitmapContainer();
    assertEquals(-1, container.previousValue((char) 9));
    assertEquals(10, container.previousValue((char) 10));
    assertEquals(10, container.previousValue((char) 11));
    assertEquals(20, container.previousValue((char) 21));
    assertEquals(30, container.previousValue((char) 30));
  }

  @Test
  public void testPreviousValueAfterEnd() {
    BitmapContainer container = new ArrayContainer(new char[] {10, 20, 30}).toBitmapContainer();
    assertEquals(30, container.previousValue((char) 31));
  }

  @Test
  public void testPreviousEvenBits() {
    BitmapContainer container = new BitmapContainer(evenBits(), 1 << 15);
    assertEquals(0, container.previousValue((char) 0));
    assertEquals(0, container.previousValue((char) 1));
    assertEquals(2, container.previousValue((char) 2));
    assertEquals(2, container.previousValue((char) 3));
  }

  @Test
  public void testPreviousValueUnsigned() {
    BitmapContainer container =
        new ArrayContainer(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)})
            .toBitmapContainer();
    assertEquals(-1, container.previousValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 5), container.previousValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 5), container.previousValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 7), container.previousValue((char) ((1 << 15) | 7)));
    assertEquals(((1 << 15) | 7), container.previousValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testPreviousAbsentValue1() {
    BitmapContainer container = new ArrayContainer().iadd(64, 129).toBitmapContainer();
    assertEquals(0, container.previousAbsentValue((char) 0));
    assertEquals(63, container.previousAbsentValue((char) 63));
    assertEquals(63, container.previousAbsentValue((char) 64));
    assertEquals(63, container.previousAbsentValue((char) 65));
    assertEquals(63, container.previousAbsentValue((char) 128));
    assertEquals(129, container.previousAbsentValue((char) 129));
  }

  @Test
  public void testPreviousAbsentValue2() {
    BitmapContainer container =
        new ArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201).toBitmapContainer();
    assertEquals(0, container.previousAbsentValue((char) 0));
    assertEquals(63, container.previousAbsentValue((char) 63));
    assertEquals(63, container.previousAbsentValue((char) 64));
    assertEquals(63, container.previousAbsentValue((char) 65));
    assertEquals(63, container.previousAbsentValue((char) 128));
    assertEquals(129, container.previousAbsentValue((char) 129));
    assertEquals(199, container.previousAbsentValue((char) 199));
    assertEquals(199, container.previousAbsentValue((char) 200));
    assertEquals(199, container.previousAbsentValue((char) 250));
    assertEquals(2500, container.previousAbsentValue((char) 2500));
    assertEquals(4999, container.previousAbsentValue((char) 5000));
    assertEquals(4999, container.previousAbsentValue((char) 5200));
  }

  @Test
  public void testPreviousAbsentValueEmpty() {
    BitmapContainer container = new ArrayContainer().toBitmapContainer();
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, container.previousAbsentValue((char) i));
    }
  }

  @Test
  public void testPreviousAbsentValueSparse() {
    BitmapContainer container = new ArrayContainer(new char[] {10, 20, 30}).toBitmapContainer();
    assertEquals(9, container.previousAbsentValue((char) 9));
    assertEquals(9, container.previousAbsentValue((char) 10));
    assertEquals(11, container.previousAbsentValue((char) 11));
    assertEquals(21, container.previousAbsentValue((char) 21));
    assertEquals(29, container.previousAbsentValue((char) 30));
  }

  @Test
  public void testPreviousAbsentEvenBits() {
    BitmapContainer container = new BitmapContainer(evenBits(), 1 << 15);
    for (int i = 0; i < 1 << 10; i += 2) {
      assertEquals(i - 1, container.previousAbsentValue((char) i));
      assertEquals(i + 1, container.previousAbsentValue((char) (i + 1)));
    }
  }

  @Test
  public void testPreviousAbsentValueUnsigned() {
    BitmapContainer container =
        new ArrayContainer(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)})
            .toBitmapContainer();
    assertEquals(((1 << 15) | 4), container.previousAbsentValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 4), container.previousAbsentValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 6), container.previousAbsentValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 6), container.previousAbsentValue((char) ((1 << 15) | 7)));
    assertEquals(((1 << 15) | 8), container.previousAbsentValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testNextAbsentValue1() {
    BitmapContainer container = new ArrayContainer().iadd(64, 129).toBitmapContainer();
    assertEquals(0, container.nextAbsentValue((char) 0));
    assertEquals(63, container.nextAbsentValue((char) 63));
    assertEquals(129, container.nextAbsentValue((char) 64));
    assertEquals(129, container.nextAbsentValue((char) 65));
    assertEquals(129, container.nextAbsentValue((char) 128));
    assertEquals(129, container.nextAbsentValue((char) 129));
  }

  @Test
  public void testNextAbsentValue2() {
    BitmapContainer container =
        new ArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201).toBitmapContainer();
    assertEquals(0, container.nextAbsentValue((char) 0));
    assertEquals(63, container.nextAbsentValue((char) 63));
    assertEquals(129, container.nextAbsentValue((char) 64));
    assertEquals(129, container.nextAbsentValue((char) 65));
    assertEquals(129, container.nextAbsentValue((char) 128));
    assertEquals(129, container.nextAbsentValue((char) 129));
    assertEquals(199, container.nextAbsentValue((char) 199));
    assertEquals(501, container.nextAbsentValue((char) 200));
    assertEquals(501, container.nextAbsentValue((char) 250));
    assertEquals(2500, container.nextAbsentValue((char) 2500));
    assertEquals(5201, container.nextAbsentValue((char) 5000));
    assertEquals(5201, container.nextAbsentValue((char) 5200));
  }

  @Test
  public void testNextAbsentValueEmpty() {
    BitmapContainer container = new ArrayContainer().toBitmapContainer();
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, container.nextAbsentValue((char) i));
    }
  }

  @Test
  public void testNextAbsentValueSparse() {
    BitmapContainer container = new ArrayContainer(new char[] {10, 20, 30}).toBitmapContainer();
    assertEquals(9, container.nextAbsentValue((char) 9));
    assertEquals(11, container.nextAbsentValue((char) 10));
    assertEquals(11, container.nextAbsentValue((char) 11));
    assertEquals(21, container.nextAbsentValue((char) 21));
    assertEquals(31, container.nextAbsentValue((char) 30));
  }

  @Test
  public void testNextAbsentEvenBits() {
    BitmapContainer container = new BitmapContainer(evenBits(), 1 << 15);
    for (int i = 0; i < 1 << 10; i += 2) {
      assertEquals(i + 1, container.nextAbsentValue((char) i));
      assertEquals(i + 1, container.nextAbsentValue((char) (i + 1)));
    }
  }

  @Test
  public void testNextAbsentValueUnsigned() {
    BitmapContainer container =
        new ArrayContainer(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)})
            .toBitmapContainer();
    assertEquals(((1 << 15) | 4), container.nextAbsentValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 6), container.nextAbsentValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 6), container.nextAbsentValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 8), container.nextAbsentValue((char) ((1 << 15) | 7)));
    assertEquals(((1 << 15) | 8), container.nextAbsentValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testRangeConsumer() {
    char[] entries = new char[] {3, 4, 7, 8, 10, 65530, 65534, 65535};
    BitmapContainer container = new ArrayContainer(entries).toBitmapContainer();

    ValidationRangeConsumer consumer =
        ValidationRangeConsumer.validate(
            new ValidationRangeConsumer.Value[] {
              ABSENT, ABSENT, ABSENT, PRESENT, PRESENT, ABSENT, ABSENT, PRESENT, PRESENT, ABSENT,
              PRESENT
            });
    container.forAllUntil(0, (char) 11, consumer);
    assertEquals(11, consumer.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer2 =
        ValidationRangeConsumer.validate(
            new ValidationRangeConsumer.Value[] {PRESENT, ABSENT, ABSENT, PRESENT, PRESENT});
    container.forAllInRange((char) 4, (char) 9, consumer2);
    assertEquals(5, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 =
        ValidationRangeConsumer.validate(
            new ValidationRangeConsumer.Value[] {
              PRESENT, ABSENT, ABSENT, ABSENT, PRESENT, PRESENT
            });
    container.forAllFrom((char) 65530, consumer3);
    assertEquals(6, consumer3.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer4 =
        ValidationRangeConsumer.ofSize(BitmapContainer.MAX_CAPACITY);
    container.forAll(0, consumer4);
    consumer4.assertAllAbsentExcept(entries, 0);

    ValidationRangeConsumer consumer5 =
        ValidationRangeConsumer.ofSize(2 * BitmapContainer.MAX_CAPACITY);
    consumer5.acceptAllAbsent(0, BitmapContainer.MAX_CAPACITY);
    container.forAll(BitmapContainer.MAX_CAPACITY, consumer5);
    consumer5.assertAllAbsentExcept(entries, BitmapContainer.MAX_CAPACITY);

    // Completely Empty
    container = new BitmapContainer();
    ValidationRangeConsumer consumer6 =
        ValidationRangeConsumer.ofSize(BitmapContainer.MAX_CAPACITY);
    container.forAll(0, consumer6);
    consumer6.assertAllAbsent();

    // Completely Full
    container = new BitmapContainer();
    container.iadd(0, BitmapContainer.MAX_CAPACITY);
    ValidationRangeConsumer consumer7 =
        ValidationRangeConsumer.ofSize(BitmapContainer.MAX_CAPACITY);
    container.forAll(0, consumer7);
    consumer7.assertAllPresent();

    int middle = BitmapContainer.MAX_CAPACITY / 2;
    ValidationRangeConsumer consumer8 = ValidationRangeConsumer.ofSize(middle);
    container.forAllFrom((char) middle, consumer8);
    consumer8.assertAllPresent();

    ValidationRangeConsumer consumer9 = ValidationRangeConsumer.ofSize(middle);
    container.forAllUntil(0, (char) middle, consumer9);
    consumer9.assertAllPresent();

    int quarter = middle / 2;
    ValidationRangeConsumer consumer10 = ValidationRangeConsumer.ofSize(middle);
    container.forAllInRange((char) quarter, (char) (middle + quarter), consumer10);
    consumer10.assertAllPresent();
  }

  private static long[] evenBits() {
    long[] bitmap = new long[1 << 10];
    Arrays.fill(bitmap, 0x5555555555555555L);
    return bitmap;
  }

  private static int lower16Bits(int x) {
    return (char) x;
  }
}
