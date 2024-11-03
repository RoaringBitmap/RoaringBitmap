package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.ValidationRangeConsumer.Value.ABSENT;
import static org.roaringbitmap.ValidationRangeConsumer.Value.PRESENT;

import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

@Execution(ExecutionMode.CONCURRENT)
public class TestArrayContainer {

  @Test
  public void testConst() {
    ArrayContainer ac1 = new ArrayContainer(5, 15);
    char[] data = {5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
    ArrayContainer ac2 = new ArrayContainer(data);
    assertEquals(ac1, ac2);
  }

  @Test
  public void testRemove() {
    ArrayContainer ac1 = new ArrayContainer(5, 15);
    ac1.remove((char) 14);
    ArrayContainer ac2 = new ArrayContainer(5, 14);
    assertEquals(ac1, ac2);
  }

  @Test
  public void arrayContainersNeverFull() {
    assertFalse(new ArrayContainer(5, 15).isFull());
  }

  @Test
  public void testToString() {
    ArrayContainer ac1 = new ArrayContainer(5, 15);
    ac1.add((char) -3);
    ac1.add((char) -17);
    assertEquals("{5,6,7,8,9,10,11,12,13,14,65519,65533}", ac1.toString());
  }

  @Test
  public void testIandNot() {
    ArrayContainer ac1 = new ArrayContainer(5, 15);
    ArrayContainer ac2 = new ArrayContainer(10, 15);
    BitmapContainer bc = new BitmapContainer(5, 10);
    ArrayContainer ac3 = ac1.iandNot(bc);
    assertEquals(ac2, ac3);
  }

  @Test
  public void testReverseArrayContainerShortIterator() {
    // Test Clone
    ArrayContainer ac1 = new ArrayContainer(5, 15);
    ReverseArrayContainerCharIterator rac1 = new ReverseArrayContainerCharIterator(ac1);
    CharIterator rac2 = rac1.clone();
    assertNotNull(rac2);
    assertEquals(asList(rac1), asList(rac2));
  }

  private static List<Integer> asList(CharIterator ints) {
    int[] values = new int[10];
    int size = 0;
    while (ints.hasNext()) {
      if (!(size < values.length)) {
        values = Arrays.copyOf(values, values.length * 2);
      }
      values[size++] = ints.next();
    }
    return Ints.asList(Arrays.copyOf(values, size));
  }

  @Test
  public void roundtrip() throws Exception {
    Container ac = new ArrayContainer();
    ac = ac.add(1, 5);
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oo = new ObjectOutputStream(bos)) {
      ac.writeExternal(oo);
    }
    Container ac2 = new ArrayContainer();
    final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ac2.readExternal(new ObjectInputStream(bis));

    assertEquals(4, ac2.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(ac2.contains((char) i));
    }
  }

  @Test
  public void intersectsArray() {
    Container ac = new ArrayContainer();
    ac = ac.add(1, 10);
    Container ac2 = new ArrayContainer();
    ac2 = ac2.add(5, 25);
    assertTrue(ac.intersects(ac2));
  }

  @Test
  public void orFullToRunContainer() {
    ArrayContainer ac = new ArrayContainer(0, 1 << 12);
    BitmapContainer half = new BitmapContainer(1 << 12, 1 << 16);
    Container result = ac.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof RunContainer);
  }

  @Test
  public void orFullToRunContainer2() {
    ArrayContainer ac = new ArrayContainer(0, 1 << 15);
    ArrayContainer half = new ArrayContainer(1 << 15, 1 << 16);
    Container result = ac.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof RunContainer);
  }

  @Test
  public void iandBitmap() {
    Container ac = new ArrayContainer();
    ac = ac.add(1, 10);
    Container bc = new BitmapContainer();
    bc = bc.add(5, 25);
    ac.iand(bc);
    assertEquals(5, ac.getCardinality());
    for (int i = 5; i < 10; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void iandRun() {
    Container ac = new ArrayContainer();
    ac = ac.add(1, 10);
    Container rc = new RunContainer();
    rc = rc.add(5, 25);
    ac = ac.iand(rc);
    assertEquals(5, ac.getCardinality());
    for (int i = 5; i < 10; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void addEmptyRange() {
    Container ac = new ArrayContainer();
    ac = ac.add(1, 1);
    assertEquals(0, ac.getCardinality());
  }

  @Test
  public void addInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Container ac = new ArrayContainer();
          ac.add(13, 1);
        });
  }

  @Test
  public void iaddEmptyRange() {
    Container ac = new ArrayContainer();
    ac = ac.iadd(1, 1);
    assertEquals(0, ac.getCardinality());
  }

  @Test
  public void iaddInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Container ac = new ArrayContainer();
          ac.iadd(13, 1);
        });
  }

  @Test
  public void iaddSanityTest() {
    Container ac = new ArrayContainer();
    ac = ac.iadd(10, 20);
    // insert disjoint at end
    ac = ac.iadd(30, 70);
    // insert disjoint between
    ac = ac.iadd(25, 26);
    // insert disjoint at start
    ac = ac.iadd(1, 2);
    // insert overlap at end
    ac = ac.iadd(60, 80);
    // insert overlap between
    ac = ac.iadd(10, 30);
    // insert overlap at start
    ac = ac.iadd(1, 20);
    assertEquals(79, ac.getCardinality());
  }

  @Test
  public void clear() {
    Container ac = new ArrayContainer();
    ac = ac.add(1, 10);
    ac.clear();
    assertEquals(0, ac.getCardinality());
  }

  @Test
  public void testLazyORFull() {
    ArrayContainer ac = new ArrayContainer(0, 1 << 15);
    ArrayContainer ac2 = new ArrayContainer(1 << 15, 1 << 16);
    Container rbc = ac.lazyor(ac2);
    assertEquals(-1, rbc.getCardinality());
    Container repaired = rbc.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertTrue(repaired instanceof RunContainer);
  }

  @Test
  public void testFirst_Empty() {
    assertThrows(NoSuchElementException.class, () -> new ArrayContainer().first());
  }

  @Test
  public void testLast_Empty() {
    assertThrows(NoSuchElementException.class, () -> new ArrayContainer().last());
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
  public void testContainsBitmapContainer_ExcludeShiftedSet() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new BitmapContainer().add(2, 12);
    assertFalse(ac.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_AlwaysFalse() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new BitmapContainer().add(0, 10);
    assertFalse(ac.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeSuperSet() {
    Container ac = new ArrayContainer().add(0, 10);
    Container superset = new BitmapContainer().add(0, 20);
    assertFalse(ac.contains(superset));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeDisJointSet() {
    Container ac = new ArrayContainer().add(0, 10);
    Container disjoint = new BitmapContainer().add(20, 40);
    assertFalse(ac.contains(disjoint));
    assertFalse(disjoint.contains(ac));
  }

  @Test
  public void testContainsRunContainer_EmptyContainsEmpty() {
    Container ac = new ArrayContainer();
    Container subset = new RunContainer();
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsRunContainer_IncludeProperSubset() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new RunContainer().add(0, 9);
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsRunContainer_IncludeSelf() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new RunContainer().add(0, 10);
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeSuperSet() {
    Container ac = new ArrayContainer().add(0, 10);
    Container superset = new RunContainer().add(0, 20);
    assertFalse(ac.contains(superset));
  }

  @Test
  public void testContainsRunContainer_IncludeProperSubsetDifferentStart() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new RunContainer().add(1, 9);
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeShiftedSet() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new RunContainer().add(2, 12);
    assertFalse(ac.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeDisJointSet() {
    Container ac = new ArrayContainer().add(0, 10);
    Container disjoint = new RunContainer().add(20, 40);
    assertFalse(ac.contains(disjoint));
    assertFalse(disjoint.contains(ac));
  }

  @Test
  public void testContainsRunContainer_Issue723Case1() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new RunContainer().add(5, 6);
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsRunContainer_Issue723Case2() {
    Container ac = new ArrayContainer().add(0, 10);
    Container rc = new RunContainer().add(5, 11);
    assertFalse(ac.contains(rc));
  }

  @Test
  public void testContainsArrayContainer_EmptyContainsEmpty() {
    Container ac = new ArrayContainer();
    Container subset = new ArrayContainer();
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_IncludeProperSubset() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new ArrayContainer().add(0, 9);
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_IncludeProperSubsetDifferentStart() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new ArrayContainer().add(2, 9);
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeShiftedSet() {
    Container ac = new ArrayContainer().add(0, 10);
    Container shifted = new ArrayContainer().add(2, 12);
    assertFalse(ac.contains(shifted));
  }

  @Test
  public void testContainsArrayContainer_IncludeSelf() {
    Container ac = new ArrayContainer().add(0, 10);
    Container subset = new ArrayContainer().add(0, 10);
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeSuperSet() {
    Container ac = new ArrayContainer().add(0, 10);
    Container superset = new ArrayContainer().add(0, 20);
    assertFalse(ac.contains(superset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeDisJointSet() {
    Container ac = new ArrayContainer().add(0, 10);
    Container disjoint = new ArrayContainer().add(20, 40);
    assertFalse(ac.contains(disjoint));
    assertFalse(disjoint.contains(ac));
  }

  @Test
  public void iorNotIncreaseCapacity() {
    Container ac1 = new ArrayContainer();
    Container ac2 = new ArrayContainer();
    ac1.add((char) 128);
    ac1.add((char) 256);
    ac2.add((char) 1024);

    ac1.ior(ac2);
    assertTrue(ac1.contains((char) 128));
    assertTrue(ac1.contains((char) 256));
    assertTrue(ac1.contains((char) 1024));
  }

  @Test
  public void iorIncreaseCapacity() {
    Container ac1 = new ArrayContainer();
    Container ac2 = new ArrayContainer();
    ac1.add((char) 128);
    ac1.add((char) 256);
    ac1.add((char) 512);
    ac1.add((char) 513);
    ac2.add((char) 1024);

    ac1.ior(ac2);
    assertTrue(ac1.contains((char) 128));
    assertTrue(ac1.contains((char) 256));
    assertTrue(ac1.contains((char) 512));
    assertTrue(ac1.contains((char) 513));
    assertTrue(ac1.contains((char) 1024));
  }

  @Test
  public void iorSanityCheck() {
    Container ac = new ArrayContainer().add(0, 10);
    Container disjoint = new ArrayContainer().add(20, 40);
    ac.ior(disjoint);
    assertTrue(ac.contains(disjoint));
  }

  @Test
  public void iorNot() {
    Container rc1 = new ArrayContainer();
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
    Container rc1 = new ArrayContainer();
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
    Container rc1 = new ArrayContainer();
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
    Container rc1 = new ArrayContainer();
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
    final Container rc1 = new ArrayContainer();

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
    Container rc1 = new ArrayContainer();
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
  public void testIntersectsWithRange() {
    Container container = new ArrayContainer().add(0, 10);
    assertTrue(container.intersects(0, 1));
    assertTrue(container.intersects(0, 101));
    assertTrue(container.intersects(0, lower16Bits(-1)));
    assertFalse(container.intersects(11, lower16Bits(-1)));
  }

  @Test
  public void testIntersectsWithRange2() {
    Container container = new ArrayContainer().add(lower16Bits(-50), lower16Bits(-10));
    assertFalse(container.intersects(0, 1));
    assertTrue(container.intersects(0, lower16Bits(-40)));
    assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
    assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
    assertTrue(container.intersects(11, 1 << 16));
  }

  @Test
  public void testIntersectsWithRange3() {
    Container container = new ArrayContainer().add((char) 1).add((char) 300).add((char) 1024);
    assertTrue(container.intersects(0, 300));
    assertTrue(container.intersects(1, 300));
    assertFalse(container.intersects(2, 300));
    assertFalse(container.intersects(2, 299));
    assertTrue(container.intersects(0, lower16Bits(-1)));
    assertFalse(container.intersects(1025, 1 << 16));
  }

  @Test
  public void testContainsRange() {
    Container ac = new ArrayContainer().add(20, 100);
    assertFalse(ac.contains(1, 21));
    assertFalse(ac.contains(1, 19));
    assertTrue(ac.contains(20, 100));
    assertTrue(ac.contains(20, 99));
    assertTrue(ac.contains(21, 100));
    assertFalse(ac.contains(21, 101));
    assertFalse(ac.contains(19, 99));
    assertFalse(ac.contains(190, 9999));
  }

  @Test
  public void testContainsRange2() {
    Container ac = new ArrayContainer().add((char) 1).add((char) 10).add(20, 100);
    assertFalse(ac.contains(1, 21));
    assertFalse(ac.contains(1, 20));
    assertTrue(ac.contains(1, 2));
  }

  @Test
  public void testContainsRangeUnsigned() {
    Container ac = new ArrayContainer().add(1 << 15, 1 << 8 | 1 << 15);
    assertTrue(ac.contains(1 << 15, 1 << 8 | 1 << 15));
    assertTrue(ac.contains(1 + (1 << 15), (1 << 8 | 1 << 15) - 1));
    assertFalse(ac.contains(1 + (1 << 15), (1 << 8 | 1 << 15) + 1));
    assertFalse(ac.contains((1 << 15) - 1, (1 << 8 | 1 << 15) - 1));
    assertFalse(ac.contains(0, 1 << 15));
    assertFalse(ac.contains(1 << 8 | 1 << 15 | 1, 1 << 16));
  }

  @Test
  public void testNextValueBeforeStart() {
    ArrayContainer container = new ArrayContainer(new char[] {10, 20, 30});
    assertEquals(10, container.nextValue((char) 5));
  }

  @Test
  public void testNextValue() {
    ArrayContainer container = new ArrayContainer(new char[] {10, 20, 30});
    assertEquals(10, container.nextValue((char) 10));
    assertEquals(20, container.nextValue((char) 11));
    assertEquals(30, container.nextValue((char) 30));
  }

  @Test
  public void testNextValueAfterEnd() {
    ArrayContainer container = new ArrayContainer(new char[] {10, 20, 30});
    assertEquals(-1, container.nextValue((char) 31));
  }

  @Test
  public void testNextValue2() {
    Container container = new ArrayContainer().iadd(64, 129);
    assertTrue(container instanceof ArrayContainer);
    assertEquals(64, container.nextValue((char) 0));
    assertEquals(64, container.nextValue((char) 64));
    assertEquals(65, container.nextValue((char) 65));
    assertEquals(128, container.nextValue((char) 128));
    assertEquals(-1, container.nextValue((char) 129));
    assertEquals(-1, container.nextValue((char) 5000));
  }

  @Test
  public void testNextValueBetweenRuns() {
    Container container = new ArrayContainer().iadd(64, 129).iadd(256, 321);
    assertTrue(container instanceof ArrayContainer);
    assertEquals(64, container.nextValue((char) 0));
    assertEquals(64, container.nextValue((char) 64));
    assertEquals(65, container.nextValue((char) 65));
    assertEquals(128, container.nextValue((char) 128));
    assertEquals(256, container.nextValue((char) 129));
    assertEquals(-1, container.nextValue((char) 512));
  }

  @Test
  public void testNextValue3() {
    Container container = new ArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
    assertTrue(container instanceof ArrayContainer);
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
  public void testPreviousValue1() {
    Container container = new ArrayContainer().iadd(64, 129);
    assertTrue(container instanceof ArrayContainer);
    assertEquals(-1, container.previousValue((char) 0));
    assertEquals(-1, container.previousValue((char) 63));
    assertEquals(64, container.previousValue((char) 64));
    assertEquals(65, container.previousValue((char) 65));
    assertEquals(128, container.previousValue((char) 128));
    assertEquals(128, container.previousValue((char) 129));
  }

  @Test
  public void testPreviousValue2() {
    Container container = new ArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
    assertTrue(container instanceof ArrayContainer);
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
    ArrayContainer container = new ArrayContainer(new char[] {10, 20, 30});
    assertEquals(-1, container.previousValue((char) 5));
  }

  @Test
  public void testPreviousValueSparse() {
    ArrayContainer container = new ArrayContainer(new char[] {10, 20, 30});
    assertEquals(-1, container.previousValue((char) 9));
    assertEquals(10, container.previousValue((char) 10));
    assertEquals(10, container.previousValue((char) 11));
    assertEquals(20, container.previousValue((char) 21));
    assertEquals(30, container.previousValue((char) 30));
  }

  @Test
  public void testPreviousValueUnsigned() {
    ArrayContainer container =
        new ArrayContainer(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)});
    assertEquals(-1, container.previousValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 5), container.previousValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 5), container.previousValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 7), container.previousValue((char) ((1 << 15) | 7)));
    assertEquals(((1 << 15) | 7), container.previousValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testNextValueUnsigned() {
    ArrayContainer container =
        new ArrayContainer(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)});
    assertEquals(((1 << 15) | 5), container.nextValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 5), container.nextValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 7), container.nextValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 7), container.nextValue((char) ((1 << 15) | 7)));
    assertEquals(-1, container.nextValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testPreviousValueAfterEnd() {
    ArrayContainer container = new ArrayContainer(new char[] {10, 20, 30});
    assertEquals(30, container.previousValue((char) 31));
  }

  @Test
  public void testPreviousAbsentValue1() {
    Container container = new ArrayContainer().iadd(64, 129);
    assertEquals(0, container.previousAbsentValue((char) 0));
    assertEquals(63, container.previousAbsentValue((char) 63));
    assertEquals(63, container.previousAbsentValue((char) 64));
    assertEquals(63, container.previousAbsentValue((char) 65));
    assertEquals(63, container.previousAbsentValue((char) 128));
    assertEquals(129, container.previousAbsentValue((char) 129));
  }

  @Test
  public void testPreviousAbsentValue2() {
    Container container = new ArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
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
    ArrayContainer container = new ArrayContainer();
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, container.previousAbsentValue((char) i));
    }
  }

  @Test
  public void testPreviousAbsentValueSparse() {
    ArrayContainer container = new ArrayContainer(new char[] {10, 20, 30});
    assertEquals(9, container.previousAbsentValue((char) 9));
    assertEquals(9, container.previousAbsentValue((char) 10));
    assertEquals(11, container.previousAbsentValue((char) 11));
    assertEquals(21, container.previousAbsentValue((char) 21));
    assertEquals(29, container.previousAbsentValue((char) 30));
  }

  @Test
  public void testPreviousAbsentValueUnsigned() {
    ArrayContainer container =
        new ArrayContainer(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)});
    assertEquals(((1 << 15) | 4), container.previousAbsentValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 4), container.previousAbsentValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 6), container.previousAbsentValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 6), container.previousAbsentValue((char) ((1 << 15) | 7)));
    assertEquals(((1 << 15) | 8), container.previousAbsentValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testNextAbsentValue1() {
    Container container = new ArrayContainer().iadd(64, 129);
    assertEquals(0, container.nextAbsentValue((char) 0));
    assertEquals(63, container.nextAbsentValue((char) 63));
    assertEquals(129, container.nextAbsentValue((char) 64));
    assertEquals(129, container.nextAbsentValue((char) 65));
    assertEquals(129, container.nextAbsentValue((char) 128));
    assertEquals(129, container.nextAbsentValue((char) 129));
  }

  @Test
  public void testNextAbsentValue2() {
    Container container = new ArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
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
    ArrayContainer container = new ArrayContainer();
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, container.nextAbsentValue((char) i));
    }
  }

  @Test
  public void testNextAbsentValueSparse() {
    ArrayContainer container = new ArrayContainer(new char[] {10, 20, 30});
    assertEquals(9, container.nextAbsentValue((char) 9));
    assertEquals(11, container.nextAbsentValue((char) 10));
    assertEquals(11, container.nextAbsentValue((char) 11));
    assertEquals(21, container.nextAbsentValue((char) 21));
    assertEquals(31, container.nextAbsentValue((char) 30));
  }

  @Test
  public void testNextAbsentValueUnsigned() {
    ArrayContainer container =
        new ArrayContainer(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)});
    assertEquals(((1 << 15) | 4), container.nextAbsentValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 6), container.nextAbsentValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 6), container.nextAbsentValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 8), container.nextAbsentValue((char) ((1 << 15) | 7)));
    assertEquals(((1 << 15) | 8), container.nextAbsentValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testRangeConsumer() {
    char[] entries = new char[] {3, 4, 7, 8, 10, 65530, 65534, 65535};
    ArrayContainer container = new ArrayContainer(entries);

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

    container = new ArrayContainer();
    ValidationRangeConsumer consumer6 =
        ValidationRangeConsumer.ofSize(BitmapContainer.MAX_CAPACITY);
    container.forAll(0, consumer6);
    consumer6.assertAllAbsent();

    container = new ArrayContainer();
    Container c = container.iadd(0, ArrayContainer.DEFAULT_MAX_SIZE);
    assertTrue(container == c, "Container type changed!");
    ValidationRangeConsumer consumer7 =
        ValidationRangeConsumer.ofSize(ArrayContainer.DEFAULT_MAX_SIZE);
    container.forAllUntil(0, (char) ArrayContainer.DEFAULT_MAX_SIZE, consumer7);
    consumer7.assertAllPresent();
  }

  private static int lower16Bits(int x) {
    return ((char) x) & 0xFFFF;
  }
}
