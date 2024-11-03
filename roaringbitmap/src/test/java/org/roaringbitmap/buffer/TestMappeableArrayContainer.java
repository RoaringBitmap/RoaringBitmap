package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.roaringbitmap.IntConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.CharBuffer;
import java.util.Arrays;

@Execution(ExecutionMode.CONCURRENT)
public class TestMappeableArrayContainer {

  @Test
  public void addEmptyRange() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.iadd(1, 1);
    assertEquals(0, ac.getCardinality());
  }

  @Test
  public void addInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          MappeableContainer ac = new MappeableArrayContainer();
          ac.add(13, 1);
        });
  }

  @Test
  public void iaddInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          MappeableContainer ac = new MappeableArrayContainer();
          ac.iadd(13, 1);
        });
  }

  @Test
  public void iaddSanityTest() {
    MappeableContainer ac = new MappeableArrayContainer();
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
  public void remove() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.iadd(1, 3);
    ac = ac.remove((char) 2);
    assertEquals(1, ac.getCardinality());
    assertTrue(ac.contains((char) 1));
  }

  @Test
  public void removeInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          MappeableContainer ac = new MappeableArrayContainer();
          ac.remove(13, 1);
        });
  }

  @Test
  public void iremoveEmptyRange() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac.remove(1, 1);
    assertEquals(0, ac.getCardinality());
  }

  @Test
  public void iremoveInvalidRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          MappeableContainer ac = new MappeableArrayContainer();
          ac.iremove(13, 1);
        });
  }

  @Test
  public void constructorWithRun() {
    MappeableContainer ac = new MappeableArrayContainer(1, 13);
    assertEquals(12, ac.getCardinality());
    for (int i = 1; i <= 12; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void add() {
    MappeableContainer ac = newArrayContainer(1, 2, 3, 5);
    ac = ac.add((char) 4);
    assertEquals(5, ac.getCardinality());
    for (int i = 1; i <= 5; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void add2() {
    MappeableContainer ac = newArrayContainer(1, 5000);
    ac = ac.add((char) 7000);
    assertEquals(5000, ac.getCardinality());
    for (int i = 1; i < 5000; i++) {
      assertTrue(ac.contains((char) i));
    }
    assertTrue(ac.contains((char) 7000));
  }

  @Test
  public void flip() {
    MappeableContainer ac = newArrayContainer(1, 2, 3, 5);
    ac = ac.flip((char) 4);
    assertEquals(5, ac.getCardinality());
    for (int i = 1; i <= 5; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void flip2() {
    MappeableContainer ac = newArrayContainer(1, 2, 3, 4, 5);
    ac = ac.flip((char) 5);
    assertEquals(4, ac.getCardinality());
    for (int i = 1; i <= 4; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void flip3() {
    MappeableContainer ac = newArrayContainer(1, 5000);
    ac = ac.flip((char) 7000);
    assertEquals(5000, ac.getCardinality());
    for (int i = 1; i < 5000; i++) {
      assertTrue(ac.contains((char) i));
    }
    assertTrue(ac.contains((char) 7000));
  }

  static MappeableArrayContainer newArrayContainer(int... values) {
    CharBuffer buffer = CharBuffer.allocate(values.length);
    for (int value : values) {
      buffer.put((char) value);
    }
    return new MappeableArrayContainer(buffer.asReadOnlyBuffer(), values.length);
  }

  static MappeableArrayContainer newArrayContainer(int firstOfRun, final int lastOfRun) {
    CharBuffer buffer = CharBuffer.allocate(lastOfRun - firstOfRun);
    for (int i = firstOfRun; i < lastOfRun; i++) {
      buffer.put((char) i);
    }
    return new MappeableArrayContainer(buffer.asReadOnlyBuffer(), lastOfRun - firstOfRun);
  }

  @Test
  public void iand() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.add(10, 20);
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(15, 25);
    ac.iand(bc);
    assertEquals(5, ac.getCardinality());
    for (int i = 15; i < 20; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void iandNotArray() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.add(10, 20);
    MappeableContainer ac2 = newArrayContainer(15, 25);
    ac.iandNot(ac2);
    assertEquals(5, ac.getCardinality());
    for (int i = 10; i < 15; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void iandNotBitmap() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.add(10, 20);
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(15, 25);
    ac.iandNot(bc);
    assertEquals(5, ac.getCardinality());
    for (int i = 10; i < 15; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void intersects() {
    MappeableContainer ac1 = new MappeableArrayContainer();
    ac1 = ac1.add(10, 20);
    MappeableContainer ac2 = new MappeableArrayContainer();
    ac2 = ac2.add(15, 25);
    assertTrue(ac1.intersects(ac2));
  }

  @Test
  public void numberOfRuns() {
    MappeableContainer ac = newArrayContainer(1, 13);
    assertEquals(1, ac.numberOfRuns());
  }

  @Test
  public void roundtrip() throws Exception {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.add(1, 5);
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oo = new ObjectOutputStream(bos)) {
      ac.writeExternal(oo);
    }
    MappeableContainer ac2 = new MappeableArrayContainer();
    final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ac2.readExternal(new ObjectInputStream(bis));

    assertEquals(4, ac2.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(ac2.contains((char) i));
    }
  }

  @Test
  public void orArray() {
    MappeableContainer ac = newArrayContainer(0, 8192);
    MappeableContainer ac2 = newArrayContainer(15, 25);
    ac = ac.or(ac2);
    assertEquals(8192, ac.getCardinality());
    for (int i = 0; i < 8192; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void xorArray() {
    MappeableContainer ac = newArrayContainer(0, 8192);
    MappeableContainer ac2 = newArrayContainer(15, 25);
    ac = ac.xor(ac2);
    assertEquals(8182, ac.getCardinality());
    for (int i = 0; i < 15; i++) {
      assertTrue(ac.contains((char) i));
    }
    for (int i = 25; i < 8192; i++) {
      assertTrue(ac.contains((char) i));
    }
  }

  @Test
  public void foreach() {
    MappeableContainer ac = newArrayContainer(0, 64);
    ac.forEach(
        (char) 0,
        new IntConsumer() {
          int expected = 0;

          @Override
          public void accept(int value) {
            assertEquals(value, expected++);
          }
        });
  }

  @Test
  public void orFullToRunContainer() {
    MappeableArrayContainer ac = new MappeableArrayContainer(0, 1 << 12);
    MappeableBitmapContainer half = new MappeableBitmapContainer(1 << 12, 1 << 16);
    MappeableContainer result = ac.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof MappeableRunContainer);
  }

  @Test
  public void orFullToRunContainer2() {
    MappeableArrayContainer ac = new MappeableArrayContainer(0, 1 << 15);
    MappeableArrayContainer half = new MappeableArrayContainer(1 << 15, 1 << 16);
    MappeableContainer result = ac.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof MappeableRunContainer);
  }

  @Test
  public void testLazyORFull() {
    MappeableArrayContainer ac = new MappeableArrayContainer(0, 1 << 15);
    MappeableArrayContainer ac2 = new MappeableArrayContainer(1 << 15, 1 << 16);
    MappeableContainer rbc = ac.lazyor(ac2);
    assertEquals(-1, rbc.getCardinality());
    MappeableContainer repaired = rbc.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertTrue(repaired instanceof MappeableRunContainer);
  }

  @Test
  public void isNotFull() {
    assertFalse(new MappeableArrayContainer().add('a').isFull());
  }

  @Test
  public void testToString() {
    MappeableArrayContainer ac1 = new MappeableArrayContainer(5, 15);
    ac1.add((char) -3);
    ac1.add((char) -17);
    assertEquals("{5,6,7,8,9,10,11,12,13,14,65519,65533}", ac1.toString());
  }

  @Test
  public void testContainsRunContainer_Issue723Case1() {
    MappeableContainer ac = new MappeableArrayContainer().add(0, 10);
    MappeableContainer subset = new MappeableRunContainer().add(5, 6);
    assertTrue(ac.contains(subset));
  }

  @Test
  public void testContainsRunContainer_Issue723Case2() {
    MappeableContainer ac = new MappeableArrayContainer().add(0, 10);
    MappeableContainer rc = new MappeableRunContainer().add(5, 11);
    assertFalse(ac.contains(rc));
  }

  @Test
  public void iorNotIncreaseCapacity() {
    MappeableArrayContainer ac1 = new MappeableArrayContainer();
    MappeableArrayContainer ac2 = new MappeableArrayContainer();
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
    MappeableArrayContainer ac1 = new MappeableArrayContainer();
    MappeableArrayContainer ac2 = new MappeableArrayContainer();
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
    MappeableContainer ac = new MappeableArrayContainer().add(0, 10);
    MappeableContainer disjoint = new MappeableArrayContainer().add(20, 40);
    ac.ior(disjoint);
    assertTrue(ac.contains(disjoint));
  }

  @Test
  public void testIntersectsWithRange() {
    MappeableContainer container = new MappeableArrayContainer().add(0, 10);
    assertTrue(container.intersects(0, 1));
    assertTrue(container.intersects(0, 101));
    assertTrue(container.intersects(0, lower16Bits(-1)));
    assertFalse(container.intersects(11, lower16Bits(-1)));
  }

  @Test
  public void testIntersectsWithRange2() {
    MappeableContainer container =
        new MappeableArrayContainer().add(lower16Bits(-50), lower16Bits(-10));
    assertFalse(container.intersects(0, 1));
    assertTrue(container.intersects(0, lower16Bits(-40)));
    assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
    assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
    assertTrue(container.intersects(11, 1 << 16));
  }

  @Test
  public void testIntersectsWithRange3() {
    MappeableContainer container =
        new MappeableArrayContainer().add((char) 1).add((char) 300).add((char) 1024);
    assertTrue(container.intersects(0, 300));
    assertTrue(container.intersects(1, 300));
    assertFalse(container.intersects(2, 300));
    assertFalse(container.intersects(2, 299));
    assertTrue(container.intersects(0, lower16Bits(-1)));
    assertFalse(container.intersects(1025, 1 << 16));
  }

  @Test
  public void testContainsRange() {
    MappeableContainer ac = new MappeableArrayContainer().add(20, 100);
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
    MappeableContainer ac = new MappeableArrayContainer().add((char) 1).add((char) 10).add(20, 100);
    assertFalse(ac.contains(1, 21));
    assertFalse(ac.contains(1, 20));
    assertTrue(ac.contains(1, 2));
  }

  @Test
  public void testContainsRangeUnsigned() {
    MappeableContainer ac = new MappeableArrayContainer().add(1 << 15, 1 << 8 | 1 << 15);
    assertTrue(ac.contains(1 << 15, 1 << 8 | 1 << 15));
    assertTrue(ac.contains(1 + (1 << 15), (1 << 8 | 1 << 15) - 1));
    assertFalse(ac.contains(1 + (1 << 15), (1 << 8 | 1 << 15) + 1));
    assertFalse(ac.contains((1 << 15) - 1, (1 << 8 | 1 << 15) - 1));
    assertFalse(ac.contains(0, 1 << 15));
    assertFalse(ac.contains(1 << 8 | 1 << 15 | 1, 1 << 16));
  }

  @Test
  public void testNextValueBeforeStart() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(CharBuffer.wrap(new char[] {10, 20, 30}), 3);
    assertEquals(10, container.nextValue((char) 5));
  }

  @Test
  public void testNextValue() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(CharBuffer.wrap(new char[] {10, 20, 30}), 3);
    assertEquals(10, container.nextValue((char) 10));
    assertEquals(20, container.nextValue((char) 11));
    assertEquals(30, container.nextValue((char) 30));
  }

  @Test
  public void testNextValueAfterEnd() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(CharBuffer.wrap(new char[] {10, 20, 30}), 3);
    assertEquals(-1, container.nextValue((char) 31));
  }

  @Test
  public void testNextValue2() {
    MappeableContainer container = new MappeableArrayContainer().iadd(64, 129);
    assertTrue(container instanceof MappeableArrayContainer);
    assertEquals(64, container.nextValue((char) 0));
    assertEquals(64, container.nextValue((char) 64));
    assertEquals(65, container.nextValue((char) 65));
    assertEquals(128, container.nextValue((char) 128));
    assertEquals(-1, container.nextValue((char) 129));
    assertEquals(-1, container.nextValue((char) 5000));
  }

  @Test
  public void testNextValueBetweenRuns() {
    MappeableContainer container = new MappeableArrayContainer().iadd(64, 129).iadd(256, 321);
    assertTrue(container instanceof MappeableArrayContainer);
    assertEquals(64, container.nextValue((char) 0));
    assertEquals(64, container.nextValue((char) 64));
    assertEquals(65, container.nextValue((char) 65));
    assertEquals(128, container.nextValue((char) 128));
    assertEquals(256, container.nextValue((char) 129));
    assertEquals(-1, container.nextValue((char) 512));
  }

  @Test
  public void testNextValue3() {
    MappeableContainer container =
        new MappeableArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
    assertTrue(container instanceof MappeableArrayContainer);
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
    MappeableContainer container = new MappeableArrayContainer().iadd(64, 129);
    assertTrue(container instanceof MappeableArrayContainer);
    assertEquals(-1, container.previousValue((char) 0));
    assertEquals(-1, container.previousValue((char) 63));
    assertEquals(64, container.previousValue((char) 64));
    assertEquals(65, container.previousValue((char) 65));
    assertEquals(128, container.previousValue((char) 128));
    assertEquals(128, container.previousValue((char) 129));
  }

  @Test
  public void testPreviousValue2() {
    MappeableContainer container =
        new MappeableArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
    assertTrue(container instanceof MappeableArrayContainer);
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
    MappeableContainer container =
        new MappeableArrayContainer(CharBuffer.wrap(new char[] {10, 20, 30}), 3);
    assertEquals(-1, container.previousValue((char) 5));
  }

  @Test
  public void testPreviousValueSparse() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(CharBuffer.wrap(new char[] {10, 20, 30}), 3);
    assertEquals(-1, container.previousValue((char) 9));
    assertEquals(10, container.previousValue((char) 10));
    assertEquals(10, container.previousValue((char) 11));
    assertEquals(20, container.previousValue((char) 21));
    assertEquals(30, container.previousValue((char) 30));
  }

  @Test
  public void testPreviousValueUnsigned() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(
            CharBuffer.wrap(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)}), 2);
    assertEquals(-1, container.previousValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 5), container.previousValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 5), container.previousValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 7), container.previousValue((char) ((1 << 15) | 7)));
    assertEquals(((1 << 15) | 7), container.previousValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testNextValueUnsigned() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(
            CharBuffer.wrap(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)}), 2);
    assertEquals(((1 << 15) | 5), container.nextValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 5), container.nextValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 7), container.nextValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 7), container.nextValue((char) ((1 << 15) | 7)));
    assertEquals(-1, container.nextValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testPreviousValueAfterEnd() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(CharBuffer.wrap(new char[] {10, 20, 30}), 3);
    assertEquals(30, container.previousValue((char) 31));
  }

  @Test
  public void testPreviousAbsentValue1() {
    MappeableContainer container = new MappeableArrayContainer().iadd(64, 129);
    assertEquals(0, container.previousAbsentValue((char) 0));
    assertEquals(63, container.previousAbsentValue((char) 63));
    assertEquals(63, container.previousAbsentValue((char) 64));
    assertEquals(63, container.previousAbsentValue((char) 65));
    assertEquals(63, container.previousAbsentValue((char) 128));
    assertEquals(129, container.previousAbsentValue((char) 129));
  }

  @Test
  public void testPreviousAbsentValue2() {
    MappeableContainer container =
        new MappeableArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
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
    MappeableArrayContainer container = new MappeableArrayContainer();
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, container.previousAbsentValue((char) i));
    }
  }

  @Test
  public void testPreviousAbsentValueSparse() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(CharBuffer.wrap(new char[] {10, 20, 30}), 3);
    assertEquals(9, container.previousAbsentValue((char) 9));
    assertEquals(9, container.previousAbsentValue((char) 10));
    assertEquals(11, container.previousAbsentValue((char) 11));
    assertEquals(21, container.previousAbsentValue((char) 21));
    assertEquals(29, container.previousAbsentValue((char) 30));
  }

  @Test
  public void testPreviousAbsentValueUnsigned() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(
            CharBuffer.wrap(new char[] {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)}), 2);
    assertEquals(((1 << 15) | 4), container.previousAbsentValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 4), container.previousAbsentValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 6), container.previousAbsentValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 6), container.previousAbsentValue((char) ((1 << 15) | 7)));
    assertEquals(((1 << 15) | 8), container.previousAbsentValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testNextAbsentValue1() {
    MappeableContainer container = new MappeableArrayContainer().iadd(64, 129);
    assertEquals(0, container.nextAbsentValue((char) 0));
    assertEquals(63, container.nextAbsentValue((char) 63));
    assertEquals(129, container.nextAbsentValue((char) 64));
    assertEquals(129, container.nextAbsentValue((char) 65));
    assertEquals(129, container.nextAbsentValue((char) 128));
    assertEquals(129, container.nextAbsentValue((char) 129));
  }

  @Test
  public void testNextAbsentValue2() {
    MappeableContainer container =
        new MappeableArrayContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
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
    MappeableArrayContainer container = new MappeableArrayContainer();
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, container.nextAbsentValue((char) i));
    }
  }

  @Test
  public void testNextAbsentValueSparse() {
    MappeableArrayContainer container =
        new MappeableArrayContainer(CharBuffer.wrap(new char[] {10, 20, 30}), 3);
    assertEquals(9, container.nextAbsentValue((char) 9));
    assertEquals(11, container.nextAbsentValue((char) 10));
    assertEquals(11, container.nextAbsentValue((char) 11));
    assertEquals(21, container.nextAbsentValue((char) 21));
    assertEquals(31, container.nextAbsentValue((char) 30));
  }

  @Test
  public void testNextAbsentValueUnsigned() {
    char[] array = {(char) ((1 << 15) | 5), (char) ((1 << 15) | 7)};
    MappeableArrayContainer container = new MappeableArrayContainer(CharBuffer.wrap(array), 2);
    assertEquals(((1 << 15) | 4), container.nextAbsentValue((char) ((1 << 15) | 4)));
    assertEquals(((1 << 15) | 6), container.nextAbsentValue((char) ((1 << 15) | 5)));
    assertEquals(((1 << 15) | 6), container.nextAbsentValue((char) ((1 << 15) | 6)));
    assertEquals(((1 << 15) | 8), container.nextAbsentValue((char) ((1 << 15) | 7)));
    assertEquals(((1 << 15) | 8), container.nextAbsentValue((char) ((1 << 15) | 8)));
  }

  @Test
  public void testAndInto() {
    long[] bits = new long[1024];
    Arrays.fill(bits, 0xAAAAAAAAAAAAAAAAL);
    MappeableArrayContainer container = new MappeableArrayContainer();
    for (int i = 1; i < 64; i += 2) {
      container.add((char) i);
    }
    container.andInto(bits);
    assertEquals(0xAAAAAAAAAAAAAAAAL, bits[0]);
    container = new MappeableArrayContainer();
    for (int i = 0; i < 64; i += 2) {
      container.add((char) i);
    }
    container.andInto(bits);
    assertEquals(0L, bits[0]);
  }

  @Test
  public void testOrInto() {
    long[] bits = new long[1024];
    Arrays.fill(bits, 0xAAAAAAAAAAAAAAAAL);
    MappeableArrayContainer container = new MappeableArrayContainer();
    for (int i = 1; i < 64; i += 2) {
      container.add((char) i);
    }
    container.orInto(bits);
    assertEquals(0xAAAAAAAAAAAAAAAAL, bits[0]);
    container = new MappeableArrayContainer();
    for (int i = 0; i < 64; i += 2) {
      container.add((char) i);
    }
    container.orInto(bits);
    assertEquals(-1L, bits[0]);
  }

  private static int lower16Bits(int x) {
    return ((char) x);
  }
}
