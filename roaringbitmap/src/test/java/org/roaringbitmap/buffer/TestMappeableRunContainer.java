package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.buffer.MappeableBitmapContainer.MAX_CAPACITY;
import static org.roaringbitmap.buffer.TestMappeableArrayContainer.newArrayContainer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

@Execution(ExecutionMode.CONCURRENT)
public class TestMappeableRunContainer {

  protected static MappeableRunContainer generateContainer(char[] values, int numOfRuns) {
    CharBuffer array = ByteBuffer.allocateDirect(values.length * 2).asCharBuffer();
    for (char v : values) {
      array.put(v);
    }
    return new MappeableRunContainer(array, numOfRuns);
  }

  @Test
  public void constructorArray() {
    MappeableArrayContainer ac = newArrayContainer(1, 100);
    MappeableRunContainer rc = new MappeableRunContainer(ac, 1);
    assertEquals(99, rc.getCardinality());
    for (int i = 1; i < 100; i++) {
      assertTrue(rc.contains((char) i));
    }
  }

  @Test
  public void constructorBitmap() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~0L);
    MappeableBitmapContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    MappeableRunContainer rc = new MappeableRunContainer(bc, 1);
    assertEquals(64, rc.getCardinality());
    for (int i = 0; i < 64; i++) {
      assertTrue(rc.contains((char) i));
    }
  }

  @Test
  public void not() {
    MappeableRunContainer rc = newRunContainer(1, 13);
    MappeableContainer result = rc.not(5, 8);
    assertEquals(9, result.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(rc.contains((char) i));
    }
    for (int i = 8; i < 13; i++) {
      assertTrue(rc.contains((char) i));
    }
  }

  static MappeableRunContainer newRunContainer(int firstOfRun, final int lastOfRun) {
    CharBuffer buffer = CharBuffer.allocate(2);
    buffer.put((char) firstOfRun);
    buffer.put((char) (lastOfRun - firstOfRun - 1));
    return new MappeableRunContainer(buffer.asReadOnlyBuffer(), 1);
  }

  @Test
  public void selectInvalidPosition() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          MappeableContainer bc = new MappeableRunContainer();
          bc = bc.add(1, 13);
          bc.select(100);
        });
  }

  @Test
  public void trim() {
    MappeableContainer rc = new MappeableRunContainer(10);
    rc = rc.add(1, 5);
    rc.trim();
    assertEquals(4, rc.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(rc.contains((char) i));
    }
  }

  @Test
  public void roundtrip() throws Exception {
    MappeableContainer rc = new MappeableRunContainer();
    rc = rc.add(1, 5);
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oo = new ObjectOutputStream(bos)) {
      rc.writeExternal(oo);
    }
    MappeableContainer rc2 = new MappeableRunContainer();
    final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    rc2.readExternal(new ObjectInputStream(bis));

    assertEquals(4, rc2.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(rc2.contains((char) i));
    }
  }

  @Test
  public void andCardinalityArray() {
    MappeableRunContainer rc = new MappeableRunContainer();
    MappeableArrayContainer ac = new MappeableArrayContainer();
    ac.iadd(15, 25);
    assertEquals(0, rc.andCardinality(ac));
    rc.iadd(10, 20);
    assertEquals(5, rc.andCardinality(ac));
    rc.iadd(24, 30);
    assertEquals(6, rc.andCardinality(ac));
  }

  @Test
  public void andCardinalityBitmap() {
    MappeableRunContainer rc = new MappeableRunContainer();
    MappeableBitmapContainer ac = new MappeableBitmapContainer();
    ac.iadd(15, 25);
    assertEquals(0, rc.andCardinality(ac));
    rc.iadd(10, 20);
    assertEquals(5, rc.andCardinality(ac));
    rc.iadd(24, 30);
    assertEquals(6, rc.andCardinality(ac));
  }

  @Test
  public void andCardinalityRun() {
    MappeableRunContainer rc1 = new MappeableRunContainer();
    MappeableRunContainer rc2 = new MappeableRunContainer();
    rc2.iadd(15, 25);
    assertEquals(0, rc1.andCardinality(rc2));
    assertEquals(0, rc2.andCardinality(rc1));
    rc1.iadd(10, 20);
    assertEquals(5, rc1.andCardinality(rc2));
    assertEquals(5, rc2.andCardinality(rc1));
    rc1.iadd(24, 30);
    assertEquals(6, rc1.andCardinality(rc2));
    assertEquals(6, rc2.andCardinality(rc1));
    rc1.iadd(55, 66);
    rc2.iadd(100, 110);
    assertEquals(6, rc1.andCardinality(rc2));
    assertEquals(6, rc2.andCardinality(rc1));
    rc1.iadd(100, 110);
    rc1.iadd(120, 130);
    rc2.iadd(130, 140);
    assertEquals(16, rc1.andCardinality(rc2));
    assertEquals(16, rc2.andCardinality(rc1));
  }

  @Test
  public void orFullToRunContainer() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes(0, 1 << 15);
    MappeableBitmapContainer half = new MappeableBitmapContainer(1 << 15, 1 << 16);
    assertTrue(rc instanceof MappeableRunContainer);
    MappeableContainer result = rc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof MappeableRunContainer);
  }

  @Test
  public void orFullToRunContainer2() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes(0, 1 << 15);
    MappeableArrayContainer half = new MappeableArrayContainer(1 << 15, 1 << 16);
    assertTrue(rc instanceof MappeableRunContainer);
    MappeableContainer result = rc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof MappeableRunContainer);
  }

  @Test
  public void orFullToRunContainer3() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes(0, 1 << 15);
    MappeableContainer half = MappeableContainer.rangeOfOnes(1 << 15, 1 << 16);
    assertTrue(rc instanceof MappeableRunContainer);
    MappeableContainer result = rc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof MappeableRunContainer);
  }

  @Test
  public void testLazyORFull() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes(0, 1 << 15);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer(3210, 1 << 16);
    MappeableContainer rbc = rc.lazyOR(bc2);
    assertEquals(-1, rbc.getCardinality());
    MappeableContainer repaired = rbc.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertTrue(repaired instanceof MappeableRunContainer);
  }

  @Test
  public void testLazyORFull2() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes((1 << 10) - 200, 1 << 16);
    MappeableArrayContainer ac = new MappeableArrayContainer(0, 1 << 10);
    MappeableContainer rbc = rc.lazyOR(ac);
    assertEquals(1 << 16, rbc.getCardinality());
    assertTrue(rbc instanceof MappeableRunContainer);
  }

  @Test
  public void testLazyORFull3() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes(0, 1 << 15);
    MappeableContainer rc2 = MappeableContainer.rangeOfOnes(1 << 15, 1 << 16);
    MappeableContainer result = rc.lazyOR(rc2);
    MappeableContainer iresult = rc.lazyIOR(rc2);
    assertEquals(1 << 16, result.getCardinality());
    assertEquals(1 << 16, iresult.getCardinality());
    assertTrue(result instanceof MappeableRunContainer);
    assertTrue(iresult instanceof MappeableRunContainer);
  }

  @Test
  public void testRangeCardinality() {
    MappeableBitmapContainer bc =
        TestMappeableBitmapContainer.generateContainer((char) 100, (char) 10000, 5);
    MappeableRunContainer rc = generateContainer(new char[] {7, 300, 400, 900, 1400, 2200}, 3);
    MappeableContainer result = rc.or(bc);
    assertEquals(8677, result.getCardinality());
  }

  @Test
  public void testRangeCardinality2() {
    MappeableBitmapContainer bc =
        TestMappeableBitmapContainer.generateContainer((char) 100, (char) 10000, 5);
    bc.add((char) 22345); // important case to have greater element than run container
    bc.add((char) Short.MAX_VALUE);
    MappeableRunContainer rc = generateContainer(new char[] {7, 300, 400, 900, 1400, 18000}, 3);
    assertTrue(rc.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE);
    MappeableContainer result = rc.andNot(bc);
    assertEquals(11437, result.getCardinality());
  }

  @Test
  public void testRangeCardinality3() {
    MappeableBitmapContainer bc =
        TestMappeableBitmapContainer.generateContainer((char) 100, (char) 10000, 5);
    MappeableRunContainer rc = generateContainer(new char[] {7, 300, 400, 900, 1400, 5200}, 3);
    MappeableBitmapContainer result = (MappeableBitmapContainer) rc.and(bc);
    assertEquals(5046, result.getCardinality());
  }

  @Test
  public void testRangeCardinality4() {
    MappeableBitmapContainer bc =
        TestMappeableBitmapContainer.generateContainer((char) 100, (char) 10000, 5);
    MappeableRunContainer rc = generateContainer(new char[] {7, 300, 400, 900, 1400, 2200}, 3);
    MappeableBitmapContainer result = (MappeableBitmapContainer) rc.xor(bc);
    assertEquals(6031, result.getCardinality());
  }

  @Test
  public void testEqualsArrayContainer_Equal() {
    MappeableContainer rc = new MappeableRunContainer().add(0, 10);
    MappeableContainer ac = new MappeableArrayContainer().add(0, 10);
    assertTrue(rc.equals(ac));
    assertTrue(ac.equals(rc));
  }

  @Test
  public void testEqualsArrayContainer_NotEqual_ArrayLarger() {
    MappeableContainer rc = new MappeableRunContainer().add(0, 10);
    MappeableContainer ac = new MappeableArrayContainer().add(0, 11);
    assertFalse(rc.equals(ac));
    assertFalse(ac.equals(rc));
  }

  @Test
  public void testEqualsArrayContainer_NotEqual_ArraySmaller() {
    MappeableContainer rc = new MappeableRunContainer().add(0, 10);
    MappeableContainer ac = new MappeableArrayContainer().add(0, 9);
    assertFalse(rc.equals(ac));
    assertFalse(ac.equals(rc));
  }

  @Test
  public void testEqualsArrayContainer_NotEqual_ArrayShifted() {
    MappeableContainer rc = new MappeableRunContainer().add(0, 10);
    MappeableContainer ac = new MappeableArrayContainer().add(1, 11);
    assertFalse(rc.equals(ac));
    assertFalse(ac.equals(rc));
  }

  @Test
  public void testEqualsArrayContainer_NotEqual_ArrayDiscontiguous() {
    MappeableContainer rc = new MappeableRunContainer().add(0, 10);
    MappeableContainer ac = new MappeableArrayContainer().add(0, 11);
    ac.flip((char) 9);
    assertFalse(rc.equals(ac));
    assertFalse(ac.equals(rc));
  }

  @Test
  public void testAndInto() {
    long[] bits = new long[1024];
    Arrays.fill(bits, 0xAAAAAAAAAAAAAAAAL);
    MappeableRunContainer container = new MappeableRunContainer();
    for (int i = 1; i < 64; i += 2) {
      container.add((char) i);
    }
    container.andInto(bits);
    assertEquals(0xAAAAAAAAAAAAAAAAL, bits[0]);
    container = new MappeableRunContainer();
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
    MappeableRunContainer container = new MappeableRunContainer();
    for (int i = 1; i < 64; i += 2) {
      container.add((char) i);
    }
    container.orInto(bits);
    assertEquals(0xAAAAAAAAAAAAAAAAL, bits[0]);
    container = new MappeableRunContainer();
    for (int i = 0; i < 64; i += 2) {
      container.add((char) i);
    }
    container.orInto(bits);
    assertEquals(-1L, bits[0]);
  }
}
