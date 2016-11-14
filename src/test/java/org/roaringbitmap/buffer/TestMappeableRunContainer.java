package org.roaringbitmap.buffer;

import org.junit.Test;

import java.io.*;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.roaringbitmap.buffer.MappeableBitmapContainer.MAX_CAPACITY;
import static org.roaringbitmap.buffer.TestMappeableArrayContainer.newArrayContainer;


public class TestMappeableRunContainer {

  @Test
  public void constructorArray() {
    MappeableArrayContainer ac = newArrayContainer(1, 100);
    MappeableRunContainer rc = new MappeableRunContainer(ac, 1);
    assertEquals(99, rc.getCardinality());
    for (int i = 1; i < 100; i++) {
      assertTrue(rc.contains((short) i));
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
      assertTrue(rc.contains((short) i));
    }
  }

  @Test
  public void not() {
    MappeableRunContainer rc = newRunContainer(1, 13);
    MappeableContainer result = rc.not(5, 8);
    assertEquals(9, result.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(rc.contains((short) i));
    }
    for (int i = 8; i < 13; i++) {
      assertTrue(rc.contains((short) i));
    }
  }

  static MappeableRunContainer newRunContainer(int firstOfRun, final int lastOfRun) {
    ShortBuffer buffer = ShortBuffer.allocate(2);
    buffer.put((short) firstOfRun);
    buffer.put((short) (lastOfRun-firstOfRun-1));
    return new MappeableRunContainer(buffer.asReadOnlyBuffer(), 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void selectInvalidPosition() {
    MappeableContainer bc = new MappeableRunContainer();
    bc = bc.add(1,13);
    bc.select(100);
  }

  @Test
  public void trim() {
    MappeableContainer rc = new MappeableRunContainer(10);
    rc = rc.add(1, 5);
    rc.trim();
    assertEquals(4, rc.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(rc.contains((short) i));
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
      assertTrue(rc2.contains((short) i));
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
    assertThat(rc, instanceOf(MappeableRunContainer.class));
    MappeableContainer result = rc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertThat(result, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void orFullToRunContainer2() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes(0, 1 << 15);
    MappeableArrayContainer half = new MappeableArrayContainer(1 << 15, 1 << 16);
    assertThat(rc, instanceOf(MappeableRunContainer.class));
    MappeableContainer result = rc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertThat(result, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void orFullToRunContainer3() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes(0, 1 << 15);
    MappeableContainer half = MappeableContainer.rangeOfOnes(1 << 15, 1 << 16);
    assertThat(rc, instanceOf(MappeableRunContainer.class));
    MappeableContainer result = rc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertThat(result, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void testLazyORFull() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes(0, 1 << 15);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer(3210, 1 << 16);
    MappeableContainer rbc = rc.lazyOR(bc2);
    assertEquals(-1, rbc.getCardinality());
    MappeableContainer repaired = rbc.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertThat(repaired, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void testLazyORFull2() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes((1 << 10) - 200, 1 << 16);
    MappeableArrayContainer ac = new MappeableArrayContainer(0, 1 << 10);
    MappeableContainer rbc = rc.lazyOR(ac);
    assertEquals(1 << 16, rbc.getCardinality());
    assertThat(rbc, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void testLazyORFull3() {
    MappeableContainer rc = MappeableContainer.rangeOfOnes(0, 1 << 15);
    MappeableContainer rc2 = MappeableContainer.rangeOfOnes(1 << 15, 1 << 16);
    MappeableContainer result = rc.lazyOR(rc2);
    MappeableContainer iresult = rc.lazyIOR(rc2);
    assertEquals(1 << 16, result.getCardinality());
    assertEquals(1 << 16, iresult.getCardinality());
    assertThat(result, instanceOf(MappeableRunContainer.class));
    assertThat(iresult, instanceOf(MappeableRunContainer.class));
  }
}
