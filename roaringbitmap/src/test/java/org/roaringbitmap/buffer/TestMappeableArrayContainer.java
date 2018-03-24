package org.roaringbitmap.buffer;

import org.junit.Test;
import org.roaringbitmap.IntConsumer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ShortBuffer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;


public class TestMappeableArrayContainer {

  @Test
  public void addEmptyRange() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.iadd(1,1);
    assertEquals(0, ac.getCardinality());
  }

  @Test(expected = IllegalArgumentException.class)
  public void addInvalidRange() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac.add(13,1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void iaddInvalidRange() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac.iadd(13,1);
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
    ac = ac.iadd(1,3);
    ac = ac.remove((short) 2);
    assertEquals(1, ac.getCardinality());
    assertTrue(ac.contains((short) 1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void removeInvalidRange() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac.remove(13,1);
  }

  @Test
  public void iremoveEmptyRange() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac.remove(1,1);
    assertEquals(0, ac.getCardinality());
  }

  @Test(expected = IllegalArgumentException.class)
  public void iremoveInvalidRange() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac.iremove(13,1);
  }

  @Test
  public void constructorWithRun() {
    MappeableContainer ac = new MappeableArrayContainer(1, 13);
    assertEquals(12, ac.getCardinality());
    for (int i = 1; i <= 12; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

  @Test
  public void add() {
    MappeableContainer ac = newArrayContainer(1, 2, 3, 5);
    ac = ac.add((short) 4);
    assertEquals(5, ac.getCardinality());
    for (int i = 1; i <= 5; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

  @Test
  public void add2() {
    MappeableContainer ac = newArrayContainer(1, 5000);
    ac = ac.add((short) 7000);
    assertEquals(5000, ac.getCardinality());
    for (int i = 1; i < 5000; i++) {
      assertTrue(ac.contains((short) i));
    }
    assertTrue(ac.contains((short) 7000));
  }

  @Test
  public void flip() {
    MappeableContainer ac = newArrayContainer(1, 2, 3, 5);
    ac = ac.flip((short) 4);
    assertEquals(5, ac.getCardinality());
    for (int i = 1; i <= 5; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

  @Test
  public void flip2() {
    MappeableContainer ac = newArrayContainer(1, 2, 3, 4, 5);
    ac = ac.flip((short) 5);
    assertEquals(4, ac.getCardinality());
    for (int i = 1; i <= 4; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

  @Test
  public void flip3() {
    MappeableContainer ac = newArrayContainer(1, 5000);
    ac = ac.flip((short) 7000);
    assertEquals(5000, ac.getCardinality());
    for (int i = 1; i < 5000; i++) {
      assertTrue(ac.contains((short) i));
    }
    assertTrue(ac.contains((short) 7000));
  }

  static MappeableArrayContainer newArrayContainer(int... values) {
    ShortBuffer buffer = ShortBuffer.allocate(values.length);
    for (int value : values) {
      buffer.put((short) value);
    }
    return new MappeableArrayContainer(buffer.asReadOnlyBuffer(), values.length);
  }

  static MappeableArrayContainer newArrayContainer(int firstOfRun, final int lastOfRun) {
    ShortBuffer buffer = ShortBuffer.allocate(lastOfRun - firstOfRun);
    for (int i = firstOfRun; i < lastOfRun; i++) {
      buffer.put((short) i);
    }
    return new MappeableArrayContainer(buffer.asReadOnlyBuffer(), lastOfRun - firstOfRun);
  }

  @Test
  public void iand() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.add(10,20);
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(15,25);
    ac.iand(bc);
    assertEquals(5, ac.getCardinality());
    for (int i = 15; i < 20; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

  @Test
  public void iandNotArray() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.add(10,20);
    MappeableContainer ac2 = newArrayContainer(15,25);
    ac.iandNot(ac2);
    assertEquals(5, ac.getCardinality());
    for (int i = 10; i < 15; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

  @Test
  public void iandNotBitmap() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.add(10,20);
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(15,25);
    ac.iandNot(bc);
    assertEquals(5, ac.getCardinality());
    for (int i = 10; i < 15; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

  @Test
  public void intersects() {
    MappeableContainer ac1 = new MappeableArrayContainer();
    ac1 = ac1.add(10,20);
    MappeableContainer ac2 = new MappeableArrayContainer();
    ac2 = ac2.add(15,25);
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
      assertTrue(ac2.contains((short) i));
    }
  }

  @Test
  public void orArray() {
    MappeableContainer ac = newArrayContainer(0,8192);
    MappeableContainer ac2 = newArrayContainer(15,25);
    ac = ac.or(ac2);
    assertEquals(8192, ac.getCardinality());
    for (int i = 0; i < 8192; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

  @Test
  public void xorArray() {
    MappeableContainer ac = newArrayContainer(0,8192);
    MappeableContainer ac2 = newArrayContainer(15,25);
    ac = ac.xor(ac2);
    assertEquals(8182, ac.getCardinality());
    for (int i = 0; i < 15; i++) {
      assertTrue(ac.contains((short) i));
    }
    for (int i = 25; i < 8192; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

  @Test
  public void foreach() {
    MappeableContainer ac = newArrayContainer(0, 64);
    ac.forEach((short) 0, new IntConsumer() {
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
    assertThat(result, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void orFullToRunContainer2() {
    MappeableArrayContainer ac = new MappeableArrayContainer(0, 1 << 15);
    MappeableArrayContainer half = new MappeableArrayContainer(1 << 15, 1 << 16);
    MappeableContainer result = ac.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertThat(result, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void testLazyORFull() {
    MappeableArrayContainer ac = new MappeableArrayContainer(0, 1 << 15);
    MappeableArrayContainer ac2 = new MappeableArrayContainer(1 << 15, 1 << 16);
    MappeableContainer rbc = ac.lazyor(ac2);
    assertEquals(-1, rbc.getCardinality());
    MappeableContainer repaired = rbc.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertThat(repaired, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void testToString() {
    MappeableArrayContainer ac1 = new MappeableArrayContainer(5, 15);
    ac1.add((short) -3);
    ac1.add((short) -17);
    assertEquals("{5,6,7,8,9,10,11,12,13,14,65519,65533}", ac1.toString());
  }
  
  @Test
  public void iorNotIncreaseCapacity() {
    MappeableArrayContainer ac1 = new MappeableArrayContainer();
    MappeableArrayContainer ac2 = new MappeableArrayContainer();
    ac1.add((short) 128);
    ac1.add((short) 256);
    ac2.add((short) 1024);
    
    ac1.ior(ac2);
    assertTrue(ac1.contains((short) 128));
    assertTrue(ac1.contains((short) 256));
    assertTrue(ac1.contains((short) 1024));
  }
  
  @Test
  public void iorIncreaseCapacity() {
    MappeableArrayContainer ac1 = new MappeableArrayContainer();
    MappeableArrayContainer ac2 = new MappeableArrayContainer();
    ac1.add((short) 128);
    ac1.add((short) 256);
    ac1.add((short) 512);
    ac1.add((short) 513);
    ac2.add((short) 1024);
    
    ac1.ior(ac2);
    assertTrue(ac1.contains((short) 128));
    assertTrue(ac1.contains((short) 256));
    assertTrue(ac1.contains((short) 512));
    assertTrue(ac1.contains((short) 513));
    assertTrue(ac1.contains((short) 1024));
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
    MappeableContainer container = new MappeableArrayContainer().add(lower16Bits(-50), lower16Bits(-10));
    assertFalse(container.intersects(0, 1));
    assertTrue(container.intersects(0, lower16Bits(-40)));
    assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
    assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
    assertTrue(container.intersects(11, 1 << 16));
  }


  @Test
  public void testIntersectsWithRange3() {
    MappeableContainer container = new MappeableArrayContainer()
            .add((short) 1)
            .add((short) 300)
            .add((short) 1024);
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
    MappeableContainer ac = new MappeableArrayContainer()
            .add((short)1).add((short)10)
            .add(20, 100);
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

  private static int lower16Bits(int x) {
    return ((short)x) & 0xFFFF;
  }
}
