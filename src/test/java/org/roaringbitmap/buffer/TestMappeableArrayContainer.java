package org.roaringbitmap.buffer;

import org.junit.Test;

import java.nio.ShortBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


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

  @Test
  public void remove() {
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.iadd(1,3);
    ac = ac.remove((short) 2);
    assertEquals(1, ac.getCardinality());
    assertTrue(ac.contains((short) 1));
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

  private MappeableContainer newArrayContainer(int... values) {
    ShortBuffer buffer = ShortBuffer.allocate(values.length);
    for (int value : values) {
      buffer.put((short) value);
    }
    return new MappeableArrayContainer(buffer.asReadOnlyBuffer(), values.length);
  }

  private MappeableContainer newArrayContainer(int firstOfRun, final int lastOfRun) {
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
  public void iandNot() {
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

}
