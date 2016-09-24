package org.roaringbitmap.buffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestMappeableArrayContainer {

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
    for (int i = 1; i <= 12 ; i++) {
      assertTrue(ac.contains((short) i));
    }
  }

}
