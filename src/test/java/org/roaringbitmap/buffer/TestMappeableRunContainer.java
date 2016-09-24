package org.roaringbitmap.buffer;

import org.junit.Test;

import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
    MappeableBitmapContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 1);
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

}
