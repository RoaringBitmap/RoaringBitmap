package org.roaringbitmap;

import org.junit.Test;

import static org.junit.Assert.*;

public class RoaringArrayTest {


  @Test
  public void whenAppendEmpty_ShouldBeUnchanged() {
    RoaringArray array = new RoaringArray();
    array.keys = new short[2];
    array.values = new Container[2];
    array.size = 1;

    RoaringArray appendage = new RoaringArray();
    appendage.size = 0;
    appendage.keys = new short[4];
    appendage.values = new Container[4];

    array.append(appendage);
    assertEquals(1, array.size);
    assertEquals(2, array.keys.length);
  }

  @Test
  public void whenAppendToEmpty_ShouldEqualAppendage() {
    RoaringArray array = new RoaringArray();
    array.size = 0;
    array.keys = new short[4];
    array.values = new Container[4];

    RoaringArray appendage = new RoaringArray();
    appendage.size = 3;
    appendage.keys = new short[4];
    appendage.values = new Container[4];

    array.append(appendage);

    assertEquals(3, array.size);
    assertEquals(4, array.keys.length);
  }

  @Test
  public void whenAppendNonEmpty_SizeShouldEqualSumOfSizes() {
    RoaringArray array = new RoaringArray();
    array.size = 2;
    array.keys = new short[]{0, 2, 0, 0};
    array.values = new Container[4];

    RoaringArray appendage = new RoaringArray();
    appendage.size = 3;
    appendage.keys = new short[]{5, 6, 7, 0};
    appendage.values = new Container[4];

    array.append(appendage);

    assertEquals(5, array.size);
  }


  @Test
  public void whenAppendNonEmpty_ResultantKeysShouldBeMonotonic() {
    RoaringArray array = new RoaringArray();
    array.size = 2;
    array.keys = new short[]{0, 2, 0, 0};
    array.values = new Container[4];

    RoaringArray appendage = new RoaringArray();
    appendage.size = 3;
    appendage.keys = new short[]{5, 6, 7, 0};
    appendage.values = new Container[4];

    array.append(appendage);

    assertArrayEquals(new short[] {0, 2, 5, 6, 7}, array.keys);
  }

}