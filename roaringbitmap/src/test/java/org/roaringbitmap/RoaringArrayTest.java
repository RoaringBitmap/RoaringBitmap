package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

public class RoaringArrayTest {

  @Test
  public void whenAppendEmpty_ShouldBeUnchanged() {
    RoaringArray array = new RoaringArray();
    array.keys = new char[2];
    array.values = new Container[2];
    array.size = 1;

    RoaringArray appendage = new RoaringArray();
    appendage.size = 0;
    appendage.keys = new char[4];
    appendage.values = new Container[4];

    array.append(appendage);
    assertEquals(1, array.size);
    assertEquals(2, array.keys.length);
  }

  @Test
  public void whenAppendToEmpty_ShouldEqualAppendage() {
    RoaringArray array = new RoaringArray();
    array.size = 0;
    array.keys = new char[4];
    array.values = new Container[4];

    RoaringArray appendage = new RoaringArray();
    appendage.size = 3;
    appendage.keys = new char[4];
    appendage.values = new Container[4];

    array.append(appendage);

    assertEquals(3, array.size);
    assertEquals(4, array.keys.length);
  }

  @Test
  public void whenAppendNonEmpty_SizeShouldEqualSumOfSizes() {
    RoaringArray array = new RoaringArray();
    array.size = 2;
    array.keys = new char[] {0, 2, 0, 0};
    array.values = new Container[4];

    RoaringArray appendage = new RoaringArray();
    appendage.size = 3;
    appendage.keys = new char[] {5, 6, 7, 0};
    appendage.values = new Container[4];

    array.append(appendage);

    assertEquals(5, array.size);
  }

  @Test
  public void whenAppendNonEmpty_ResultantKeysShouldBeMonotonic() {
    RoaringArray array = new RoaringArray();
    array.size = 2;
    array.keys = new char[] {0, 2, 0, 0};
    array.values = new Container[4];

    RoaringArray appendage = new RoaringArray();
    appendage.size = 3;
    appendage.keys = new char[] {5, 6, 7, 0};
    appendage.values = new Container[4];

    array.append(appendage);

    assertArrayEquals(new char[] {0, 2, 5, 6, 7}, array.keys);
  }

  @Test
  public void resizeOnlyIfNecessary() {
    char[] keys = new char[1];
    int size = 0;
    Container[] values = new Container[1];
    RoaringArray array = new RoaringArray(keys, values, size);
    array.extendArray(1);
    assertSame(keys, array.keys, "Keys were not reallocated");
  }
}
