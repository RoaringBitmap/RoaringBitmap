package org.roaringbitmap.buffer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;


public class MutableRoaringArrayTest {


  @Test
  public void resizeOnlyIfNecessary() {
    char[] keys = new char[1];
    int size = 0;
    MappeableContainer[] values = new MappeableContainer[1];
    MutableRoaringArray array = new MutableRoaringArray(keys, values, size);
    array.extendArray(1);
    assertSame(keys, array.keys, "Keys were not reallocated");
  }
}
