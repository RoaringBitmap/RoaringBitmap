package org.roaringbitmap.buffer;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MutableRoaringArrayTest {


  @Test
  public void resizeOnlyIfNecessary() {
    char[] keys = new char[1];
    int size = 0;
    MappeableContainer[] values = new MappeableContainer[1];
    MutableRoaringArray array = new MutableRoaringArray(keys, values, size);
    array.extendArray(1);
    assertTrue("Keys were not reallocated", keys == array.keys);
  }
}
