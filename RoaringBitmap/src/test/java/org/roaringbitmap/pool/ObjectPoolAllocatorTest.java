package org.roaringbitmap.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ObjectPoolAllocatorTest {

  @Test
  public void testAllocateLongs() {
    long[] longs = ObjectPoolAllocator.INSTANCE.allocateLongs(30);
    assertEquals(30, longs.length);
    ObjectPoolAllocator.INSTANCE.free(longs);
  }

}
