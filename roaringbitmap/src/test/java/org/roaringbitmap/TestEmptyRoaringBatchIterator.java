package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import org.junit.jupiter.api.Test;

public class TestEmptyRoaringBatchIterator {

  @Test
  public void testEmptyMutableRoaringBitmap() {
    MutableRoaringBitmap mutableRoaringBitmap = new MutableRoaringBitmap();
    BatchIterator iterator = mutableRoaringBitmap.getBatchIterator();
    int[] ints = new int[1024];
    int cnt = iterator.nextBatch(ints);
    assertEquals(0, cnt);

    mutableRoaringBitmap.add(1);
    iterator = mutableRoaringBitmap.getBatchIterator();
    cnt = iterator.nextBatch(ints);
    assertEquals(1, cnt);
  }

  @Test
  public void testEmptyImmutableRoaringBitmap() {
    MutableRoaringBitmap mutableRoaringBitmap = new MutableRoaringBitmap();
    ImmutableRoaringBitmap immutableRoaringBitmap = mutableRoaringBitmap.toImmutableRoaringBitmap();
    BatchIterator iterator = immutableRoaringBitmap.getBatchIterator();
    int[] ints = new int[1024];
    int cnt = iterator.nextBatch(ints);
    assertEquals(0, cnt);

    mutableRoaringBitmap.add(1);
    iterator = mutableRoaringBitmap.toImmutableRoaringBitmap().getBatchIterator();
    cnt = iterator.nextBatch(ints);
    assertEquals(1, cnt);
  }

  @Test
  public void testEmptyRoaringBitmap() {
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    BatchIterator iterator = roaringBitmap.getBatchIterator();
    int[] ints = new int[1024];
    int cnt = iterator.nextBatch(ints);
    assertEquals(0, cnt);

    roaringBitmap.add(1);
    iterator = roaringBitmap.getBatchIterator();
    cnt = iterator.nextBatch(ints);
    assertEquals(1, cnt);
  }
}
