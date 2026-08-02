package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ReverseIntIteratorFlyweightManyContainersTest {

  @Test
  public void reverseIterationYieldsEveryValueWithMoreThanShortMaxContainers() {
    // One value per distinct high-16-bit key => one container per value, so the
    // container count lands just past the largest index a signed short can hold.
    final int containerCount = Short.MAX_VALUE + 2; // 32769
    final int[] data = new int[containerCount];
    for (int i = 0; i < containerCount; i++) {
      data[i] = i << 16; // low 16 bits zero, high 16 bits = i => a distinct container each
    }

    final RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
    assertEquals(containerCount, bitmap.getCardinality());

    final ReverseIntIteratorFlyweight it = new ReverseIntIteratorFlyweight(bitmap);

    assertTrue(it.hasNext());
    assertEquals((containerCount - 1) << 16, it.next()); // highest value first

    int seen = 1;
    while (it.hasNext()) {
      it.next();
      seen++;
    }
    assertEquals(containerCount, seen);
  }
}
