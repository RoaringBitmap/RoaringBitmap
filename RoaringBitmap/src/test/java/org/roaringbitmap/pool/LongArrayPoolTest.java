package org.roaringbitmap.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

public class LongArrayPoolTest {

  @Test
  public void testTakeFree() {
    long[] array = LongArrayPool.INSTANCE.take(10);
    assertEquals(10, array.length);
    LongArrayPool.INSTANCE.free(array);

    long[] array2 = LongArrayPool.INSTANCE.take(5);
    assertEquals(5, array2.length);
    assertNotSame(array, array2);
    LongArrayPool.INSTANCE.free(array2);

    long[] array3 = LongArrayPool.INSTANCE.take(10);
    assertSame(array, array3);
    LongArrayPool.INSTANCE.free(array3);
  }

}
