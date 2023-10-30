package org.roaringbitmap.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

public class CharArrayPoolTest {

  @Test
  public void testTakeFree() {
    char[] array = CharArrayPool.INSTANCE.take(10);
    assertEquals(10, array.length);
    CharArrayPool.INSTANCE.free(array);

    char[] array2 = CharArrayPool.INSTANCE.take(5);
    assertEquals(5, array2.length);
    assertNotSame(array, array2);
    CharArrayPool.INSTANCE.free(array2);

    char[] array3 = CharArrayPool.INSTANCE.take(10);
    assertSame(array, array3);
    CharArrayPool.INSTANCE.free(array3);
  }

}
