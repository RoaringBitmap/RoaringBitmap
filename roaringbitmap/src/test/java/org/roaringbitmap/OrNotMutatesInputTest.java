package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class OrNotMutatesInputTest {

  @Test
  public void staticOrNotMustNotModifyInputX1() {
    // Dense key 0 -> BitmapContainer whose ior() mutates in place.
    // rangeEnd 0x10000 makes key 0 the last in-range key (maxKey == 0),
    // present in x1 but absent in x2 => the ".ior()" branch is taken.
    final RoaringBitmap x1 = new RoaringBitmap();
    for (int i = 0; i < 5000; i++) {
      x1.add(i);
    }
    final RoaringBitmap x2 = new RoaringBitmap();
    final RoaringBitmap x1Before = x1.clone();

    RoaringBitmap.orNot(x1, x2, 0x10000L);

    assertEquals(x1Before, x1, "static orNot must not modify input x1");
  }
}
