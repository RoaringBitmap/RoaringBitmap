package org.roaringbitmap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;



public class CheckedRemoveTest {

  @Test
  void testCheckedRemove() {
    RoaringBitmap bitmap = new RoaringBitmap();
    // We add enough values so that the container becomes a
    // bitmap container.
    for(int i = 0; i < 10000; i++) {
      bitmap.add(i * 2);
    }
    // Next we remove them one by one.
    // At some point, the container should become an array container.
    for(int i = 0; i < 10000; i++) {
      bitmap.checkedRemove(i * 2);
      assertTrue(bitmap.validate());
    }
  }

}
