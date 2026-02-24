package org.roaringbitmap.art;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LeafNodeTest {

  @Test
  public void testClone() {
    LeafNode leafOne = new LeafNode(new byte[] {1, 2, 3, 4, 5, 0}, 10);
    LeafNode cloned = leafOne.clone();

    Assertions.assertEquals(leafOne.toString(), cloned.toString());
  }
}
