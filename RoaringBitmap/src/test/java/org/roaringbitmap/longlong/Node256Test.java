package org.roaringbitmap.longlong;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.art.LeafNode;
import org.roaringbitmap.art.Node;
import org.roaringbitmap.art.Node256;
import org.roaringbitmap.art.Node48;

public class Node256Test {

  @Test
  public void test() {
    Node256 node256 = new Node256(0);
    LeafNode leafNode = new LeafNode(0, 0);
    for (int i = 0; i < 256; i++) {
      node256 = Node256.insert(node256, leafNode, (byte) i);
    }
    int minPos = node256.getMinPos();
    Assertions.assertEquals(0, minPos);
    int currentPos = minPos;
    for (int i = 1; i < 256; i++) {
      int nextLargerPos = node256.getNextLargerPos(currentPos);
      Assertions.assertEquals(i, nextLargerPos);
      currentPos = nextLargerPos;
    }
    int maxPos = node256.getMaxPos();
    Assertions.assertEquals(255, maxPos);
    currentPos = maxPos;
    for (int i = 254; i >= 0; i--) {
      int nextSmallerPos = node256.getNextSmallerPos(currentPos);
      Assertions.assertEquals(i, nextSmallerPos);
      currentPos = nextSmallerPos;
    }
    node256 = (Node256) node256.remove(120);
    int pos119 = node256.getChildPos((byte) 119);
    Assertions.assertEquals((byte) 119, node256.getChildKey(pos119));
    Assertions.assertEquals(119, pos119);
    int pos121 = node256.getNextLargerPos(pos119);
    Assertions.assertEquals(121, pos121);
    int nextPos119 = node256.getNextSmallerPos(pos121);
    Assertions.assertEquals(119, nextPos119);
  }

  @Test
  public void testShrinkToNode48() {
    Node256 node256 = new Node256(0);
    LeafNode leafNode = new LeafNode(0, 0);
    for (int i = 0; i < 37; i++) {
      node256 = Node256.insert(node256, leafNode, (byte) i);
    }
    int maxPos = node256.getMaxPos();
    Node node = node256.remove(maxPos);
    Assertions.assertTrue(node instanceof Node48);
    Node48 node48 = (Node48) node;
    LeafNode minLeafNode48 = (LeafNode) node48.getChild(node48.getMinPos());
    LeafNode minLeafNode256 = (LeafNode) node256.getChild(node256.getMinPos());
    Assertions.assertEquals(minLeafNode256, minLeafNode48);
  }
}
