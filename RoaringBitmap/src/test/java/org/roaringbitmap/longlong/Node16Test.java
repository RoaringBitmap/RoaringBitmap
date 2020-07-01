package org.roaringbitmap.longlong;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.art.LeafNode;
import org.roaringbitmap.art.Node;
import org.roaringbitmap.art.Node16;
import org.roaringbitmap.art.Node4;
import org.roaringbitmap.art.Node48;

public class Node16Test {

  @Test
  public void test() {
    Node4 node4 = new Node4(0);
    //insert 4 nodes
    for (int i = 0; i < 4; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      node4 = (Node4) Node4.insert(node4, leafNode, (byte) i);
    }
    //insert the fifth node
    LeafNode leafNode4 = new LeafNode(4, 4);
    Node16 node16 = (Node16) Node4.insert(node4, leafNode4, (byte) 4);
    //remove two nodes to shrink to node4
    node16 = (Node16) node16.remove(4);
    Node degenerativeNode = node16.remove(3);
    Assertions.assertTrue(degenerativeNode instanceof Node4);
    //recover to node16 by re-insert two nodes
    Node4 degenerativeNode4 = (Node4) degenerativeNode;
    Node node = Node4.insert(degenerativeNode4, leafNode4, (byte) 4);
    LeafNode leafNode3 = new LeafNode(3, 3);
    node16 = (Node16) Node4.insert(node, leafNode3, (byte) 3);

    byte key = 4;
    Assertions.assertTrue(node16.getChildPos(key) == 4);
    for (int i = 5; i < 12; i++) {
      byte key1 = (byte) i;
      LeafNode leafNode = new LeafNode(i, i);
      node16 = (Node16) Node16.insert(node16, leafNode, key1);
      Assertions.assertEquals(i, node16.getChildPos(key1));
    }
    LeafNode leafNode = new LeafNode(12, 12);
    key = (byte) -2;
    node16 = (Node16) Node16.insert(node16, leafNode, key);
    Assertions.assertEquals(12, node16.getChildPos(key));
    leafNode = new LeafNode(13, 13);
    byte key12 = (byte) 12;
    node16 = (Node16) Node16.insert(node16, leafNode, key12);
    Assertions.assertEquals(12, node16.getChildPos(key12));
    Assertions.assertEquals(13, node16.getChildPos(key));
  }

  @Test
  public void testGrowToNode48() {
    Node16 node16 = new Node16(0);
    LeafNode leafNode;
    for (int i = 0; i < 16; i++) {
      leafNode = new LeafNode(i, i);
      node16 = (Node16) Node16.insert(node16, leafNode, (byte) i);
    }
    leafNode = new LeafNode(16, 16);
    Node node = Node16.insert(node16, leafNode, (byte) 16);
    Assertions.assertTrue(node instanceof Node48);
    Node48 node48 = (Node48) node;
    int maxPos = node48.getMaxPos();
    Assertions.assertEquals(16, maxPos);
    int pos = node48.getChildPos((byte) 16);
    Assertions.assertEquals(maxPos, pos);
  }

  @Test
  public void testVisit() {
    Node16 node16 = new Node16(0);
    LeafNode leafNode;
    for (int i = 0; i < 15; i++) {
      leafNode = new LeafNode(i, i);
      node16 = (Node16) Node16.insert(node16, leafNode, (byte) i);
    }
    Assertions.assertEquals(0, node16.getMinPos());
    Assertions.assertEquals(14, node16.getMaxPos());
    int i = 0;
    for (i = 0; i < 14; i++) {
      int pos = node16.getNextLargerPos(i);
      LeafNode leafNode1 = (LeafNode) node16.getChild(pos);
      Assertions.assertEquals(i + 1, leafNode1.getContainerIdx());
    }
    i = 14;
    for (; i >= 1; i--) {
      int pos = node16.getNextSmallerPos(i);
      LeafNode leafNode1 = (LeafNode) node16.getChild(pos);
      Assertions.assertEquals(i - 1, leafNode1.getContainerIdx());
    }
    Assertions.assertEquals(-1, node16.getNextSmallerPos(i));
  }
}
