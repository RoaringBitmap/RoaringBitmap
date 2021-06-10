package org.roaringbitmap.art;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class Node48Test {

  @Test
  public void test() throws IOException {
    Node48 node48 = new Node48(0);
    LeafNode leafNode;
    for (int i = 0; i < 48; i++) {
      leafNode = new LeafNode(i, i);
      node48 = (Node48) Node48.insert(node48, leafNode, (byte) i);
    }
    int minPos = node48.getMinPos();
    Assertions.assertEquals(0, minPos);
    Assertions.assertEquals(0, ((LeafNode) node48.getChild(minPos)).getContainerIdx());
    int currentPos = minPos;
    for (int i = 1; i < 48; i++) {
      int nextPos = node48.getNextLargerPos(currentPos);
      Assertions.assertEquals(i, nextPos);
      LeafNode leafNode1 = (LeafNode) node48.getChild(nextPos);
      Assertions.assertEquals(i, leafNode1.getContainerIdx());
      byte key = (byte) i;
      int childPos = node48.getChildPos(key);
      Assertions.assertEquals(i, childPos);
      Assertions.assertEquals(key, node48.getChildKey(childPos));
      currentPos = nextPos;
    }
    int maxPos = node48.getMaxPos();
    Assertions.assertEquals(47, maxPos);
    currentPos = maxPos;
    for (int i = 46; i >= 0; i--) {
      int pos = node48.getNextSmallerPos(currentPos);
      Assertions.assertEquals(i, pos);
      currentPos = pos;
    }
    int sizeInBytes = node48.serializeSizeInBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    node48.serialize(dataOutputStream);
    Assertions.assertEquals(sizeInBytes, byteArrayOutputStream.toByteArray().length);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    Node48 deserNode48 = (Node48) Node.deserialize(dataInputStream);
    currentPos = maxPos;
    for (int i = 46; i >= 0; i--) {
      int pos = deserNode48.getNextSmallerPos(currentPos);
      Assertions.assertEquals(i, pos);
      currentPos = pos;
    }

    node48 = (Node48) node48.remove(minPos);
    int newMinPos = node48.getMinPos();
    Assertions.assertEquals(1, newMinPos);
  }

  @Test
  public void testWithOffsetBeforeBytes() {
    Node48 nodes = new Node48(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 48;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = (Node48) Node48.insert(nodes, leafNode, (byte) (offset + i));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node48);

    // this is bad test, because it's checking the internal implementation which really
    // should not matter, because the small nodes do not use this gappy method, therefore
    // to rely on it is really fragile, thus it should not really be tested. But here we are.
    Assertions.assertEquals(offset, nodes.getMinPos());

    // The position of a value before the "first" value dose not exist thus ILLEGAL_IDX
    Assertions.assertEquals(Node.ILLEGAL_IDX, nodes.getNextSmallerPos(nodes.getMinPos()));

    // The position of a value after the "last" value dose not exist thus ILLEGAL_IDX
    Assertions.assertEquals(Node.ILLEGAL_IDX, nodes.getNextLargerPos(nodes.getMaxPos()));

    // so for each value in the inserted range the next of the prior should be the same as
    // the location of found current.
    int currentPos = nodes.getMinPos();
    for (int i = 1; i < (insertCount - 1); i++) {
      int nextPos = nodes.getNextLargerPos(currentPos);
      Assertions.assertEquals(nodes.getChildPos((byte) (i + offset)), nextPos);
      currentPos = nextPos;
    }

    // so for each value in the inserted range the next of the prior should be the same as
    // the location of found current.
    currentPos = nodes.getMaxPos();
    for (int i = (insertCount - 2); i > 0; i--) {
      int nextPos = nodes.getNextSmallerPos(currentPos);
      Assertions.assertEquals(nodes.getChildPos((byte) (i + offset)), nextPos);
      currentPos = nextPos;
    }
  }

  @Test
  public void testWithOffsetAndGapsBytes() {
    Node48 nodes = new Node48(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 48;
    int step = 2;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = (Node48) Node48.insert(nodes, leafNode, (byte) (offset + (i*step)));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node48);

    // this is bad test, because it's checking the internal implementation which really
    // should not matter, because the small nodes do not use this gap method, therefore
    // to rely on it is really fragile, thus it should not really be tested. But here we are.
    Assertions.assertEquals(offset, nodes.getMinPos());

    // The position of a value before the "first" value dose not exist thus ILLEGAL_IDX
    Assertions.assertEquals(Node.ILLEGAL_IDX, nodes.getNextSmallerPos(nodes.getMinPos()));

    // The position of a value after the "last" value dose not exist thus ILLEGAL_IDX
    Assertions.assertEquals(Node.ILLEGAL_IDX, nodes.getNextLargerPos(nodes.getMaxPos()));

    // so for each value in the inserted range the next of the prior should be the same as
    // the location of found current.
    int currentPos = nodes.getMinPos();
    for (int i = 1; i < (insertCount - 1); i++) {
      int nextPos = nodes.getNextLargerPos(currentPos);
      int valKey = offset + i * step;
      Assertions.assertEquals(nodes.getChildPos((byte) valKey), nextPos);
      currentPos = nextPos;
    }

    // so for each value in the inserted range the next of the prior should be the same as
    // the location of found current.
    currentPos = nodes.getMaxPos();
    for (int i = (insertCount - 2); i > 0; i--) {
      int nextPos = nodes.getNextSmallerPos(currentPos);
      int valKey = offset + i * step;
      Assertions.assertEquals(nodes.getChildPos((byte) valKey), nextPos);
      currentPos = nextPos;
    }
  }

  @Test
  public void testGrowToNode256() {
    Node48 node48 = new Node48(0);
    LeafNode leafNode;
    for (int i = 0; i < 48; i++) {
      leafNode = new LeafNode(i, i);
      node48 = (Node48) Node48.insert(node48, leafNode, (byte) i);
    }
    int key48 = 48;
    leafNode = new LeafNode(key48, key48);
    Node node = Node48.insert(node48, leafNode, (byte) key48);
    Assertions.assertTrue(node instanceof Node256);
    Node256 node256 = (Node256) node;
    int pos48 = node256.getChildPos((byte) key48);
    Assertions.assertEquals(48, pos48);
    Assertions.assertEquals(48, node256.getMaxPos());
    Assertions.assertEquals(47, node256.getNextSmallerPos(48));
  }

  @Test
  public void testShrinkToNode16() {
    Node48 node48 = new Node48(0);
    LeafNode leafNode;
    for (int i = 0; i < 13; i++) {
      leafNode = new LeafNode(i, i);
      node48 = (Node48) Node48.insert(node48, leafNode, (byte) i);
    }
    int maxPos = node48.getMaxPos();
    Assertions.assertEquals(12, maxPos);
    Node node = node48.remove(maxPos);
    Assertions.assertTrue(node instanceof Node16);
    Node16 node16 = (Node16) node;
    int pos = node16.getChildPos((byte) 0);
    Assertions.assertEquals(0, pos);
    pos = node16.getChildPos((byte) 12);
    Assertions.assertEquals(Node.ILLEGAL_IDX, pos);
    pos = node16.getChildPos((byte) 11);
    Assertions.assertEquals(11, pos);
  }

  @Test
  public void testNegative() {
    Node48 node48 = new Node48(0);
    LeafNode leafNode;

    for (int i = 0; i < 48; i++) {
      int byteKey = -128 + i;
      leafNode = new LeafNode(i, i);
      node48 = (Node48) Node48.insert(node48, leafNode, (byte) byteKey);
    }
    int minPos = node48.getMinPos();
    Assertions.assertEquals(128, minPos);
    Assertions.assertEquals(0, ((LeafNode) node48.getChild(minPos)).getContainerIdx());
    int currentPos = minPos;
    for (int i = 1; i < 48; i++) {
      int nextPos = node48.getNextLargerPos(currentPos);
      Assertions.assertEquals(128 + i, nextPos);
      LeafNode leafNode1 = (LeafNode) node48.getChild(nextPos);
      Assertions.assertEquals(i, leafNode1.getContainerIdx());
      int byteKey = -128 + i;
      int childPos = node48.getChildPos((byte) byteKey);
      Assertions.assertEquals(128 + i, childPos);
      currentPos = nextPos;
    }
    int maxPos = node48.getMaxPos();
    Assertions.assertEquals(175, maxPos);
    currentPos = maxPos;
    for (int i = 46; i >= 0; i--) {
      int pos = node48.getNextSmallerPos(currentPos);
      maxPos--;
      Assertions.assertEquals(maxPos, pos);
      currentPos = pos;
    }
    node48 = (Node48) node48.remove(minPos);
    int newMinPos = node48.getMinPos();
    Assertions.assertEquals(129, newMinPos);
  }
}
