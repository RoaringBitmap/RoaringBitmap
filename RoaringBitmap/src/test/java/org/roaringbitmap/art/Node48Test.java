package org.roaringbitmap.art;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    assertEquals(0, minPos);
    assertEquals(0, ((LeafNode) node48.getChild(minPos)).getContainerIdx());
    int currentPos = minPos;
    for (int i = 1; i < 48; i++) {
      int nextPos = node48.getNextLargerPos(currentPos);
      assertEquals(i, nextPos);
      LeafNode leafNode1 = (LeafNode) node48.getChild(nextPos);
      assertEquals(i, leafNode1.getContainerIdx());
      byte key = (byte) i;
      int childPos = node48.getChildPos(key);
      assertEquals(i, childPos);
      assertEquals(key, node48.getChildKey(childPos));
      currentPos = nextPos;
    }
    int maxPos = node48.getMaxPos();
    assertEquals(47, maxPos);
    currentPos = maxPos;
    for (int i = 46; i >= 0; i--) {
      int pos = node48.getNextSmallerPos(currentPos);
      assertEquals(i, pos);
      currentPos = pos;
    }
    int sizeInBytes = node48.serializeSizeInBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    node48.serialize(dataOutputStream);
    assertEquals(sizeInBytes, byteArrayOutputStream.toByteArray().length);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    Node48 deserNode48 = (Node48) Node.deserialize(dataInputStream);
    currentPos = maxPos;
    for (int i = 46; i >= 0; i--) {
      int pos = deserNode48.getNextSmallerPos(currentPos);
      assertEquals(i, pos);
      currentPos = pos;
    }

    node48 = (Node48) node48.remove(minPos);
    int newMinPos = node48.getMinPos();
    assertEquals(1, newMinPos);
  }

  @Test
  public void testWithOffsetBeforeBytes() {
    Node nodes = new Node48(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 48;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = Node48.insert(nodes, leafNode, (byte) (offset + i));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node48);

    // this is bad test, because it's checking the internal implementation which really
    // should not matter, because the small nodes do not use this gappy method, therefore
    // to rely on it is really fragile, thus it should not really be tested. But here we are.
    assertEquals(offset, nodes.getMinPos());

    // The position of a value before the "first" value dose not exist thus ILLEGAL_IDX
    assertEquals(Node.ILLEGAL_IDX, nodes.getNextSmallerPos(nodes.getMinPos()));

    // The position of a value after the "last" value dose not exist thus ILLEGAL_IDX
    assertEquals(Node.ILLEGAL_IDX, nodes.getNextLargerPos(nodes.getMaxPos()));

    // so for each value in the inserted range the next of the prior should be the same as
    // the location of found current.
    int currentPos = nodes.getMinPos();
    for (int i = 1; i < (insertCount - 1); i++) {
      int nextPos = nodes.getNextLargerPos(currentPos);
      assertEquals(nodes.getChildPos((byte) (i + offset)), nextPos);
      currentPos = nextPos;
    }

    // so for each value in the inserted range the next of the prior should be the same as
    // the location of found current.
    currentPos = nodes.getMaxPos();
    for (int i = (insertCount - 2); i > 0; i--) {
      int nextPos = nodes.getNextSmallerPos(currentPos);
      assertEquals(nodes.getChildPos((byte) (i + offset)), nextPos);
      currentPos = nextPos;
    }
  }

  @Test
  public void testWithOffsetAndGapsBytes() {
    Node nodes = new Node48(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 48;
    int step = 2;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = Node48.insert(nodes, leafNode, (byte) (offset + (i*step)));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node48);

    // this is bad test, because it's checking the internal implementation which really
    // should not matter, because the small nodes do not use this gap method, therefore
    // to rely on it is really fragile, thus it should not really be tested. But here we are.
    assertEquals(offset, nodes.getMinPos());

    // The position of a value before the "first" value dose not exist thus ILLEGAL_IDX
    assertEquals(Node.ILLEGAL_IDX, nodes.getNextSmallerPos(nodes.getMinPos()));

    // The position of a value after the "last" value dose not exist thus ILLEGAL_IDX
    assertEquals(Node.ILLEGAL_IDX, nodes.getNextLargerPos(nodes.getMaxPos()));

    // so for each value in the inserted range the next of the prior should be the same as
    // the location of found current.
    int currentPos = nodes.getMinPos();
    for (int i = 1; i < (insertCount - 1); i++) {
      int nextPos = nodes.getNextLargerPos(currentPos);
      int valKey = offset + i * step;
      assertEquals(nodes.getChildPos((byte) valKey), nextPos);
      currentPos = nextPos;
    }

    // so for each value in the inserted range the next of the prior should be the same as
    // the location of found current.
    currentPos = nodes.getMaxPos();
    for (int i = (insertCount - 2); i > 0; i--) {
      int nextPos = nodes.getNextSmallerPos(currentPos);
      int valKey = offset + i * step;
      assertEquals(nodes.getChildPos((byte) valKey), nextPos);
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
    assertEquals(48, pos48);
    assertEquals(48, node256.getMaxPos());
    assertEquals(47, node256.getNextSmallerPos(48));
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
    assertEquals(12, maxPos);
    Node node = node48.remove(maxPos);
    Assertions.assertTrue(node instanceof Node16);
    Node16 node16 = (Node16) node;
    int pos = node16.getChildPos((byte) 0);
    assertEquals(0, pos);
    pos = node16.getChildPos((byte) 12);
    assertEquals(Node.ILLEGAL_IDX, pos);
    pos = node16.getChildPos((byte) 11);
    assertEquals(11, pos);
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
    assertEquals(128, minPos);
    assertEquals(0, ((LeafNode) node48.getChild(minPos)).getContainerIdx());
    int currentPos = minPos;
    for (int i = 1; i < 48; i++) {
      int nextPos = node48.getNextLargerPos(currentPos);
      assertEquals(128 + i, nextPos);
      LeafNode leafNode1 = (LeafNode) node48.getChild(nextPos);
      assertEquals(i, leafNode1.getContainerIdx());
      int byteKey = -128 + i;
      int childPos = node48.getChildPos((byte) byteKey);
      assertEquals(128 + i, childPos);
      currentPos = nextPos;
    }
    int maxPos = node48.getMaxPos();
    assertEquals(175, maxPos);
    currentPos = maxPos;
    for (int i = 46; i >= 0; i--) {
      int pos = node48.getNextSmallerPos(currentPos);
      maxPos--;
      assertEquals(maxPos, pos);
      currentPos = pos;
    }
    node48 = (Node48) node48.remove(minPos);
    int newMinPos = node48.getMinPos();
    assertEquals(129, newMinPos);
  }

  @Test
  public void testDenseNonZeroBasedKeysSearch() {
    Node nodes = new Node48(0);
    final int insertCount = 47;
    final int keyOffset = 0x20;

    // create the data
    for (int i = 0; i < insertCount; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      byte key = (byte) (i + keyOffset);
      nodes = Node48.insert(nodes, leafNode, key);
    }
    // check we are testing the correct thing
    Assertions.assertTrue(nodes instanceof Node48);

    // check that searching for each key, is FOUND
    for (int i = 0; i < insertCount; i++) {
      byte key = (byte) (i + keyOffset);
      SearchResult sr = nodes.getNearestChildPos(key);

      assertEquals(SearchResult.Outcome.FOUND, sr.outcome);
      Assertions.assertTrue(sr.hasKeyPos());
      assertEquals(key, nodes.getChildKey(sr.getKeyPos()));
    }

    // search before the first value "keyOffset", and surprise, nothing will be found
    SearchResult sr = nodes.getNearestChildPos((byte) (keyOffset - 1));
    assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    assertEquals(Node.ILLEGAL_IDX, sr.getNextSmallerPos());

    // search after the last value aka "insertCount", and surprise, nothing will be found
    sr = nodes.getNearestChildPos((byte) (keyOffset + insertCount));
    assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    assertEquals(Node.ILLEGAL_IDX, sr.getNextLargerPos());
  }

  @Test
  public void testSparseNonZeroBasedKeysSearch() {
    Node nodes = new Node48(0);
    final int insertCount = 47;
    final int lastValue = insertCount - 1;
    final int step = 3;
    final int keyOffset = 0x20;

    // create the data
    for (int i = 0; i < insertCount; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      byte key = (byte) ((i * step) + keyOffset);
      nodes = Node48.insert(nodes, leafNode, key);
    }
    // check we are testing the correct thing
    Assertions.assertTrue(nodes instanceof Node48);

    // check that searching for each key, is FOUND
    for (int i = 0; i < insertCount; i++) {
      byte key = (byte) ((i * step) + keyOffset);
      SearchResult sr = nodes.getNearestChildPos(key);

      assertEquals(SearchResult.Outcome.FOUND, sr.outcome);
      Assertions.assertTrue(sr.hasKeyPos());
      int keyPos = sr.getKeyPos();
      assertEquals(key, nodes.getChildKey(sr.getKeyPos()));

      // search in the "gaps" before the key
      {
        byte bKey = (byte) (key - 1);
        sr = nodes.getNearestChildPos(bKey);
        assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
        Assertions.assertFalse(sr.hasKeyPos());

        // the value smaller than the first should be INVALID, and the rest should be the prior key
        if (i == 0) {
          assertEquals(Node.ILLEGAL_IDX, sr.getNextSmallerPos());
        } else {
          int expect = Byte.toUnsignedInt(key) - step;
          int result = Byte.toUnsignedInt(nodes.getChildKey(sr.getNextSmallerPos()));
          assertEquals(expect, result);
        }
        // the NextLarger of the "key-1" should be the key
        assertEquals(keyPos, sr.getNextLargerPos());
        assertEquals(key, nodes.getChildKey(sr.getNextLargerPos()));
      }

      // search in the "gaps" after the key
      {
        byte aKey = (byte) (key + 1);

        sr = nodes.getNearestChildPos(aKey);
        assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
        Assertions.assertFalse(sr.hasKeyPos());

        // the next smaller pos than "key+1" should always be key
        assertEquals(keyPos, sr.getNextSmallerPos());
        assertEquals(key, nodes.getChildKey(sr.getNextSmallerPos()));

        // the value larger than the last should be INVALID and the rest should be the next key
        if (i == lastValue) {
          assertEquals(Node.ILLEGAL_IDX, sr.getNextLargerPos());
        } else {
          int expected = Byte.toUnsignedInt(key) + step;
          int result = Byte.toUnsignedInt(nodes.getChildKey(sr.getNextLargerPos()));
          assertEquals(expected, result);
        }
      }
    }

    // search before the first value "keyOffset", and surprise, nothing will be found
    SearchResult sr = nodes.getNearestChildPos((byte) (keyOffset - 1));
    assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    assertEquals(Node.ILLEGAL_IDX, sr.getNextSmallerPos());

    // search after the last value aka "insertCount", and surprise, nothing will be found
    sr = nodes.getNearestChildPos((byte) (keyOffset + (insertCount * step)));
    assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    assertEquals(Node.ILLEGAL_IDX, sr.getNextLargerPos());
  }

  @Test
  public void testGetNextSmallerPosEdgeCase() {
    Node nodes = new Node48(0);
    LeafNode leafNode = new LeafNode(0,0);

    nodes = Node48.insert(nodes, leafNode, (byte)67);
    // check we are testing the correct thing
    Assertions.assertTrue(nodes instanceof Node48);

    assertEquals(Node.ILLEGAL_IDX, nodes.getNextSmallerPos(66));
    Assertions.assertNotEquals(Node.ILLEGAL_IDX, nodes.getNextSmallerPos(74));
    assertEquals(67, nodes.getChildKey(nodes.getNextSmallerPos(74)));

    assertEquals(Node.ILLEGAL_IDX, nodes.getNextLargerPos(68));
    Assertions.assertNotEquals(Node.ILLEGAL_IDX, nodes.getNextLargerPos(60));
    assertEquals(67, nodes.getChildKey(nodes.getNextLargerPos(60)));
  }

  @Test
  public void testGetNextPosShouldNotThrowOnLegalInputs() {
    Node node = new Node48(0);
    for (int key = 0; key < 256; key++) {
      assertEquals(Node.ILLEGAL_IDX, node.getNextSmallerPos(key));
      assertEquals(Node.ILLEGAL_IDX, node.getNextLargerPos(key));
    }
  }

  @Test
  public void testSetOneByte() {
    long[] longs = new long[Node48.LONGS_USED];

    Node48.setOneByte(0,  (byte)0x67, longs);
    assertEquals(0x6700_0000_0000_0000L, longs[0]);
    Node48.setOneByte(1,  (byte)0x23, longs);
    assertEquals(0x6723_0000_0000_0000L, longs[0]);
    Node48.setOneByte(2,  (byte)0x14, longs);
    assertEquals(0x6723_1400_0000_0000L, longs[0]);
    Node48.setOneByte(3,  (byte)0x98, longs);
    assertEquals(0x6723_1498_0000_0000L, longs[0]);

    Node48.setOneByte(249,  (byte)0x67, longs);
    assertEquals(0x0067_0000_0000_0000L, longs[31]);
    Node48.setOneByte(250,  (byte)0x23, longs);
    assertEquals(0x0067_2300_0000_0000L, longs[31]);
    Node48.setOneByte(251,  (byte)0x14, longs);
    assertEquals(0x0067_2314_0000_0000L, longs[31]);
    Node48.setOneByte(252,  (byte)0x98, longs);
    assertEquals(0x0067_2314_9800_0000L, longs[31]);
  }

  @Test
  public void replaceChildrenFast() {
    Node48 node = new Node48(0);
    for (int i = 0; i < 30; i += 3) {
      Node48.insert(node, new Node4(0), (byte) i);
    }
    short originalCount = node.count;

    Node48 anotherNode = new Node48(0);
    for (int i = 0; i < 30; i += 3) {
      Node48.insert(anotherNode, new Node4(0), (byte) i);
    }
    Node[] children = anotherNode.children;

    int N = 1000_0000;
    long start = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      node.replaceChildren(children);
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start + " ms - optimized");
    assertEquals(originalCount, node.count);


    start = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      node.replaceChildrenOriginal(children);
    }
    end = System.currentTimeMillis();
    System.out.println(end - start + " ms - original");
    assertEquals(originalCount, node.count);
  }
}
