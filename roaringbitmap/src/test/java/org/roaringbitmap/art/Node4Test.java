package org.roaringbitmap.art;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertSame;

public class Node4Test {

  private void assertContent(Node4 node4, Node ... children) {
    Assertions.assertEquals(children.length, node4.count);
    Assertions.assertArrayEquals(Arrays.copyOf(children, 4), node4.children);
  }

  private void assertKeys(Node4 node4, byte ... keys) {
    Assertions.assertEquals(keys.length, node4.count);
    for (int i = 0; i < keys.length; i++) {
      Assertions.assertEquals(keys[i], (byte)(node4.key >> (24-8*i)), "key at pos " + i);
    }
    //it seems that the rest of the key are undefined
    //not sure if they should be though
  }

  @Test
  public void testTheBasics() throws IOException {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);
    Node4 node4 = new Node4(0);
    assertKeys(node4);
    assertContent(node4);
    byte key1 = 2;
    node4.insert(leafNode1, key1);
    Assertions.assertEquals(0, node4.getMaxPos());
    Assertions.assertEquals(0, node4.getMinPos());
    Assertions.assertEquals(0, node4.getChildPos(key1));
    Assertions.assertEquals(key1, node4.getChildKey(0));
    assertKeys(node4, key1);
    assertContent(node4, leafNode1);

    byte key2 = 1;
    node4 = (Node4) node4.insert(leafNode2, key2);
    Assertions.assertEquals(0, node4.getChildPos(key2));
    Assertions.assertEquals(1, node4.getChildPos(key1));
    Assertions.assertEquals(key2, node4.getChildKey(0));
    assertKeys(node4, key2, key1);
    assertContent(node4, leafNode2, leafNode1);

    byte key3 = -1;
    node4 = (Node4) node4.insert(leafNode3, key3);
    Assertions.assertEquals(2, node4.getChildPos(key3));
    Assertions.assertEquals(key3, node4.getChildKey(2));
    node4 = (Node4) node4.remove(1);
    Assertions.assertEquals(0, node4.getChildPos(key2));
    Assertions.assertEquals(1, node4.getChildPos(key3));
    Assertions.assertEquals(key3, node4.getChildKey(1));
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, node4.getChildPos(key1));
    assertKeys(node4, key2, key3);
    assertContent(node4, leafNode2, leafNode3);

    int bytesSize = node4.serializeSizeInBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    node4.serialize(dataOutputStream);
    Assertions.assertEquals(bytesSize, byteArrayOutputStream.toByteArray().length);
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    Node4 deserializedNode4 = (Node4) Node.deserialize(dataInputStream);
    Assertions.assertEquals(0, deserializedNode4.getChildPos(key2));
    Assertions.assertEquals(1, deserializedNode4.getChildPos(key3));
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, deserializedNode4.getChildPos(key1));

    Node node = node4.remove(0);
    Assertions.assertTrue(node instanceof LeafNode);
  }
  byte[] keys(int... keys) {
    byte[] result = new byte[keys.length];
    for (int i = 0; i < keys.length; i++) {
      result[i] = (byte) keys[i];
    }
    return result;
  }
    Node[] nodes(Node... nodes) {
        return nodes;
    }

  @Test
  void testOrderedInsert() {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);
    LeafNode leafNode4 = new LeafNode(4, 4);

    Node4 node4 = new Node4(0);
    addEachNode(node4, nodes(leafNode1, leafNode2, leafNode3, leafNode4), keys(1, 2, 3, 4));

    assertKeys(node4, keys(1,2,3,4));
    assertContent(node4, nodes(leafNode1, leafNode2, leafNode3, leafNode4));

  }
  @Test
  void testUnorderedInsert() {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);
    LeafNode leafNode4 = new LeafNode(4, 4);

    Node4 node4 = new Node4(0);
    addEachNode(node4, nodes(leafNode2, leafNode4, leafNode3, leafNode1), keys(2,4,3,1));

    assertKeys(node4, keys(1,2,3,4));
    assertContent(node4, nodes(leafNode1, leafNode2, leafNode3, leafNode4));

  }
  @Test
  void testRemoveFullLast() {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);
    LeafNode leafNode4 = new LeafNode(4, 4);

    Node4 node4 = new Node4(0);
    addEachNode(node4, nodes(leafNode1, leafNode2, leafNode3, leafNode4), keys(1,2,3,4));

    node4.remove(3);
    assertKeys(node4, keys(1,2,3));
    assertContent(node4, nodes(leafNode1, leafNode2, leafNode3));

  }
  @Test
  void testRemoveFullMiddle() {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);
    LeafNode leafNode4 = new LeafNode(4, 4);

    Node4 node4 = new Node4(0);
    addEachNode(node4, nodes(leafNode1, leafNode2, leafNode3, leafNode4), keys(1,2,3,4));

    node4.remove(1);
    assertKeys(node4, keys(1,3,4));
    assertContent(node4, nodes(leafNode1, leafNode3, leafNode4));

  }
  @Test
  void testRemoveFullFirst() {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);
    LeafNode leafNode4 = new LeafNode(4, 4);

    Node4 node4 = new Node4(0);
    addEachNode(node4, nodes(leafNode1, leafNode2, leafNode3, leafNode4), keys(1,2,3,4));

    node4.remove(0);
    assertKeys(node4, keys(2,3,4));
    assertContent(node4, nodes(leafNode2, leafNode3, leafNode4));

  }
  @Test
  void testRemoveNotFullLast() {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);

    Node4 node4 = new Node4(0);
    addEachNode(node4, nodes(leafNode1, leafNode2, leafNode3), keys(1,2,3));

    node4.remove(2);
    assertKeys(node4, keys(1,2));
    assertContent(node4, nodes(leafNode1, leafNode2));

  }
  @Test
  void testRemoveNotFullMiddle() {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);

    Node4 node4 = new Node4(0);
    addEachNode(node4, nodes(leafNode1, leafNode2, leafNode3), keys(1,2,3));

    node4.remove(1);
    assertKeys(node4, keys(1,3));
    assertContent(node4, nodes(leafNode1, leafNode3));

  }
  @Test
  void testRemoveNotFullFirst() {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);

    Node4 node4 = new Node4(0);
    addEachNode(node4, nodes(leafNode1, leafNode2, leafNode3), keys(1,2,3));

    node4.remove(0);
    assertKeys(node4, keys(2,3));
    assertContent(node4, nodes(leafNode2, leafNode3));

  }

  private void addEachNode(Node4 node4, Node[] nodes, byte[] keys) {
    Assertions.assertEquals(nodes.length, keys.length, "Nodes and keys should have the same length");
    for (int i = 0; i < nodes.length; i++) {
      Node node = nodes[i];
      byte key = keys[i];
      Node result = node4.insert(node, key);
      assertSame(node4, result, "Inserting node at position " + i + " with key " + key);
      Assertions.assertEquals(i+1, node4.count);
    }
  }

  @Test
  public void testGetNearestChildPosWithOneItem() {
    LeafNode ln1 = new LeafNode(0x0100, 1);
    byte key1 = 0x10;
    int key1Pos = 0;

    Node4 node = new Node4(0);

    node.insert(ln1, key1);

    // search for the key we just added returns the
    SearchResult sr = node.getNearestChildPos(key1);
    Assertions.assertEquals(SearchResult.Outcome.FOUND, sr.outcome);
    Assertions.assertTrue(sr.hasKeyPos());
    // With only 1 item in the list the pos, it seems safe to assume the pos is zero
    Assertions.assertEquals(key1Pos, sr.getKeyPos());
    // ether way, we should be able to get our key back.
    Assertions.assertEquals(key1, node.getChildKey(sr.getKeyPos()));

    // search key + 1
    sr = node.getNearestChildPos((byte) (key1 + 1));
    // which should not match anything
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    // given we are beyond the key1, the "smaller" should be that position
    Assertions.assertEquals(key1Pos, sr.getNextSmallerPos());
    // and the "larger" should be ILLEGAL_IDX
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextLargerPos());

    // search key - 1
    sr = node.getNearestChildPos((byte) (key1 - 1));
    // which should not match anything
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    // given we are before the key1, the "smaller" should be ILLEGAL_IDX
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextSmallerPos());
    // and the "larger" should be position of key1
    Assertions.assertEquals(key1Pos, sr.getNextLargerPos());
  }

  @Test
  public void testGetNearestChildPosWithTwoItems() {
    LeafNode ln1 = new LeafNode(0x0100, 1);
    LeafNode ln2 = new LeafNode(0x0100, 2);
    byte key1 = 0x10;
    byte key2 = 0x20;
    int key1Pos = 0;
    int key2Pos = 1;

    Node4 node = new Node4(0);

    node.insert(ln1, key1);
    node.insert(ln2, key2);

    // value checks
    Assertions.assertTrue((key1 + 1) < (key2 - 1));
    Assertions.assertTrue(node instanceof Node4);

    // search for the first key we just added returns the
    SearchResult sr = node.getNearestChildPos(key1);
    Assertions.assertEquals(SearchResult.Outcome.FOUND, sr.outcome);
    Assertions.assertEquals(key1Pos, sr.getKeyPos());

    // search for the second key we just added returns the
    sr = node.getNearestChildPos(key2);
    Assertions.assertEquals(SearchResult.Outcome.FOUND, sr.outcome);
    Assertions.assertEquals(key2Pos, sr.getKeyPos());

    // search key1 + 1, aka in between
    sr = node.getNearestChildPos((byte) (key1 + 1));
    // which should not match anything
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    // given we are beyond the key1, the "smaller" should be that position
    Assertions.assertEquals(key1Pos, sr.getNextSmallerPos());
    // and the "larger" should be key2pos
    Assertions.assertEquals(key2Pos, sr.getNextLargerPos());

    // search key1 - 1
    sr = node.getNearestChildPos((byte) (key1 - 1));
    // which should not match anything
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    // given we are before the key1, the "smaller" should be ILLEGAL_IDX
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextSmallerPos());
    // and the "larger" should be key2pos
    Assertions.assertEquals(key1Pos, sr.getNextLargerPos());

    // search key2 - 1, aka in between again
    sr = node.getNearestChildPos((byte) (key2 - 1));
    // which should not match anything
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    // given we are before the key2, the "smaller" should be key1pos
    Assertions.assertEquals(key1Pos, sr.getNextSmallerPos());
    // and the "larger" should be key2pos
    Assertions.assertEquals(key2Pos, sr.getNextLargerPos());

    // search key2 + 1, after both
    sr = node.getNearestChildPos((byte) (key2 + 1));
    // which should not match anything
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    // given we are beyond the key2, the "smaller" should be key2pos
    Assertions.assertEquals(key2Pos, sr.getNextSmallerPos());
    // and the "larger" should be ILLEGAL_IDX
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextLargerPos());
  }

  @Test
  public void testDenseNonZeroBasedKeysSearch() {
    BranchNode nodes = new Node4(0);
    final int insertCount = 3;
    final int keyOffset = 0x20;

    // create the data
    for (int i = 0; i < insertCount; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      byte key = (byte) (i + keyOffset);
      nodes = nodes.insert(leafNode, key);
    }
    // check we are testing the correct thing
    Assertions.assertTrue(nodes instanceof Node4);

    // check that searching for each key, is FOUND
    for (int i = 0; i < insertCount; i++) {
      byte key = (byte) (i + keyOffset);
      SearchResult sr = nodes.getNearestChildPos(key);

      Assertions.assertEquals(SearchResult.Outcome.FOUND, sr.outcome);
      Assertions.assertTrue(sr.hasKeyPos());
      // the positions are zero based, even though the keys values are offset
      Assertions.assertEquals(i, sr.getKeyPos());
      Assertions.assertEquals(key, nodes.getChildKey(sr.getKeyPos()));
    }

    // search before the first value "keyOffset", and surprise, nothing will be found
    SearchResult sr = nodes.getNearestChildPos((byte) (keyOffset - 1));
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextSmallerPos());

    // search after the last value aka "insertCount", and surprise, nothing will be found
    sr = nodes.getNearestChildPos((byte) (keyOffset + insertCount));
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextLargerPos());
  }

  @Test
  public void testSparseNonZeroBasedKeysSearch() {
    BranchNode nodes = new Node4(0);
    final int insertCount = 3;
    final int lastValue = insertCount - 1;

    final int step = 3;
    final int keyOffset = 0x20;

    // create the data
    for (int i = 0; i < insertCount; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      byte key = (byte) ((i * step) + keyOffset);
      nodes = nodes.insert(leafNode, key);
    }
    // check we are testing the correct thing
    Assertions.assertTrue(nodes instanceof Node4);

    // check that searching for each key, is FOUND
    for (int i = 0; i < insertCount; i++) {
      byte key = (byte) ((i * step) + keyOffset);
      SearchResult sr = nodes.getNearestChildPos(key);

      Assertions.assertEquals(SearchResult.Outcome.FOUND, sr.outcome);
      Assertions.assertTrue(sr.hasKeyPos());
      // the positions are zero based, even though the keys values are offset
      int keyPos = sr.getKeyPos();
      Assertions.assertEquals(i, keyPos);
      Assertions.assertEquals(key, nodes.getChildKey(sr.getKeyPos()));

      // search in the "gaps" before the key
      {
        byte bKey = (byte) (key - 1);
        sr = nodes.getNearestChildPos(bKey);
        Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
        Assertions.assertFalse(sr.hasKeyPos());

        // the value smaller than the first should be INVALID, and the rest should be the prior key
        if (i == 0) {
          Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextSmallerPos());
        } else {
          int expected = Byte.toUnsignedInt(key) - step;
          int result = Byte.toUnsignedInt(nodes.getChildKey(sr.getNextSmallerPos()));
          Assertions.assertEquals(expected, result);
        }
        // the NextLarger of the "key-1" should be the key
        Assertions.assertEquals(keyPos, sr.getNextLargerPos());
        Assertions.assertEquals(key, nodes.getChildKey(sr.getNextLargerPos()));
      }

      // search in the "gaps" after the key
      {
        byte aKey = (byte) (key + 1);

        sr = nodes.getNearestChildPos(aKey);
        Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
        Assertions.assertFalse(sr.hasKeyPos());

        // the next smaller pos than "key+1" should always be key
        Assertions.assertEquals(keyPos, sr.getNextSmallerPos());
        Assertions.assertEquals(key, nodes.getChildKey(sr.getNextSmallerPos()));

        // the value larger than the last should be INVALID and the rest should be the next key
        if (i == lastValue) {
          Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextLargerPos());
        } else {
          int expected = Byte.toUnsignedInt(key) + step;
          int result = Byte.toUnsignedInt(nodes.getChildKey(sr.getNextLargerPos()));
          Assertions.assertEquals(expected, result);
        }
      }
    }

    // search before the first value "keyOffset", and surprise, nothing will be found
    SearchResult sr = nodes.getNearestChildPos((byte) (keyOffset - 1));
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextSmallerPos());

    // search after the last value aka "insertCount", and surprise, nothing will be found
    sr = nodes.getNearestChildPos((byte) (keyOffset + (insertCount * step)));
    Assertions.assertEquals(SearchResult.Outcome.NOT_FOUND, sr.outcome);
    Assertions.assertFalse(sr.hasKeyPos());
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, sr.getNextLargerPos());
  }

  @Test
  public void testWithOffsetBeforeBytes() {
    BranchNode nodes = new Node4(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 4;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = nodes.insert(leafNode, (byte) (offset + i));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node4);

    // The position of a value before the "first" value dose not exist thus ILLEGAL_IDX
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, nodes.getNextSmallerPos(nodes.getMinPos()));

    // The position of a value after the "last" value dose not exist thus ILLEGAL_IDX
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, nodes.getNextLargerPos(nodes.getMaxPos()));

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
    BranchNode nodes = new Node4(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 4;
    int step = 2;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = nodes.insert(leafNode, (byte) (offset + (i * step)));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node4);

    // The position of a value before the "first" value dose not exist thus ILLEGAL_IDX
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, nodes.getNextSmallerPos(nodes.getMinPos()));

    // The position of a value after the "last" value dose not exist thus ILLEGAL_IDX
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, nodes.getNextLargerPos(nodes.getMaxPos()));

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
}
