package org.roaringbitmap.art;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class Node16Test {

  @Test
  public void test() {
    Node4 node4 = new Node4(0);
    // insert 4 nodes
    for (int i = 0; i < 4; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      node4 = (Node4) node4.insert(leafNode, (byte) i);
    }
    // insert the fifth node
    LeafNode leafNode4 = new LeafNode(4, 4);
    Node16 node16 = (Node16) node4.insert(leafNode4, (byte) 4);
    // remove two nodes to shrink to node4
    node16 = (Node16) node16.remove(4);
    Node degenerativeNode = node16.remove(3);
    Assertions.assertTrue(degenerativeNode instanceof Node4);
    // recover to node16 by re-insert two nodes
    Node4 degenerativeNode4 = (Node4) degenerativeNode;
    BranchNode node = degenerativeNode4.insert(leafNode4, (byte) 4);
    LeafNode leafNode3 = new LeafNode(3, 3);
    node16 = (Node16) node.insert(leafNode3, (byte) 3);

    byte key = 4;
    Assertions.assertEquals(4, node16.getChildPos(key));
    Assertions.assertEquals(key, node16.getChildKey(4));
    for (int i = 5; i < 12; i++) {
      byte key1 = (byte) i;
      LeafNode leafNode = new LeafNode(i, i);
      node16 = (Node16) node16.insert(leafNode, key1);
      Assertions.assertEquals(i, node16.getChildPos(key1));
    }
    LeafNode leafNode = new LeafNode(12, 12);
    key = (byte) -2;
    node16 = (Node16) node16.insert(leafNode, key);
    Assertions.assertEquals(12, node16.getChildPos(key));
    Assertions.assertEquals(key, node16.getChildKey(12));
    leafNode = new LeafNode(13, 13);
    byte key12 = (byte) 12;
    node16 = (Node16) node16.insert(leafNode, key12);
    Assertions.assertEquals(12, node16.getChildPos(key12));
    Assertions.assertEquals(key12, node16.getChildKey(12));
    Assertions.assertEquals(13, node16.getChildPos(key));
    Assertions.assertEquals(key, node16.getChildKey(13));
  }

  @Test
  public void testGrowToNode48() {
    Node16 node16 = new Node16(0);
    LeafNode leafNode;
    for (int i = 0; i < 16; i++) {
      leafNode = new LeafNode(i, i);
      node16 = (Node16) node16.insert(leafNode, (byte) i);
    }
    leafNode = new LeafNode(16, 16);
    Node node = node16.insert(leafNode, (byte) 16);
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
    final int insertCount = 15;
    final int lastValue = insertCount - 1;

    // create the data
    for (int i = 0; i < insertCount; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      node16 = (Node16) node16.insert(leafNode, (byte) i);
    }

    // check the range is as expected
    Assertions.assertEquals(0, node16.getMinPos());
    Assertions.assertEquals(lastValue, node16.getMaxPos());

    // the next larger position of each value is the + 1
    for (int i = 0; i < lastValue; i++) {
      int pos = node16.getNextLargerPos(i);
      LeafNode leafNode = (LeafNode) node16.getChild(pos);
      Assertions.assertEquals(i + 1, leafNode.getContainerIdx());
    }

    // the next smaller position of each value is the -1
    for (int i = lastValue; i >= 1; i--) {
      int pos = node16.getNextSmallerPos(i);
      LeafNode leafNode = (LeafNode) node16.getChild(pos);
      Assertions.assertEquals(i - 1, leafNode.getContainerIdx());
    }
    // there is no illegal_idx valid prior to the first
    Assertions.assertEquals(BranchNode.ILLEGAL_IDX, node16.getNextSmallerPos(0));
  }

  @Test
  public void testDenseNonZeroBasedKeysSearch() {
    BranchNode nodes = new Node16(0);
    final int insertCount = 15;
    final int lastValue = insertCount - 1;
    final int keyOffset = 0x20;

    // create the data
    for (int i = 0; i < insertCount; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      byte key = (byte) (i + keyOffset);
      nodes = nodes.insert(leafNode, key);
    }
    // check we are testing the correct thing
    Assertions.assertTrue(nodes instanceof Node16);

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
    BranchNode nodes = new Node16(0);
    final int insertCount = 15;
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
    Assertions.assertTrue(nodes instanceof Node16);

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
          int expect = Byte.toUnsignedInt(key) - step;
          int result = Byte.toUnsignedInt(nodes.getChildKey(sr.getNextSmallerPos()));
          Assertions.assertEquals(expect, result);
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
          int expect = Byte.toUnsignedInt(key) + step;
          int result = Byte.toUnsignedInt(nodes.getChildKey(sr.getNextLargerPos()));
          Assertions.assertEquals(expect, result);
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
    BranchNode nodes = new Node16(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 16;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = nodes.insert(leafNode, (byte) (offset + i));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node16);

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
    BranchNode nodes = new Node16(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 16;
    int step = 2;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = nodes.insert(leafNode, (byte) (offset + (i * step)));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node16);

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
