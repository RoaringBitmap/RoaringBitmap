package org.roaringbitmap.art;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

  @Test
  public void testWithOffsetBeforeBytes() {
    Node256 nodes = new Node256(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 75;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = Node256.insert(nodes, leafNode, (byte) (offset + i));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node256);

    // this is bad test, because it's checking the internal implementation which really
    // should not matter, because the small nodes do not use this gappy method, therefore
    // to rely on it is really fragile, thus it should not really be tested. But here we are.
    Assertions.assertEquals(offset, nodes.getMinPos());

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
    Node256 nodes = new Node256(0);
    LeafNode leafNode = new LeafNode(0, 0);
    int insertCount = 75;
    int step = 2;
    int offset = 40;

    // setup data
    for (int i = 0; i < insertCount; i++) {
      nodes = Node256.insert(nodes, leafNode, (byte) (offset + (i * step)));
    }
    // check we are testing the correct data structure
    Assertions.assertTrue(nodes instanceof Node256);

    // this is bad test, because it's checking the internal implementation which really
    // should not matter, because the small nodes do not use this gap method, therefore
    // to rely on it is really fragile, thus it should not really be tested. But here we are.
    Assertions.assertEquals(offset, nodes.getMinPos());

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

  @Test
  public void testDenseNonZeroBasedKeysSearch() {
    Node256 nodes = new Node256(0);
    final int insertCount = 75;
    final int keyOffset = 0x20;

    // create the data
    for (int i = 0; i < insertCount; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      byte key = (byte) (i + keyOffset);
      nodes = Node256.insert(nodes, leafNode, key);
    }
    // check we are testing the correct thing
    Assertions.assertTrue(nodes instanceof Node256);

    // check that searching for each key, is FOUND
    for (int i = 0; i < insertCount; i++) {
      byte key = (byte) (i + keyOffset);
      SearchResult sr = nodes.getNearestChildPos(key);

      Assertions.assertEquals(SearchResult.Outcome.FOUND, sr.outcome);
      Assertions.assertTrue(sr.hasKeyPos());
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
    Node256 nodes = new Node256(0);
    final int insertCount = 75;
    final int lastValue = insertCount - 1;
    final int step = 3;
    final int keyOffset = 0x10;

    // create the data
    for (int i = 0; i < insertCount; i++) {
      LeafNode leafNode = new LeafNode(i, i);
      byte key = (byte) ((i * step) + keyOffset);
      nodes = Node256.insert(nodes, leafNode, key);
    }
    // check we are testing the correct thing
    Assertions.assertTrue(nodes instanceof Node256);

    // check that searching for each key, is FOUND
    for (int i = 0; i < insertCount; i++) {
      byte key = (byte) ((i * step) + keyOffset);
      SearchResult sr = nodes.getNearestChildPos(key);

      Assertions.assertEquals(SearchResult.Outcome.FOUND, sr.outcome);
      Assertions.assertTrue(sr.hasKeyPos());
      int keyPos = sr.getKeyPos();
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
}
