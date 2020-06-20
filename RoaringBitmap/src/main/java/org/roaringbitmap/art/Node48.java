package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Node48 extends Node {

  //the value of childIndex content won't be beyond 48
  byte[] childIndex = new byte[256];
  Node[] children = new Node[48];
  static final byte EMPTY_VALUE = 48;

  public Node48(int compressedPrefixSize) {
    super(NodeType.NODE48, compressedPrefixSize);
    Arrays.fill(childIndex, EMPTY_VALUE);
  }

  @Override
  public int getChildPos(byte k) {
    int unsignedIdx = Byte.toUnsignedInt(k);
    if (childIndex[unsignedIdx] != EMPTY_VALUE) {
      return unsignedIdx;
    }
    return ILLEGAL_IDX;
  }

  @Override
  public Node getChild(int pos) {
    byte idx = childIndex[pos];
    return children[(int) idx];
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    byte idx = childIndex[pos];
    children[(int) idx] = freshOne;
  }

  @Override
  public int getMinPos() {
    for (int i = 0; i < 256; i++) {
      if (childIndex[i] != EMPTY_VALUE) {
        return i;
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public int getNextLargerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      pos = -1;
    }
    for (pos++; pos < 256; pos++) {
      if (childIndex[pos] != EMPTY_VALUE) {
        return pos;
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public int getMaxPos() {
    for (int i = 255; i >= 0; i--) {
      if (childIndex[i] != EMPTY_VALUE) {
        return i;
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public int getNextSmallerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      pos = 256;
    }
    for (pos--; pos >= 0; pos--) {
      if (childIndex[pos] != EMPTY_VALUE) {
        return pos;
      }
    }
    return ILLEGAL_IDX;
  }

  /**
   * insert a child node into the node48 node with the key byte
   * @param currentNode the node4
   * @param child the child node
   * @param key the key byte
   * @return the node48 or an adaptive generated node256
   */
  public static Node insert(Node currentNode, Node child, byte key) {
    Node48 node48 = (Node48) currentNode;
    if (node48.count < 48) {
      //insert leaf node into current node
      int pos = node48.count;
      if (node48.children[pos] != null) {
        pos = 0;
        while (node48.children[pos] != null) {
          pos++;
        }
      }
      node48.children[pos] = child;
      int unsignedByte = Byte.toUnsignedInt(key);
      node48.childIndex[unsignedByte] = (byte) pos;
      node48.count++;
      return node48;
    } else {
      //grow to Node256
      Node256 node256 = new Node256(node48.prefixLength);
      for (int i = 0; i < 256; i++) {
        if (node48.childIndex[i] != EMPTY_VALUE) {
          node256.children[i] = node48.children[node48.childIndex[i]];
        }
      }
      node256.count = node48.count;
      copyPrefix(node48, node256);
      Node freshOne = Node256.insert(node256, child, key);
      return freshOne;
    }
  }

  @Override
  public Node remove(int pos) {
    int idx = childIndex[pos];
    children[idx] = null;
    childIndex[pos] = EMPTY_VALUE;
    count--;
    if (count <= 12) {
      //shrink to node16
      Node16 node16 = new Node16(this.prefixLength);
      int j = 0;
      for (int i = 0; i < 256; i++) {
        if (childIndex[i] != EMPTY_VALUE) {
          node16.key[j] = (byte) i;
          node16.children[j] = children[childIndex[i]];
          j++;
        }
      }
      node16.count = j;
      copyPrefix(this, node16);
      return node16;
    }
    return this;
  }

  @Override
  public void serializeNodeBody(DataOutput dataOutput) throws IOException {
    dataOutput.write(childIndex);
  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {
    dataInput.readFully(childIndex);
  }

  @Override
  public int serializeNodeBodySizeInBytes() {
    return 256;
  }

  @Override
  public void replaceChildren(Node[] children) {
    int j = 0;
    for (int i = 0; i < 256; i++) {
      if (childIndex[i] != EMPTY_VALUE) {
        int idx = (int) childIndex[i];
        this.children[idx] = children[j];
        j++;
      }
    }
  }
}
