package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Node256 extends Node {

  Node[] children = new Node[256];

  public Node256(int compressedPrefixSize) {
    super(NodeType.NODE256, compressedPrefixSize);
  }

  @Override
  public int getChildPos(byte k) {
    int pos = Byte.toUnsignedInt(k);
    if (children[pos] != null) {
      return pos;
    }
    return ILLEGAL_IDX;
  }

  @Override
  public Node getChild(int pos) {
    return children[pos];
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    children[pos] = freshOne;
  }

  @Override
  public int getMinPos() {
    for (int i = 0; i < 256; i++) {
      if (children[i] != null) {
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
      if (children[pos] != null) {
        return pos;
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public int getMaxPos() {
    for (int i = 255; i >= 0; i--) {
      if (children[i] != null) {
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
      if (children[pos] != null) {
        return pos;
      }
    }
    return ILLEGAL_IDX;
  }

  /**
   * insert the child node into the node256 node with the key byte
   * @param currentNode the node256
   * @param child the child node
   * @param key the key byte
   * @return the node256 node
   */
  public static Node insert(Node currentNode, Node child, byte key) {
    Node256 node256 = (Node256) currentNode;
    node256.count++;
    node256.children[Byte.toUnsignedInt(key)] = child;
    return node256;
  }

  @Override
  public Node remove(int pos) {
    this.children[pos] = null;
    this.count--;
    if (this.count <= 36) {
      Node48 node48 = new Node48(this.prefixLength);
      int j = 0;
      for (int i = 0; i < 256; i++) {
        if (children[i] != null) {
          node48.childIndex[i] = (byte) j;
          node48.children[j] = children[i];
          j++;
        }
      }
      node48.count = j;
      copyPrefix(this, node48);
      return node48;
    }
    return this;
  }

  @Override
  public void replaceChildren(Node[] children) {
    this.children = children;
  }

  @Override
  public void serializeNodeBody(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {

  }

  @Override
  public int serializeNodeBodySizeInBytes() {
    return 0;
  }
}
