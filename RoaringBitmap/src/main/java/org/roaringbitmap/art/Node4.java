package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Node4 extends Node {

  byte[] key = new byte[4];
  Node[] children = new Node[4];

  public Node4(int compressedPrefixSize) {
    super(NodeType.NODE4, compressedPrefixSize);
  }

  @Override
  public int getChildPos(byte k) {
    for (int i = 0; i < count; i++) {
      if (key[i] == k) {
        return i;
      }
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
    return 0;
  }

  @Override
  public int getNextLargerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      return 0;
    }
    pos++;
    return pos < count ? pos : ILLEGAL_IDX;
  }

  @Override
  public int getMaxPos() {
    return count - 1;
  }

  @Override
  public int getNextSmallerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      return count - 1;
    }
    pos--;
    return pos >= 0 ? pos : ILLEGAL_IDX;
  }

  /**
   * insert the child node into the node4 with the key byte
   * @param node the node4 to insert into
   * @param childNode the child node
   * @param key the key byte
   * @return the input node4 or an adaptive generated node16
   */
  public static Node insert(Node node, Node childNode, byte key) {
    Node4 current = (Node4) node;
    if (current.count < 4) {
      //insert leaf into current node
      Arrays.binarySearch(current.key, (byte) -1);
      current.key[current.count] = key;
      current.children[current.count] = childNode;
      current.count++;
      Node.insertionSortOnNode4Or16(current);
      return current;
    } else {
      //grow to Node16
      Node16 node16 = new Node16(current.prefixLength);
      node16.count = 4;
      for (int i = 0; i < 4; i++) {
        node16.key[i] = current.key[i];
        node16.children[i] = current.children[i];
      }
      copyPrefix(current, node16);
      Node freshOne = Node16.insert(node16, childNode, key);
      return freshOne;
    }
  }

  @Override
  public Node remove(int pos) {
    assert pos < count;
    children[pos] = null;
    count--;
    for (; pos < count; pos++) {
      key[pos] = key[pos + 1];
      children[pos] = children[pos + 1];
    }
    if (count == 1) {
      //shrink to leaf node
      Node child = children[0];
      int newLength = child.prefixLength + this.prefixLength + 1;
      byte[] newPrefix = new byte[newLength];
      System.arraycopy(this.prefix, 0, newPrefix, 0, this.prefixLength);
      newPrefix[this.prefixLength] = this.key[0];
      System.arraycopy(child.prefix, 0, newPrefix, this.prefixLength + 1, child.prefixLength);
      child.prefixLength = newLength;
      child.prefix = newPrefix;
      return child;
    }
    return this;
  }

  @Override
  public void serializeNodeBody(DataOutput dataOutput) throws IOException {
    dataOutput.write(key);
  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {
    dataInput.readFully(key);
  }

  @Override
  public int serializeNodeBodySizeInBytes() {
    return 4;
  }


  @Override
  public void replaceChildren(Node[] children) {
    this.children = children;
  }
}
