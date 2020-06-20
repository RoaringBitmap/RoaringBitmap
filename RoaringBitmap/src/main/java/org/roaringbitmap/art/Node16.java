package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Node16 extends Node {

  byte[] key = new byte[16];
  Node[] children = new Node[16];

  public Node16(int compressionLength) {
    super(NodeType.NODE16, compressionLength);
  }

  @Override
  public int getChildPos(byte k) {
    return Node.binarySearch(this, k);
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
   * insert a child into the node with the key byte
   * @param node the node16 to insert into
   * @param child the child node to be inserted
   * @param key the key byte
   * @return the adaptive changed node of the parent node16
   */
  public static Node insert(Node node, Node child, byte key) {
    Node16 currentNode16 = (Node16) node;
    if (currentNode16.count < 16) {
      currentNode16.key[currentNode16.count] = key;
      currentNode16.children[currentNode16.count] = child;
      currentNode16.count++;
      Node.insertionSortOnNode4Or16(currentNode16);
      return currentNode16;
    } else {
      Node48 node48 = new Node48(currentNode16.prefixLength);
      for (int i = 0; i < currentNode16.count; i++) {
        int unsignedIdx = Byte.toUnsignedInt(currentNode16.key[i]);
        //i won't be beyond 48
        node48.childIndex[unsignedIdx] = (byte) i;
        node48.children[i] = currentNode16.children[i];
      }
      copyPrefix(currentNode16, node48);
      node48.count = currentNode16.count;
      Node freshOne = Node48.insert(node48, child, key);
      return freshOne;
    }
  }

  @Override
  public Node remove(int pos) {
    children[pos] = null;
    count--;
    for (; pos < count; pos++) {
      key[pos] = key[pos + 1];
      children[pos] = children[pos + 1];
    }
    if (count <= 3) {
      //shrink to node4
      Node4 node4 = new Node4(prefixLength);
      System.arraycopy(key, 0, node4.key, 0, count);
      System.arraycopy(children, 0, node4.children, 0, count);
      node4.count = count;
      copyPrefix(this, node4);
      return node4;
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
    return 16;
  }

  @Override
  public void replaceChildren(Node[] children) {
    this.children = children;
  }
}
