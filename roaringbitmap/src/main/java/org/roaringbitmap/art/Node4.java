package org.roaringbitmap.art;

import org.roaringbitmap.longlong.IntegerUtil;
import org.roaringbitmap.longlong.LongUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Node4 extends BranchNode {

  int key = 0;
  Node[] children = new Node[4];

  public Node4(int compressedPrefixSize) {
    super(NodeType.NODE4, compressedPrefixSize);
  }

  @Override
  public int getChildPos(byte k) {
    for (int i = 0; i < count; i++) {
      int shiftLeftLen = (3 - i) * 8;
      byte v = (byte) (key >> shiftLeftLen);
      if (v == k) {
        return i;
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public SearchResult getNearestChildPos(byte k) {
    byte[] firstBytes = IntegerUtil.toBDBytes(key);
    return binarySearchWithResult(firstBytes, 0, count, k);
  }

  @Override
  public byte getChildKey(int pos) {
    int shiftLeftLen = (3 - pos) * 8;
    byte v = (byte) (key >> shiftLeftLen);
    return v;
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
   *
   * @param node the node4 to insert into
   * @param childNode the child node
   * @param key the key byte
   * @return the input node4 or an adaptive generated node16
   */
  public static BranchNode insert(BranchNode node, Node childNode, byte key) {
    Node4 current = (Node4) node;
    if (current.count < 4) {
      // insert leaf into current node
      current.key = IntegerUtil.setByte(current.key, key, current.count);
      current.children[current.count] = childNode;
      current.count++;
      insertionSort(current);
      return current;
    } else {
      // grow to Node16
      Node16 node16 = new Node16(current.prefixLength);
      node16.count = 4;
      node16.firstV = LongUtils.initWithFirst4Byte(current.key);
      System.arraycopy(current.children, 0, node16.children, 0, 4);
      copyPrefix(current, node16);
      BranchNode freshOne = Node16.insert(node16, childNode, key);
      return freshOne;
    }
  }

  @Override
  public Node remove(int pos) {
    assert pos < count;
    children[pos] = null;
    count--;
    key = IntegerUtil.shiftLeftFromSpecifiedPosition(key, pos, (4 - pos - 1));
    for (; pos < count; pos++) {
      children[pos] = children[pos + 1];
    }
    if (count == 1) {
      // shrink to the child node
      Node childNode = children[0];
      if (childNode instanceof BranchNode) {
        BranchNode child = (BranchNode) childNode;
        byte newLength = (byte) (child.prefixLength + this.prefixLength + 1);
        byte[] newPrefix = new byte[newLength];
        System.arraycopy(this.prefix, 0, newPrefix, 0, this.prefixLength);
        newPrefix[this.prefixLength] = IntegerUtil.firstByte(key);
        System.arraycopy(child.prefix, 0, newPrefix, this.prefixLength + 1, child.prefixLength);
        child.prefixLength = newLength;
        child.prefix = newPrefix;
      }
      return childNode;
    }
    return this;
  }

  @Override
  public void serializeNodeBody(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(Integer.reverseBytes(key));
  }

  /**
   * serialize the node's body content
   */
  @Override
  public void serializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    byteBuffer.putInt(key);
  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {
    int v = dataInput.readInt();
    key = Integer.reverseBytes(v);
  }

  /**
   * deserialize the node's body content
   */
  @Override
  public void deserializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    key = byteBuffer.getInt();
  }

  @Override
  public int serializeNodeBodySizeInBytes() {
    return 4;
  }

  @Override
  public void replaceChildren(Node[] children) {
    System.arraycopy(children, 0, this.children, 0, count);
  }

  /**
   * sort the key byte array of node4 type by the insertion sort algorithm.
   *
   * @param node4 node14 or node16
   */
  private static void insertionSort(Node4 node4) {
    byte[] key = IntegerUtil.toBDBytes(node4.key);
    byte[] sortedKey = sortSmallByteArray(key, node4.children, 0, node4.count - 1);
    node4.key = IntegerUtil.fromBDBytes(sortedKey);
  }
}
