package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.roaringbitmap.longlong.LongUtils;

public class LeafNode extends Node {
  //key are saved as the lazy expanding logic
  byte[] key;
  long containerIdx;

  /**
   * constructor
   * @param key the 48 bit
   * @param containerIdx the corresponding container index
   */
  public LeafNode(byte[] key, long containerIdx) {
    super(NodeType.LEAF_NODE, 0);
    this.key = key;
    this.containerIdx = containerIdx;
  }

  @Override
  public void serializeNodeBody(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(Integer.reverse(key.length));
    dataOutput.write(key);
    byte[] containerIdxBytes = LongUtils.toLDBytes(containerIdx);
    dataOutput.write(containerIdxBytes);
  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {
    int keyLen = Integer.reverse(dataInput.readInt());
    this.key = new byte[keyLen];
    dataInput.readFully(key);
    byte[] littleEndianL = new byte[8];
    dataInput.readFully(littleEndianL);
    this.containerIdx = LongUtils.fromLDBytes(littleEndianL);
  }

  @Override
  public int serializeNodeBodySizeInBytes() {
    return 4 + key.length + 8;
  }

  @Override
  public int getChildPos(byte k) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Node getChild(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMinPos() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNextLargerPos(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxPos() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNextSmallerPos(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Node remove(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceChildren(Node[] children) {
    throw new UnsupportedOperationException();
  }

  public long getContainerIdx() {
    return containerIdx;
  }

  public byte[] getKey() {
    return key;
  }
}
