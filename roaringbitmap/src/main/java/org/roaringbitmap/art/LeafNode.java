package org.roaringbitmap.art;

import org.roaringbitmap.longlong.LongUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LeafNode extends Node {

  // key are saved as the lazy expanding logic,here we only care about the
  // high 48 bit data,so only the high 48 bit is valuable
  // - the top 32 bits of these 48
  private int keyHigh;
  // - the lower 16 bits of these 48. we use a char as its unsigned 16 bits
  private char keyLow;
  long containerIdx;
  public static final int LEAF_NODE_KEY_LENGTH_IN_BYTES = 6;

  /**
   * constructor
   *
   * @param key the 48 bit
   * @param containerIdx the corresponding container index
   */
  public LeafNode(byte[] key, long containerIdx) {
    super(NodeType.LEAF_NODE, 0);
    setKeyFromShifted(LongUtils.fromKey(key));
    this.containerIdx = containerIdx;
  }

  /**
   * constructor
   * @param key a long value,only the high 48 bit is valuable
   * @param containerIdx the corresponding container index
   */
  public LeafNode(long key, long containerIdx) {
    super(NodeType.LEAF_NODE, 0);
    setKeyFromShifted(key);
    this.containerIdx = containerIdx;
  }

  @Override
  public void serializeNodeBody(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(keyHigh);
    dataOutput.writeShort(keyLow);
    dataOutput.writeLong(Long.reverseBytes(containerIdx));
  }

  @Override
  public void serializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer.order() == ByteOrder.BIG_ENDIAN) {
      byteBuffer.putInt(keyHigh);
      byteBuffer.putChar(keyLow);
    } else {
      byteBuffer.putInt(Integer.reverseBytes(keyHigh));
      byteBuffer.putChar(Character.reverseBytes(keyLow));
    }
    byteBuffer.putLong(containerIdx);
  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {
    keyHigh = dataInput.readInt();
    keyLow = dataInput.readChar();
    this.containerIdx = Long.reverseBytes(dataInput.readLong());
  }

  @Override
  public void deserializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer.order() == ByteOrder.BIG_ENDIAN) {
      keyHigh = byteBuffer.getInt();
      keyLow = byteBuffer.getChar();
    } else {
      keyHigh = Integer.reverseBytes(byteBuffer.getInt());
      keyLow = Character.reverseBytes(byteBuffer.getChar());
    }
    this.containerIdx = byteBuffer.getLong();
  }

  @Override
  public int serializeNodeBodySizeInBytes() {
    return LEAF_NODE_KEY_LENGTH_IN_BYTES + 8;
  }

  @Override
  public int getChildPos(byte k) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SearchResult getNearestChildPos(byte key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getChildKey(int pos) {
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

  public byte[] getKeyBytes() {
    return LongUtils.highPart(getKey());
  }

    /**
     * Gets the key as a long value, only the high 48 bits are used.
     *
     * @return the long value representing the key
     */
  public long getKey() {
    return ((long) keyHigh) << 32 | ((long)keyLow) << 16;
  }

    /**
     * Sets the key from a long value, only the high 48 bits are used.
     *
     * @param key the long value representing the key
     */
  private void setKeyFromShifted(long key) {
    this.keyHigh = (int) (key >> 32);
    this.keyLow = (char) (key >> 16);
  }
}
