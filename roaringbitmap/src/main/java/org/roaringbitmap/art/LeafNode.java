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
    super();
    setKeyFromShifted(LongUtils.fromKey(key));
    this.containerIdx = containerIdx;
  }

  /**
   * constructor
   * @param key a long value,only the high 48 bit is valuable
   * @param containerIdx the corresponding container index
   */
  public LeafNode(long key, long containerIdx) {
    super();
    setKeyFromShifted(key);
    this.containerIdx = containerIdx;
  }

  @Override
  protected LeafNode clone() {
    return new LeafNode(getKey(), containerIdx);
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

  public long getContainerIdx() {
    return containerIdx;
  }

  public byte[] getKeyBytes() {
    return LongUtils.highPart(getKey() << 16);
  }

  public long getKey() {
    return (((long) keyHigh) & 0xFFFFFFFFL) << 16 | (((long)keyLow) & 0xFFFFL);
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

  @Override
  protected void serializeHeader(DataOutput dataOutput) throws IOException {
    // first byte: node type
    dataOutput.writeByte((byte) NodeType.LEAF_NODE.ordinal());
    // non null object count
    dataOutput.writeShort(0);
    dataOutput.writeByte(0);
  }

  @Override
  protected void serializeHeader(ByteBuffer byteBuffer) throws IOException {
    byteBuffer.put((byte) NodeType.LEAF_NODE.ordinal());
    byteBuffer.putShort((short)0);
    byteBuffer.put((byte)0);
  }

  @Override
  public String toString() {
    return "LeafNode{" +
            "key=" + Long.toHexString(getKey()) +
            ", containerIdx=" + containerIdx +
            '}';
  }

}
