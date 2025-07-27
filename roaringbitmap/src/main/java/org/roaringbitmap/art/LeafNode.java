package org.roaringbitmap.art;

import org.roaringbitmap.longlong.LongUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class LeafNode extends Node {

  // key are saved as the lazy expanding logic,here we only care about the
  // high 48 bit data,so only the high 48 bit is valuable
  private long key;
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
    byte[] bytes = new byte[8];
    System.arraycopy(key, 0, bytes, 0, LEAF_NODE_KEY_LENGTH_IN_BYTES);
    this.key = LongUtils.fromBDBytes(bytes);
    this.containerIdx = containerIdx;
  }

  /**
   * constructor
   * @param key a long value ,only the high 48 bit is valuable
   * @param containerIdx the corresponding container index
   */
  public LeafNode(long key, long containerIdx) {
    super();
    this.key = key;
    this.containerIdx = containerIdx;
  }

  @Override
  public void serializeNodeBody(DataOutput dataOutput) throws IOException {
    byte[] keyBytes = LongUtils.highPart(key);
    dataOutput.write(keyBytes);
    dataOutput.writeLong(Long.reverseBytes(containerIdx));
  }

  @Override
  public void serializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    byte[] keyBytes = LongUtils.highPart(key);
    byteBuffer.put(keyBytes);
    byteBuffer.putLong(containerIdx);
  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {
    byte[] longBytes = new byte[8];
    dataInput.readFully(longBytes, 0, LEAF_NODE_KEY_LENGTH_IN_BYTES);
    this.key = LongUtils.fromBDBytes(longBytes);
    this.containerIdx = Long.reverseBytes(dataInput.readLong());
  }

  @Override
  public void deserializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    byte[] bytes = new byte[8];
    byteBuffer.get(bytes, 0, 6);
    this.key = LongUtils.fromBDBytes(bytes);
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
    return LongUtils.highPart(key);
  }

  public long getKey() {
    return key >>> 16;
  }

  protected void serializeHeader(DataOutput dataOutput) throws IOException {
    // first byte: node type
    dataOutput.writeByte((byte) NodeType.LEAF_NODE.ordinal());
    // non null object count
    dataOutput.writeShort(0);
    dataOutput.writeByte(0);
  }

  protected void serializeHeader(ByteBuffer byteBuffer) throws IOException {
    byteBuffer.put((byte) NodeType.LEAF_NODE.ordinal());
    byteBuffer.putShort((short)0);
    byteBuffer.put((byte)0);
  }

}
