package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class Node {

  public Node() {
  }

  /**
   * sort the small arrays through the insertion sort alg.
   */
  protected static byte[] sortSmallByteArray(
      byte[] key, Node[] children, int left, int right) { // x
    for (int i = left, j = i; i < right; j = ++i) {
      byte ai = key[i + 1];
      Node child = children[i + 1];
      int unsignedByteAi = Byte.toUnsignedInt(ai);
      while (unsignedByteAi < Byte.toUnsignedInt(key[j])) {
        key[j + 1] = key[j];
        children[j + 1] = children[j];
        if (j-- == left) {
          break;
        }
      }
      key[j + 1] = ai;
      children[j + 1] = child;
    }
    return key;
  }

  /**
   * serialize
   *
   * @param dataOutput the DataOutput
   * @throws IOException signal a exception happened while the serialization
   */
  public void serialize(DataOutput dataOutput) throws IOException {
    serializeHeader(dataOutput);
    serializeNodeBody(dataOutput);
  }

  /**
   * serialize
   *
   * @param byteBuffer the ByteBuffer
   * @throws IOException signal a exception happened while the serialization
   */
  public void serialize(ByteBuffer byteBuffer) throws IOException {
    serializeHeader(byteBuffer);
    serializeNodeBody(byteBuffer);
  }

  /**
   * the serialized size in bytes of this node
   *
   * @return the size in bytes
   */
  public int serializeSizeInBytes() {
    int size = 0;
    size += serializeHeaderSizeInBytes();
    size += serializeNodeBodySizeInBytes();
    return size;
  }

  /**
   * deserialize into a typed node from the byte stream
   *
   * @param dataInput the input byte stream
   * @return the typed node
   * @throws IOException indicate a exception happened
   */
  public static Node deserialize(DataInput dataInput) throws IOException {
    Node node = deserializeHeader(dataInput);
    if (node != null) {
      node.deserializeNodeBody(dataInput);
      return node;
    }
    return null;
  }

  /**
   * deserialize into a typed node
   * @param byteBuffer the ByteBuffer
   * @return the typed node
   * @throws IOException indicate a exception happened
   */
  public static Node deserialize(ByteBuffer byteBuffer) throws IOException {
    Node node = deserializeHeader(byteBuffer);
    if (node != null) {
      node.deserializeNodeBody(byteBuffer);
      return node;
    }
    return null;
  }

  /**
   * serialize the node's body content
   * @param dataOutput the DataOutput
   * @throws IOException exception indicates serialization errors
   */
  abstract void serializeNodeBody(DataOutput dataOutput) throws IOException;

  /**
   * serialize the node's body content
   * @param byteBuffer the ByteBuffer
   * @throws IOException exception indicates serialization errors
   */
  abstract void serializeNodeBody(ByteBuffer byteBuffer) throws IOException;

  /**
   * deserialize the node's body content
   * @param dataInput the DataInput
   * @throws IOException exception indicates deserialization errors
   */
  abstract void deserializeNodeBody(DataInput dataInput) throws IOException;

  /**
   * deserialize the node's body content
   * @param byteBuffer the ByteBuffer
   * @throws IOException exception indicates deserialization errors
   */
  abstract void deserializeNodeBody(ByteBuffer byteBuffer) throws IOException;

  /**
   * the serialized size except the common node header part
   *
   * @return the size in bytes
   */
  public abstract int serializeNodeBodySizeInBytes();

  protected abstract void serializeHeader(DataOutput dataOutput) throws IOException ;

  protected abstract void serializeHeader(ByteBuffer byteBuffer) throws IOException ;

  protected int serializeHeaderSizeInBytes() {
    return 1 + 2 + 1;
  }

  private static Node deserializeHeader(DataInput dataInput) throws IOException {
    final int nodeTypeOrdinal = dataInput.readByte();
    final short count = Short.reverseBytes(dataInput.readShort());
    final byte prefixLength = dataInput.readByte();
    final byte[] prefix;
    if (prefixLength == 0) {
      prefix = Art.EMPTY_BYTES;
    } else {
      prefix = new byte[prefixLength];
      dataInput.readFully(prefix);
    }
    if (nodeTypeOrdinal == NodeType.NODE4.ordinal()) {
      Node4 node4 = new Node4(prefixLength);
      node4.prefix = prefix;
      node4.count = count;
      return node4;
    }
    if (nodeTypeOrdinal == NodeType.NODE16.ordinal()) {
      Node16 node16 = new Node16(prefixLength);
      node16.prefix = prefix;
      node16.count = count;
      return node16;
    }
    if (nodeTypeOrdinal == NodeType.NODE48.ordinal()) {
      Node48 node48 = new Node48(prefixLength);
      node48.prefix = prefix;
      node48.count = count;
      return node48;
    }
    if (nodeTypeOrdinal == NodeType.NODE256.ordinal()) {
      Node256 node256 = new Node256(prefixLength);
      node256.prefix = prefix;
      node256.count = count;
      return node256;
    }
    if (nodeTypeOrdinal == NodeType.LEAF_NODE.ordinal()) {
      return new LeafNode(0L, 0);
    }
    return null;
  }

  private static Node deserializeHeader(ByteBuffer byteBuffer) throws IOException {
    final int nodeTypeOrdinal = byteBuffer.get();
    final short count = byteBuffer.getShort();
    final byte prefixLength = byteBuffer.get();
    final byte[] prefix;
    if (prefixLength == 0) {
      prefix = Art.EMPTY_BYTES;
    } else {
      prefix = new byte[prefixLength];
      byteBuffer.get(prefix);
    }
    if (nodeTypeOrdinal == NodeType.NODE4.ordinal()) {
      Node4 node4 = new Node4(prefixLength);
      node4.prefix = prefix;
      node4.count = count;
      return node4;
    }
    if (nodeTypeOrdinal == NodeType.NODE16.ordinal()) {
      Node16 node16 = new Node16(prefixLength);
      node16.prefix = prefix;
      node16.count = count;
      return node16;
    }
    if (nodeTypeOrdinal == NodeType.NODE48.ordinal()) {
      Node48 node48 = new Node48(prefixLength);
      node48.prefix = prefix;
      node48.count = count;
      return node48;
    }
    if (nodeTypeOrdinal == NodeType.NODE256.ordinal()) {
      Node256 node256 = new Node256(prefixLength);
      node256.prefix = prefix;
      node256.count = count;
      return node256;
    }
    if (nodeTypeOrdinal == NodeType.LEAF_NODE.ordinal()) {
      return new LeafNode(0L, 0);
    }
    return null;
  }
}
