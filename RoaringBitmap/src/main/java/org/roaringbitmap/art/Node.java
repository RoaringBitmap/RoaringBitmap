package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class Node {

  //node type
  protected NodeType nodeType;
  //length of compressed path(prefix)
  protected byte prefixLength;
  //the compressed path path (prefix)
  protected byte[] prefix;
  //number of non-null children, the largest value will not beyond 255
  //to benefit calculation,we keep the value as a short type
  protected short count;
  public static final int ILLEGAL_IDX = -1;

  /**
   * constructor
   *
   * @param nodeType the node type
   * @param compressedPrefixSize the prefix byte array size,less than or equal to 6
   */
  public Node(NodeType nodeType, int compressedPrefixSize) {
    this.nodeType = nodeType;
    this.prefixLength = (byte) compressedPrefixSize;
    prefix = new byte[prefixLength];
    count = 0;
  }

  /**
   * get the position of a child corresponding to the input key 'k'
   * @param k a key value of the byte range
   * @return the child position corresponding to the key 'k'
   */
  public abstract int getChildPos(byte k);

  /**
   * get the corresponding key byte of the requested position
   * @param pos the position
   * @return the corresponding key byte
   */
  public abstract byte getChildKey(int pos);

  /**
   * get the child at the specified position in the node, the 'pos' range from 0 to count
   * @param pos the position
   * @return a Node corresponding to the input position
   */
  public abstract Node getChild(int pos);

  /**
   * replace the position child to the fresh one
   * @param pos the position
   * @param freshOne the fresh node to replace the old one
   */
  public abstract void replaceNode(int pos, Node freshOne);

  /**
   * get the position of the min element in current node.
   * @return the minimum key's position
   */
  public abstract int getMinPos();

  /**
   * get the next position in the node
   *
   * @param pos current position,-1 to start from the min one
   * @return the next larger byte key's position which is close to 'pos' position,-1 for end
   */
  public abstract int getNextLargerPos(int pos);

  /**
   * get the max child's position
   * @return the max byte key's position
   */
  public abstract int getMaxPos();

  /**
   * get the next smaller element's position
   * @param pos the position,-1 to start from the largest one
   * @return the next smaller key's position which is close to input 'pos' position,-1 for end
   */
  public abstract int getNextSmallerPos(int pos);

  /**
   * remove the specified position child
   * @param pos the position to remove
   * @return an adaptive changed fresh node of the current node
   */
  public abstract Node remove(int pos);

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
   * replace the node's children according to the given children parameter while doing the
   * deserialization phase.
   * @param children all the not null children nodes in key byte ascending order,no null element
   */
  abstract void replaceChildren(Node[] children);

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

  /**
   * insert the LeafNode as a child of the current internal node
   *
   * @param current current internal node
   * @param childNode the leaf node
   * @param key the key byte reference to the child leaf node
   * @return an adaptive changed node of the input 'current' node
   */
  public static Node insertLeaf(Node current, LeafNode childNode, byte key) {
    switch (current.nodeType) {
      case NODE4:
        return Node4.insert(current, childNode, key);
      case NODE16:
        return Node16.insert(current, childNode, key);
      case NODE48:
        return Node48.insert(current, childNode, key);
      case NODE256:
        return Node256.insert(current, childNode, key);
      default:
        throw new IllegalArgumentException("Not supported node type!");
    }
  }

  /**
   * copy the prefix between two nodes
   * @param src the source node
   * @param dst the destination node
   */
  public static void copyPrefix(Node src, Node dst) {
    dst.prefixLength = src.prefixLength;
    System.arraycopy(src.prefix, 0, dst.prefix, 0, src.prefixLength);
  }

  /**
   * search the position of the input byte key in the node's key byte array part
   *
   * @param key the input key byte array
   * @param fromIndex inclusive
   * @param toIndex exclusive
   * @param k the target key byte value
   * @return the array offset of the target input key 'k' or -1 to not found
   */
  public static int binarySearch(byte[] key, int fromIndex, int toIndex,
      byte k) {
    int inputUnsignedByte = Byte.toUnsignedInt(k);
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = Byte.toUnsignedInt(key[mid]);

      if (midVal < inputUnsignedByte) {
        low = mid + 1;
      } else if (midVal > inputUnsignedByte) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    // key not found.
    return ILLEGAL_IDX;
  }

  private void serializeHeader(DataOutput dataOutput) throws IOException {
    //first byte: node type
    dataOutput.writeByte((byte) this.nodeType.ordinal());
    //non null object count
    dataOutput.writeShort(Short.reverseBytes(this.count));
    dataOutput.writeByte(this.prefixLength);
    if (prefixLength > 0) {
      dataOutput.write(this.prefix, 0, this.prefixLength);
    }
  }

  private void serializeHeader(ByteBuffer byteBuffer) throws IOException {
    byteBuffer.put((byte) this.nodeType.ordinal());
    byteBuffer.putShort(this.count);
    byteBuffer.put(this.prefixLength);
    if (prefixLength > 0) {
      byteBuffer.put(this.prefix, 0, prefixLength);
    }
  }

  private int serializeHeaderSizeInBytes() {
    int size = 1 + 2 + 1;
    if (prefixLength > 0) {
      size = size + prefixLength;
    }
    return size;
  }

  private static Node deserializeHeader(DataInput dataInput) throws IOException {
    int nodeTypeOrdinal = dataInput.readByte();
    short count = Short.reverseBytes(dataInput.readShort());
    byte prefixLength = dataInput.readByte();
    byte[] prefix = new byte[0];
    if (prefixLength > 0) {
      prefix = new byte[prefixLength];
      dataInput.readFully(prefix);
    }
    if (nodeTypeOrdinal == NodeType.NODE4.ordinal()) {
      Node4 node4 = new Node4(prefixLength);
      node4.prefixLength = prefixLength;
      node4.prefix = prefix;
      node4.count = count;
      return node4;
    }
    if (nodeTypeOrdinal == NodeType.NODE16.ordinal()) {
      Node16 node16 = new Node16(prefixLength);
      node16.prefixLength = prefixLength;
      node16.prefix = prefix;
      node16.count = count;
      return node16;
    }
    if (nodeTypeOrdinal == NodeType.NODE48.ordinal()) {
      Node48 node48 = new Node48(prefixLength);
      node48.prefixLength = prefixLength;
      node48.prefix = prefix;
      node48.count = count;
      return node48;
    }
    if (nodeTypeOrdinal == NodeType.NODE256.ordinal()) {
      Node256 node256 = new Node256(prefixLength);
      node256.prefixLength = prefixLength;
      node256.prefix = prefix;
      node256.count = count;
      return node256;
    }
    if (nodeTypeOrdinal == NodeType.LEAF_NODE.ordinal()) {
      LeafNode leafNode = new LeafNode(0L, 0);
      leafNode.prefixLength = prefixLength;
      leafNode.prefix = prefix;
      leafNode.count = count;
      return leafNode;
    }
    return null;
  }

  private static Node deserializeHeader(ByteBuffer byteBuffer) throws IOException {
    int nodeTypeOrdinal = byteBuffer.get();
    short count = byteBuffer.getShort();
    byte prefixLength = byteBuffer.get();
    byte[] prefix = new byte[0];
    if (prefixLength > 0) {
      prefix = new byte[prefixLength];
      byteBuffer.get(prefix);
    }
    if (nodeTypeOrdinal == NodeType.NODE4.ordinal()) {
      Node4 node4 = new Node4(prefixLength);
      node4.prefixLength = prefixLength;
      node4.prefix = prefix;
      node4.count = count;
      return node4;
    }
    if (nodeTypeOrdinal == NodeType.NODE16.ordinal()) {
      Node16 node16 = new Node16(prefixLength);
      node16.prefixLength = prefixLength;
      node16.prefix = prefix;
      node16.count = count;
      return node16;
    }
    if (nodeTypeOrdinal == NodeType.NODE48.ordinal()) {
      Node48 node48 = new Node48(prefixLength);
      node48.prefixLength = prefixLength;
      node48.prefix = prefix;
      node48.count = count;
      return node48;
    }
    if (nodeTypeOrdinal == NodeType.NODE256.ordinal()) {
      Node256 node256 = new Node256(prefixLength);
      node256.prefixLength = prefixLength;
      node256.prefix = prefix;
      node256.count = count;
      return node256;
    }
    if (nodeTypeOrdinal == NodeType.LEAF_NODE.ordinal()) {
      LeafNode leafNode = new LeafNode(0L, 0);
      leafNode.prefixLength = prefixLength;
      leafNode.prefix = prefix;
      leafNode.count = count;
      return leafNode;
    }
    return null;
  }
}
