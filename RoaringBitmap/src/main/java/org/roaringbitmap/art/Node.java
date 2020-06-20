package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Node {

  //node type
  protected NodeType nodeType;
  //length of compressed path(prefix)
  protected int prefixLength;
  //the compressed path path (prefix)
  protected byte[] prefix;
  //number of non-null children
  protected int count;
  public static final int ILLEGAL_IDX = -1;

  /**
   * constructor
   * @param nodeType the node type
   * @param compressedPrefixSize the prefix byte array size
   */
  public Node(NodeType nodeType, int compressedPrefixSize) {
    this.nodeType = nodeType;
    this.prefixLength = compressedPrefixSize;
    prefix = new byte[prefixLength];
    count = 0;
  }

  /**
   * get the position of a child corresponding to the input key 'k'
   */
  public abstract int getChildPos(byte k);

  /**
   * get the child at the specified position in the node, the 'pos' range from 0 to count
   */
  public abstract Node getChild(int pos);

  /**
   * replace the position child to the fresh one
   */
  public abstract void replaceNode(int pos, Node freshOne);

  /**
   * get the position of the min element in current node.
   */
  public abstract int getMinPos();

  /**
   * get the next position in the node
   *
   * @param pos current position
   */
  public abstract int getNextLargerPos(int pos);

  /**
   * get the max child's position
   */
  public abstract int getMaxPos();

  /**
   * get the next smaller element's position
   */
  public abstract int getNextSmallerPos(int pos);

  /**
   * remove the specified position child
   *
   * @return an adaptive changed fresh node of the current node
   */
  public abstract Node remove(int pos);

  /**
   * serialize
   * @param dataOutput the DataOutput
   * @throws IOException signal a exception happened while the serialization
   */
  public void serialize(DataOutput dataOutput) throws IOException {
    serializeHeader(dataOutput);
    serializeNodeBody(dataOutput);
  }

  /**
   * the serialized size in bytes of this node
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
   * replace the node's children according to the given children parameter while doing the
   * deserialization phase.
   */
  public abstract void replaceChildren(Node[] children);

  /**
   * serialize the node's body content
   */
  public abstract void serializeNodeBody(DataOutput dataOutput) throws IOException;

  /**
   * deserialize the node's body content
   */
  public abstract void deserializeNodeBody(DataInput dataInput) throws IOException;

  /**
   * the serialized size except the common node header part
   * @return the size in bytes
   */
  public abstract int serializeNodeBodySizeInBytes();

  /**
   * find the position that the key and the node's prefix that begins to has a mismatch
   */
  public static int prefixMismatch(Node node, byte[] key, int depth) {
    int pos = 0;
    for (; pos < node.prefixLength; pos++) {
      if (key[depth + pos] != node.prefix[pos]) {
        return pos;
      }
    }
    return pos;
  }

  /**
   * insert the LeafNode as a child of the current internal node
   *
   * @param current current internal node
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
   */
  public static void copyPrefix(Node src, Node dst) {
    dst.prefixLength = src.prefixLength;
    System.arraycopy(src.prefix, 0, dst.prefix, 0, src.prefixLength);
  }

  /**
   * sort the key byte array of node4 or node16 type by the insertion sort algorithm.
   * @param node node14 or node16
   */
  public static void insertionSortOnNode4Or16(Node node) {
    if (node.nodeType == NodeType.NODE4) {
      Node4 node4 = (Node4) node;
      sortSmallByteArray(node4.key, node4.children, 0, node4.count - 1);
      return;
    }
    if (node.nodeType == NodeType.NODE16) {
      Node16 node16 = (Node16) node;
      sortSmallByteArray(node16.key, node16.children, 0, node16.count - 1);
      return;
    }
    throw new IllegalArgumentException();
  }

  /**
   * sort the small arrays through the insertion sort alg.
   */
  private static void sortSmallByteArray(byte[] key, Node[] children, int left, int right) {
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
  }

  /**
   * search the position of the input byte key in the node's
   * key byte array part
   * @param node node4 or node16
   * @param key the byte to search
   * @return the position of the node's key byte array or -1 indicating
   * not found
   */
  public static int binarySearch(Node node, byte key) {
    if (node.nodeType == NodeType.NODE4) {
      Node4 node4 = (Node4) node;
      return binarySearch(node4.key, 0, node4.count, key);
    }
    if (node.nodeType == NodeType.NODE16) {
      Node16 node16 = (Node16) node;
      return binarySearch(node16.key, 0, node16.count, key);
    }
    throw new IllegalArgumentException("Unsupported node type!");
  }

  private static int binarySearch(byte[] key, int fromIndex, int toIndex,
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
    dataOutput.writeInt(this.count);
    dataOutput.writeInt(this.prefixLength);
    if (prefixLength > 0) {
      dataOutput.write(this.prefix, 0, this.prefixLength);
    }
  }

  private int serializeHeaderSizeInBytes() {
    int size = 1 + 4 + 4;
    if (prefixLength > 0) {
      size = size + prefixLength;
    }
    return size;
  }

  private static Node deserializeHeader(DataInput dataInput) throws IOException {
    int nodeTypeOrdinal = dataInput.readByte();
    int count = dataInput.readInt();
    int prefixLength = dataInput.readInt();
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
      LeafNode leafNode = new LeafNode(new byte[0], 0);
      leafNode.prefixLength = prefixLength;
      leafNode.prefix = prefix;
      leafNode.count = count;
      return leafNode;
    }
    return null;
  }
}
