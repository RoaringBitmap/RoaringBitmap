package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;

public class Node48 extends BranchNode {

  // the actual byte value of childIndex content won't be beyond 48
  // 256 bytes packed into longs
  static final int BYTES_PER_LONG = 8;
  static final int LONGS_USED = 256 / BYTES_PER_LONG;
  static final int INDEX_SHIFT = 3; // 2^3 == BYTES_PER_LONG
  static final int POS_MASK = 0x7; // the mask to access the pos in the long for the byte
  long[] childIndex = new long[LONGS_USED];
  Node[] children = new Node[48];
  static final byte EMPTY_VALUE = (byte) 0xFF;
  static final long INIT_LONG_VALUE = 0xFFffFFffFFffFFffL;

  public Node48(int compressedPrefixSize) {
    super(NodeType.NODE48, compressedPrefixSize);
    Arrays.fill(childIndex, INIT_LONG_VALUE);
  }

  @Override
  public int getChildPos(byte k) {
    int unsignedIdx = Byte.toUnsignedInt(k);
    int childIdx = childrenIdx(unsignedIdx, childIndex);
    if (childIdx != EMPTY_VALUE) {
      return unsignedIdx;
    }
    return ILLEGAL_IDX;
  }

  @Override
  public SearchResult getNearestChildPos(byte k) {
    int unsignedIdx = Byte.toUnsignedInt(k);
    int childIdx = childrenIdx(unsignedIdx, childIndex);
    if (childIdx != EMPTY_VALUE) {
      return SearchResult.found(unsignedIdx);
    }
    return SearchResult.notFound(getNextSmallerPos(unsignedIdx), getNextLargerPos(unsignedIdx));
  }

  @Override
  public byte getChildKey(int pos) {

    return (byte) pos;
  }

  @Override
  public Node getChild(int pos) {
    byte idx = childrenIdx(pos, childIndex);
    return children[(int) idx];
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    byte idx = childrenIdx(pos, childIndex);
    children[(int) idx] = freshOne;
  }

  @Override
  public int getMinPos() {
    int pos = 0;
    for (int i = 0; i < LONGS_USED; i++) {
      long longv = childIndex[i];
      if (longv == INIT_LONG_VALUE) {
        // skip over empty bytes
        pos += BYTES_PER_LONG;
        continue;
      } else {
        for (int j = 0; j < BYTES_PER_LONG; j++) {
          byte v = (byte) (longv >>> ((BYTES_PER_LONG - 1 - j) << INDEX_SHIFT));
          if (v != EMPTY_VALUE) {
            return pos;
          }
          pos++;
        }
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public int getNextLargerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      pos = -1;
    }
    pos++;
    int i = pos >>> INDEX_SHIFT;
    for (; i < LONGS_USED; i++) {
      long longv = childIndex[i];
      if (longv == INIT_LONG_VALUE) {
        // skip over empty bytes
        pos = (pos + BYTES_PER_LONG) & 0xF8;
        continue;
      }

      for (int j = pos & POS_MASK; j < BYTES_PER_LONG; j++) {
        int shiftNum = (BYTES_PER_LONG - 1 - j) << INDEX_SHIFT;
        byte v = (byte) (longv >>> shiftNum);
        if (v != EMPTY_VALUE) {
          return pos;
        }
        pos++;
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public int getMaxPos() {
    int pos = 255;
    for (int i = (LONGS_USED - 1); i >= 0; i--) {
      long longv = childIndex[i];
      if (longv == INIT_LONG_VALUE) {
        pos -= BYTES_PER_LONG;
        continue;
      } else {
        // the zeroth value is stored in the MSB, but because we are searching from high to low
        // across all bytes, we can avoid the "double negative" of starting at 7 and j-- to 0
        // and then shifting by (7-j)*8
        for (int j = 0; j < BYTES_PER_LONG; j++) {
          byte v = (byte) (longv >>> (j << INDEX_SHIFT));
          if (v != EMPTY_VALUE) {
            return pos;
          }
          pos--;
        }
      }
    }
    return ILLEGAL_IDX;
  }

  @Override
  public int getNextSmallerPos(int pos) {
    if (pos == ILLEGAL_IDX) {
      pos = 256;
    }
    pos--;
    int i = pos >>> INDEX_SHIFT;
    for (; i >= 0 && i < LONGS_USED; i--) {
      long longv = childIndex[i];
      if (longv == INIT_LONG_VALUE) {
        // skip over empty bytes
        pos -= Math.min(BYTES_PER_LONG, (pos & POS_MASK) + 1);
        continue;
      }
      // because we are starting potentially at non aligned location, we need to start at 7
      // (or less) and decrement to zero, and then unpack the long correctly.
      for (int j = pos & POS_MASK; j >= 0; j--) {
        int shiftNum = (BYTES_PER_LONG - 1 - j) << INDEX_SHIFT;
        byte v = (byte) (longv >>> shiftNum);
        if (v != EMPTY_VALUE) {
          return pos;
        }
        pos--;
      }
    }
    return ILLEGAL_IDX;
  }

  /**
   * insert a child node into the node48 node with the key byte
   *
   * @param currentNode the node4
   * @param child the child node
   * @param key the key byte
   * @return the node48 or an adaptive generated node256
   */
  public static BranchNode insert(BranchNode currentNode, Node child, byte key) {
    Node48 node48 = (Node48) currentNode;
    if (node48.count < 48) {
      // insert leaf node into current node
      int pos = node48.count;
      if (node48.children[pos] != null) {
        pos = 0;
        while (node48.children[pos] != null) {
          pos++;
        }
      }
      node48.children[pos] = child;
      int unsignedByte = Byte.toUnsignedInt(key);
      setOneByte(unsignedByte, (byte) pos, node48.childIndex);
      node48.count++;
      return node48;
    } else {
      // grow to Node256
      Node256 node256 = new Node256(node48.prefixLength);
      int currentPos = ILLEGAL_IDX;
      while ((currentPos = node48.getNextLargerPos(currentPos)) != ILLEGAL_IDX) {
        Node childNode = node48.getChild(currentPos);
        node256.children[currentPos] = childNode;
        Node256.setBit((byte) currentPos, node256.bitmapMask);
      }
      node256.count = node48.count;
      copyPrefix(node48, node256);
      BranchNode freshOne = Node256.insert(node256, child, key);
      return freshOne;
    }
  }

  @Override
  public Node remove(int pos) {
    byte idx = childrenIdx(pos, childIndex);
    setOneByte(pos, EMPTY_VALUE, childIndex);
    children[idx] = null;
    count--;
    if (count <= 12) {
      // shrink to node16
      Node16 node16 = new Node16(this.prefixLength);
      int j = 0;
      ByteBuffer byteBuffer = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
      int currentPos = ILLEGAL_IDX;
      while ((currentPos = getNextLargerPos(currentPos)) != ILLEGAL_IDX) {
        Node child = getChild(currentPos);
        byteBuffer.put(j, (byte) currentPos);
        node16.children[j] = child;
        j++;
      }
      node16.firstV = byteBuffer.getLong(0);
      node16.secondV = byteBuffer.getLong(8);
      node16.count = (short) j;
      copyPrefix(this, node16);
      return node16;
    }
    return this;
  }

  @Override
  public void serializeNodeBody(DataOutput dataOutput) throws IOException {
    for (int i = 0; i < LONGS_USED; i++) {
      long longv = childIndex[i];
      dataOutput.writeLong(Long.reverseBytes(longv));
    }
  }

  @Override
  public void serializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    LongBuffer longBuffer = byteBuffer.asLongBuffer();
    longBuffer.put(childIndex);
    byteBuffer.position(byteBuffer.position() + LONGS_USED * BYTES_PER_LONG);
  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {
    for (int i = 0; i < LONGS_USED; i++) {
      childIndex[i] = Long.reverseBytes(dataInput.readLong());
    }
  }

  @Override
  public void deserializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    LongBuffer longBuffer = byteBuffer.asLongBuffer();
    longBuffer.get(childIndex);
    byteBuffer.position(byteBuffer.position() + LONGS_USED * BYTES_PER_LONG);
  }

  @Override
  public int serializeNodeBodySizeInBytes() {
    return LONGS_USED * BYTES_PER_LONG;
  }

  @Override
  void replaceChildren(Node[] children) {
    int step = 0;
    for (int i = 0; i < LONGS_USED; i++) {
      long longv = Long.reverseBytes(childIndex[i]);
      if (longv != INIT_LONG_VALUE) {
        for (int j = 0; j < BYTES_PER_LONG; j++) {
          long currentByte = longv & 0xFF;
          if (currentByte != 0xFF) {
            this.children[(int) currentByte] = children[step];
            step++;
          }
          longv >>>= 8;
        }
      }
    }
  }

  private static byte childrenIdx(int pos, long[] childIndex) {
    int longPos = pos >>> INDEX_SHIFT;
    int bytePos = pos & POS_MASK;
    long longV = childIndex[longPos];
    byte idx = (byte) ((longV) >>> ((BYTES_PER_LONG - 1 - bytePos) << INDEX_SHIFT));
    return idx;
  }

  static void setOneByte(int pos, byte v, long[] childIndex) {
    final int longPos = pos >>> INDEX_SHIFT;
    final int bytePos = pos & POS_MASK;
    final int shift = (BYTES_PER_LONG - 1 - bytePos) << INDEX_SHIFT;
    final long preVal = childIndex[longPos];
    final long newVal = (preVal & ~(0xFFL << shift)) | (Byte.toUnsignedLong(v) << shift);
    childIndex[longPos] = newVal;
  }
}
