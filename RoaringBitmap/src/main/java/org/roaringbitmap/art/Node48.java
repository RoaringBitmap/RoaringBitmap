package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;
import org.roaringbitmap.longlong.LongUtils;

public class Node48 extends Node {

  //the actual byte value of childIndex content won't be beyond 48
  long[] childIndex = new long[32];
  Node[] children = new Node[48];
  static final byte EMPTY_VALUE = -1;
  static final long INIT_LONG_VALUE = -1L;

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
    for (int i = 0; i < 32; i++) {
      long longv = childIndex[i];
      if (longv == INIT_LONG_VALUE) {
        //skip over empty bytes
        pos += 8;
        continue;
      } else {
        for (int j = 0; j <= 7; j++) {
          byte v = (byte) (longv >>> ((7 - j) * 8));
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
    int i = pos >>> 3;
    int offset = pos & (8 - 1);
    for (; i < 32; i++) {
      long longv = childIndex[i];
      if (offset == 0) {
        if (longv == INIT_LONG_VALUE) {
          //skip over empty bytes
          pos += 8;
          continue;
        }
      }
      for (int j = offset; j <= 7; j++) {
        int shiftNum = (7 - j) * 8;
        byte v = (byte) (longv >>> shiftNum);
        if (v != EMPTY_VALUE) {
          return pos;
        }
        pos++;
      }
      offset = 0;
    }
    return ILLEGAL_IDX;
  }

  @Override
  public int getMaxPos() {
    int pos = 255;
    for (int i = 31; i >= 0; i--) {
      long longv = childIndex[i];
      if (longv == INIT_LONG_VALUE) {
        pos -= 8;
        continue;
      } else {
        for (int j = 0; j <= 7; j++) {
          byte v = (byte) (longv >>> j * 8);
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
    int i = pos >>> 3;
    int offset = pos & (8 - 1);
    for (; i < 32; i++) {
      long longv = childIndex[i];
      if (offset == 0) {
        if (longv == INIT_LONG_VALUE) {
          //skip over empty bytes
          pos -= 8;
          continue;
        }
      }
      for (int j = offset; j <= 7; j++) {
        int shiftNum = j * 8;
        byte v = (byte) (longv >>> shiftNum);
        if (v != EMPTY_VALUE) {
          return pos;
        }
        pos--;
      }
      offset = 0;
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
  public static Node insert(Node currentNode, Node child, byte key) {
    Node48 node48 = (Node48) currentNode;
    if (node48.count < 48) {
      //insert leaf node into current node
      int pos = node48.count;
      if (node48.children[pos] != null) {
        pos = 0;
        while (node48.children[pos] != null) {
          pos++;
        }
      }
      node48.children[pos] = child;
      int unsignedByte = Byte.toUnsignedInt(key);
      int longPosition = unsignedByte >>> 3;
      int bytePosition = unsignedByte & (8 - 1);
      long original = node48.childIndex[longPosition];
      byte[] bytes = LongUtils.toBDBytes(original);
      bytes[bytePosition] = (byte) pos;
      node48.childIndex[longPosition] = LongUtils.fromBDBytes(bytes);
      node48.count++;
      return node48;
    } else {
      //grow to Node256
      Node256 node256 = new Node256(node48.prefixLength);
      int currentPos = ILLEGAL_IDX;
      while ((currentPos = node48.getNextLargerPos(currentPos)) != ILLEGAL_IDX) {
        Node childNode = node48.getChild(currentPos);
        node256.children[currentPos] = childNode;
        Node256.setBit((byte) currentPos, node256.bitmapMask);
      }
      node256.count = node48.count;
      copyPrefix(node48, node256);
      Node freshOne = Node256.insert(node256, child, key);
      return freshOne;
    }
  }

  @Override
  public Node remove(int pos) {
    int longPos = pos >>> 3;
    int bytePos = pos & (8 - 1);
    long longVal = childIndex[longPos];
    byte idx = (byte) ((longVal) >>> (7 - bytePos) * 8);
    byte[] bytes = LongUtils.toBDBytes(longVal);
    bytes[bytePos] = EMPTY_VALUE;
    long newLong = LongUtils.fromBDBytes(bytes);
    childIndex[longPos] = newLong;
    children[idx] = null;
    count--;
    if (count <= 12) {
      //shrink to node16
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
    for (int i = 0; i < 32; i++) {
      long longv = childIndex[i];
      dataOutput.writeLong(Long.reverseBytes(longv));
    }
  }

  @Override
  public void serializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    LongBuffer longBuffer = byteBuffer.asLongBuffer();
    longBuffer.put(childIndex);
    byteBuffer.position(byteBuffer.position() + 32 * 8);
  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {
    for (int i = 0; i < 32; i++) {
      childIndex[i] = Long.reverseBytes(dataInput.readLong());
    }
  }

  @Override
  public void deserializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    LongBuffer longBuffer = byteBuffer.asLongBuffer();
    longBuffer.get(childIndex);
    byteBuffer.position(byteBuffer.position() + 32 * 8);
  }

  @Override
  public int serializeNodeBodySizeInBytes() {
    return 256;
  }

  @Override
  public void replaceChildren(Node[] children) {
    int step = 0;
    for (int i = 0; i < 32; i++) {
      long longv = childIndex[i];
      for (int j = 7; j >= 0; j--) {
        byte bytePos = (byte) (longv >>> (j * 8));
        int unsignedPos = Byte.toUnsignedInt(bytePos);
        if (bytePos != EMPTY_VALUE) {
          this.children[unsignedPos] = children[step];
          step++;
        }
      }
    }
  }

  private static byte childrenIdx(int pos, long[] childIndex) {
    int longPos = pos >>> 3;
    int bytePos = pos & (8 - 1);
    long longv = childIndex[longPos];
    byte idx = (byte) ((longv) >>> (7 - bytePos) * 8);
    return idx;
  }

  static void setOneByte(int pos, byte v, long[] childIndex) {
    int longPos = pos >>> 3;
    int bytePos = pos & (8 - 1);
    long preVal = childIndex[longPos];
    byte[] bytes = LongUtils.toBDBytes(preVal);
    bytes[bytePos] = v;
    childIndex[longPos] = LongUtils.fromBDBytes(bytes);
  }
}
