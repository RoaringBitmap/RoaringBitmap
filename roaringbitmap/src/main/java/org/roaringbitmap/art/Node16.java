package org.roaringbitmap.art;

import org.roaringbitmap.longlong.LongUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Node16 extends BranchNode {

  long firstV = 0L;
  long secondV = 0L;
  Node[] children = new Node[16];

  public Node16(int compressionLength) {
    super(compressionLength);
  }

  @Override
  protected NodeType nodeType() {
    return NodeType.NODE16;
  }

  @Override
  public int getChildPos(byte k) {
    byte[] firstBytes = LongUtils.toBDBytes(firstV);
    if (count <= 8) {
      return binarySearch(firstBytes, 0, count, k);
    } else {
      int pos = binarySearch(firstBytes, 0, 8, k);
      if (pos != ILLEGAL_IDX) {
        return pos;
      } else {
        byte[] secondBytes = LongUtils.toBDBytes(secondV);
        pos = binarySearch(secondBytes, 0, (count - 8), k);
        if (pos != ILLEGAL_IDX) {
          return 8 + pos;
        } else {
          return ILLEGAL_IDX;
        }
      }
    }
  }

  @Override
  public SearchResult getNearestChildPos(byte k) {
    byte[] firstBytes = LongUtils.toBDBytes(firstV);
    if (count <= 8) {
      return binarySearchWithResult(firstBytes, 0, count, k);
    } else {
      SearchResult firstResult = binarySearchWithResult(firstBytes, 0, 8, k);
      // given the values are "in order" if we found a match or a value larger than
      // the target we are done.
      if (firstResult.outcome == SearchResult.Outcome.FOUND || firstResult.hasNextLargerPos()) {
        return firstResult;
      } else {
        byte[] secondBytes = LongUtils.toBDBytes(secondV);
        SearchResult secondResult = binarySearchWithResult(secondBytes, 0, (count - 8), k);

        switch (secondResult.outcome) {
          case FOUND:
            return SearchResult.found(8 + secondResult.getKeyPos());
          case NOT_FOUND:
            int lowPos = secondResult.getNextSmallerPos();
            int highPos = secondResult.getNextLargerPos();
            // don't map -1 into the legal range by adding 8!
            if (lowPos >= 0) {
              lowPos += 8;
            }
            if (highPos >= 0) {
              highPos += 8;
            }

            if (firstResult.hasNextLargerPos() == false
                && secondResult.hasNextSmallerPos() == false) {
              // this happens when the result is in the gap of the two ranges, the correct
              // "smaller value" is that of first result.
              lowPos = firstResult.getNextSmallerPos();
            }

            return SearchResult.notFound(lowPos, highPos);

          default:
            throw new IllegalStateException("There only two possible search outcomes");
        }
      }
    }
  }

  @Override
  public byte getChildKey(int pos) {
    int posInLong;
    if (pos <= 7) {
      posInLong = pos;
      byte[] firstBytes = LongUtils.toBDBytes(firstV);
      return firstBytes[posInLong];
    } else {
      posInLong = pos - 8;
      byte[] secondBytes = LongUtils.toBDBytes(secondV);
      return secondBytes[posInLong];
    }
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
   * insert a child into the node with the key byte
   *
   * @param child the child node to be inserted
   * @param key the key byte
   * @return the adaptive changed node of the parent node16
   */
  @Override
  protected BranchNode insert(Node child, byte key) {
    if (this.count < 8) {
      // first
      byte[] bytes = LongUtils.toBDBytes(this.firstV);
      bytes[this.count] = key;
      this.children[this.count] = child;
      sortSmallByteArray(bytes, this.children, 0, this.count);
      this.count++;
      this.firstV = LongUtils.fromBDBytes(bytes);
      return this;
    } else if (this.count < 16) {
      // second
      ByteBuffer byteBuffer = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
      byteBuffer.putLong(this.firstV);
      byteBuffer.putLong(this.secondV);
      byteBuffer.put(this.count, key);
      this.children[this.count] = child;
      sortSmallByteArray(byteBuffer.array(), this.children, 0, this.count);
      this.count++;
      this.firstV = byteBuffer.getLong(0);
      this.secondV = byteBuffer.getLong(8);
      return this;
    } else {
      Node48 node48 = new Node48(this.prefixLength);
      for (int i = 0; i < 8; i++) {
        int unsignedIdx = Byte.toUnsignedInt((byte) (this.firstV >>> ((7 - i) << 3)));
        // i won't be beyond 48
        Node48.setOneByte(unsignedIdx, (byte) i, node48.childIndex);
        node48.children[i] = this.children[i];
      }
      byte[] secondBytes = LongUtils.toBDBytes(this.secondV);
      for (int i = 8; i < this.count; i++) {
        byte v = secondBytes[i - 8];
        int unsignedIdx = Byte.toUnsignedInt(v);
        // i won't be beyond 48
        Node48.setOneByte(unsignedIdx, (byte) i, node48.childIndex);
        node48.children[i] = this.children[i];
      }
      copyPrefix(this, node48);
      node48.count = this.count;
      BranchNode freshOne = node48.insert(child, key);
      return freshOne;
    }
  }

  @Override
  public Node remove(int pos) {
    children[pos] = null;
    ByteBuffer byteBuffer = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
    byte[] bytes = byteBuffer.putLong(firstV).putLong(secondV).array();
    System.arraycopy(bytes, pos + 1, bytes, pos, (16 - pos - 1));
    System.arraycopy(children, pos + 1, children, pos, (16 - pos - 1));
    firstV = byteBuffer.getLong(0);
    secondV = byteBuffer.getLong(8);
    count--;
    if (count <= 3) {
      // shrink to node4
      Node4 node4 = new Node4(prefixLength);
      // copy the keys
      node4.key = (int) (firstV >> 32);
      System.arraycopy(children, 0, node4.children, 0, count);
      node4.count = count;
      copyPrefix(this, node4);
      return node4;
    }
    return this;
  }

  @Override
  public void serializeNodeBody(DataOutput dataOutput) throws IOException {
    // little endian
    dataOutput.writeLong(Long.reverseBytes(firstV));
    dataOutput.writeLong(Long.reverseBytes(secondV));
  }

  @Override
  public void serializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    byteBuffer.putLong(firstV);
    byteBuffer.putLong(secondV);
  }

  @Override
  public void deserializeNodeBody(DataInput dataInput) throws IOException {
    firstV = Long.reverseBytes(dataInput.readLong());
    secondV = Long.reverseBytes(dataInput.readLong());
  }

  @Override
  public void deserializeNodeBody(ByteBuffer byteBuffer) throws IOException {
    this.firstV = byteBuffer.getLong();
    this.secondV = byteBuffer.getLong();
  }

  @Override
  public int serializeNodeBodySizeInBytes() {
    return 16;
  }

  @Override
  public void replaceChildren(Node[] children) {
    System.arraycopy(children, 0, this.children, 0, count);
  }
}
