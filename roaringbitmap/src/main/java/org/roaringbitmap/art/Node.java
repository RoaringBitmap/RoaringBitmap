package org.roaringbitmap.art;

import org.roaringbitmap.longlong.HighLowContainer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class Node {

  public Node() {
  }

  @Override
  protected abstract Node clone();

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

  void serialize(DataOutput dataOutput, HighLowContainer highLow) throws IOException {
    if (this instanceof BranchNode) {
      BranchNode branchNode = (BranchNode) this;
      dataOutput.writeByte(branchNode.count-1);
    } else {
      dataOutput.writeByte(0);
    }
    serializeBody(dataOutput, highLow);
  }
  void serialize(ByteBuffer byteBuffer, HighLowContainer highLow) throws IOException {
    if (this instanceof BranchNode) {
      BranchNode branchNode = (BranchNode) this;
      byteBuffer.put((byte)(branchNode.count-1));
    } else {
      byteBuffer.put((byte)0);
    }
    serializeBody(byteBuffer, highLow);
  }

  static Node deserialize(DataInput dataInput, HighLowContainer highLow) throws IOException {
    byte sizeToken = dataInput.readByte();
    return (sizeToken == 0)
        ? LeafNode.deserializeBody(dataInput, highLow)
        : BranchNode.deserializeBody(dataInput, (sizeToken & 0xFF) + 1, highLow);
  }

  static Node deserialize(ByteBuffer byteBuffer, HighLowContainer highLow) throws IOException {
    byte sizeToken = byteBuffer.get();
    return (sizeToken == 0)
        ? LeafNode.deserializeBody(byteBuffer, highLow)
        : BranchNode.deserializeBody(byteBuffer, (sizeToken & 0xFF) + 1, highLow);
  }

  /**
   * serialize
   *
   * @param dataOutput the DataOutput
   * @throws IOException signal a exception happened while the serialization
   */
  abstract void serializeBody(DataOutput dataOutput, HighLowContainer highLow) throws IOException;

  /**
   * serialize
   *
   * @param byteBuffer the ByteBuffer
   * @param highLow
   * @throws IOException signal a exception happened while the serialization
   */
  abstract void serializeBody(ByteBuffer byteBuffer, HighLowContainer highLow) throws IOException;

  abstract long serializeSizeInBytes(HighLowContainer highLow);
}
