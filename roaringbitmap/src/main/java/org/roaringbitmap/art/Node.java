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

  void serialize(DataOutput dataOutput) throws IOException {
    if (this instanceof BranchNode) {
      BranchNode branchNode = (BranchNode) this;
      dataOutput.writeByte(branchNode.count-1);
    } else {
      dataOutput.writeByte(0);
    }
    serializeBody(dataOutput);
  }
  void serialize(ByteBuffer byteBuffer) throws IOException {
    if (this instanceof BranchNode) {
      BranchNode branchNode = (BranchNode) this;
      byteBuffer.put((byte)(branchNode.count-1));
    } else {
      byteBuffer.put((byte)0);
    }
    serializeBody(byteBuffer);
  }

  static Node deserialize(DataInput dataInput) throws IOException {
    byte sizeToken = dataInput.readByte();
    return (sizeToken == 0)
        ? LeafNode.deserializeBody(dataInput)
        : BranchNode.deserializeBody(dataInput, (sizeToken & 0xFF) + 1);
  }

  static Node deserialize(ByteBuffer byteBuffer) throws IOException {
    byte sizeToken = byteBuffer.get();
    return (sizeToken == 0)
        ? LeafNode.deserializeBody(byteBuffer)
        : BranchNode.deserializeBody(byteBuffer, (sizeToken & 0xFF) + 1);
  }

  /**
   * serialize
   *
   * @param dataOutput the DataOutput
   * @throws IOException signal a exception happened while the serialization
   */
  abstract void serializeBody(DataOutput dataOutput) throws IOException;

  /**
   * serialize
   *
   * @param byteBuffer the ByteBuffer
   * @throws IOException signal a exception happened while the serialization
   */
  abstract void serializeBody(ByteBuffer byteBuffer) throws IOException;

  abstract long serializeSizeInBytes();
}
