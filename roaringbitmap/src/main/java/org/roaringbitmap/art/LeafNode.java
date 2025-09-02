package org.roaringbitmap.art;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.RunContainer;
import org.roaringbitmap.longlong.HighLowContainer;
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
  public String toString() {
    return "LeafNode{" +
            "key=" + Long.toHexString(getKey()) +
            ", containerIdx=" + containerIdx +
            '}';
  }

  @Override
  long serializeSizeInBytes(HighLowContainer highLow) {
    Container container = highLow.getContainer(containerIdx);
    return 1 + // node type
            4 + 2 + // key
            1 + // container type;
            // container size adjustment as we are adjusting the body only (e.g. optional cardinality)
            containerType(container).sizeAdjustment +
            container.serializedSizeInBytes();
  }

  public static LeafNode deserializeBody(DataInput dataInput, HighLowContainer highLow) throws IOException {
    int keyHigh = Integer.reverseBytes(dataInput.readInt());
    char keyLow = Character.reverseBytes(dataInput.readChar());

    ContainerType containerType = ContainerType.fromOrdinal(dataInput.readByte());
    Container container;
    switch (containerType) {
      case RUN_CONTAINER:
        container = readRunContainer(dataInput);
        break;
      case BITMAP_CONTAINER:
        container = readBitmapContainer(dataInput);
        break;
      case ARRAY_CONTAINER:
        container = readArrayContainer(dataInput);
        break;
      default:
        // should not reach here
        throw new IllegalStateException("Unexpected containerType: " + containerType);
    }
    long key = (((long) keyHigh) & 0xFFFFFFFFL) << 32 | (((long) keyLow) & 0xFFFFL) << 16;
    return new LeafNode(key, highLow.addContainer(container));
  }

  public static LeafNode deserializeBody(ByteBuffer byteBuffer, HighLowContainer highLow) throws IOException {
    assert byteBuffer.order() == ByteOrder.LITTLE_ENDIAN;

    int keyHigh = byteBuffer.getInt();
    char keyLow = byteBuffer.getChar();

    ContainerType containerType = ContainerType.fromOrdinal(byteBuffer.get());
    Container container;
    switch (containerType) {
      case RUN_CONTAINER:
        container = readRunContainer(byteBuffer);
        break;
      case BITMAP_CONTAINER:
        container = readBitmapContainer(byteBuffer);
        break;
      case ARRAY_CONTAINER:
        container = readArrayContainer(byteBuffer);
        break;
      default:
        // should not reach here
        throw new IllegalStateException("Unexpected containerType: " + containerType);
    }
    long key = (((long) keyHigh) & 0xFFFFFFFFL) << 32 | (((long) keyLow) & 0xFFFFL) << 16;
    return new LeafNode(key, highLow.addContainer(container));
  }

  @Override
  void serializeBody(DataOutput dataOutput, HighLowContainer highLow) throws IOException {
    dataOutput.writeInt(Integer.reverseBytes(keyHigh));
    dataOutput.writeChar(Character.reverseBytes(keyLow));
    Container container = highLow.getContainer(containerIdx);

    ContainerType containerType = containerType(container);
    dataOutput.writeByte(containerType.ordinal());
    switch (containerType) {
      case RUN_CONTAINER:
        break;
      case BITMAP_CONTAINER:
      case ARRAY_CONTAINER:
        dataOutput.writeInt(Integer.reverseBytes( container.getCardinality()));
        break;
      default:
        // should not reach here
        throw new IllegalStateException("Unexpected containerType: " + containerType);
    }
    container.writeArray(dataOutput);
  }

  @Override
  void serializeBody(ByteBuffer byteBuffer, HighLowContainer highLow) throws IOException {
    assert byteBuffer.order() == ByteOrder.LITTLE_ENDIAN;
    Container container = highLow.getContainer(containerIdx);

    byteBuffer.putInt(keyHigh);
    byteBuffer.putChar(keyLow);

    ContainerType containerType = containerType(container);
    byteBuffer.put((byte) containerType.ordinal());
    switch (containerType) {
      case RUN_CONTAINER:
        break;
      case BITMAP_CONTAINER:
      case ARRAY_CONTAINER:
        byteBuffer.putInt(container.getCardinality());
        break;
      default:
        // should not reach here
        throw new IllegalStateException("Unexpected containerType: " + containerType);
    }
    container.writeArray(byteBuffer);
  }

  private static ContainerType containerType(Container container) {
    if (container instanceof RunContainer) {
      return ContainerType.RUN_CONTAINER;
    } else if (container instanceof BitmapContainer) {
      return ContainerType.BITMAP_CONTAINER;
    } else if (container instanceof ArrayContainer) {
      return ContainerType.ARRAY_CONTAINER;
    } else {
      throw new UnsupportedOperationException("Not supported container type");
    }
  }

  private static RunContainer readRunContainer( DataInput dataInput) throws IOException {
    int nbrruns = (Character.reverseBytes(dataInput.readChar()));
    final char[] lengthsAndValues = new char[2 * nbrruns];

    for (int j = 0; j < 2 * nbrruns; ++j) {
      lengthsAndValues[j] = Character.reverseBytes(dataInput.readChar());
    }
    return new RunContainer(lengthsAndValues, nbrruns);
  }
  private static RunContainer readRunContainer( ByteBuffer byteBuffer) throws IOException {
    int nbrruns = byteBuffer.getChar();
    final char[] lengthsAndValues = new char[2 * nbrruns];
    byteBuffer.asCharBuffer().get(lengthsAndValues);
    byteBuffer.position(byteBuffer.position() + lengthsAndValues.length * 2);
    return new RunContainer(lengthsAndValues, nbrruns);
  }

  private static BitmapContainer readBitmapContainer( DataInput dataInput) throws IOException {
    int cardinality = Integer.reverseBytes(dataInput.readInt());
    final long[] bitmapArray = new long[BitmapContainer.MAX_CAPACITY / 64];
    // little endian
    for (int l = 0; l < bitmapArray.length; ++l) {
      bitmapArray[l] = Long.reverseBytes(dataInput.readLong());
    }
    return new BitmapContainer(bitmapArray, cardinality);
  }
  private static BitmapContainer readBitmapContainer( ByteBuffer byteBuffer) throws IOException {
    int cardinality = byteBuffer.getInt();
    final long[] bitmapArray = new long[BitmapContainer.MAX_CAPACITY / 64];
    byteBuffer.asLongBuffer().get(bitmapArray);
    byteBuffer.position(byteBuffer.position() + bitmapArray.length * 8);
    return new BitmapContainer(bitmapArray, cardinality);
  }
  private static ArrayContainer readArrayContainer( DataInput dataInput) throws IOException {
    int cardinality = Integer.reverseBytes(dataInput.readInt());
    final char[] charArray = new char[cardinality];
    for (int l = 0; l < charArray.length; ++l) {
      charArray[l] = Character.reverseBytes(dataInput.readChar());
    }
    return new ArrayContainer(charArray);
  }
  private static ArrayContainer readArrayContainer( ByteBuffer byteBuffer) throws IOException {
    int cardinality = byteBuffer.getInt();
    final char[] charArray = new char[cardinality];
    byteBuffer.asCharBuffer().get(charArray);
    byteBuffer.position(byteBuffer.position() + charArray.length * 2);
    return new ArrayContainer(charArray);
  }

  private enum ContainerType {
    RUN_CONTAINER(0),
    BITMAP_CONTAINER(4),
    //ArrayContainer serialization includes a 2-byte cardinality, but we use 4
    ARRAY_CONTAINER(2);
    final int sizeAdjustment;
    ContainerType(int sizeAdjustment) {
      this.sizeAdjustment = sizeAdjustment;
    }

    private static final ContainerType[] VALUES = values();
    public static ContainerType fromOrdinal(byte b) {
      return VALUES[b];
    }
  }
}
