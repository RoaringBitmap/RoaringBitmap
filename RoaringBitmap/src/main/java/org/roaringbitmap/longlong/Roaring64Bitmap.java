package org.roaringbitmap.longlong;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.Objects;
import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.PeekableCharIterator;
import org.roaringbitmap.RunContainer;
import org.roaringbitmap.Util;
import org.roaringbitmap.art.ContainerIterator;
import org.roaringbitmap.art.KeyIterator;
import org.roaringbitmap.art.LeafNode;
import org.roaringbitmap.art.LeafNodeIterator;

/**
 * Roaring64Bitmap is a compressed 64 bit bitmap. It can contain all the numbers of long
 * rang[Long.MAX_VALUE, Long.MIN_VALUE]. Since java has no unsigned long,we treat the negative value
 * as a successor of the positive value. So the ascending ordering of all the long value is:
 * 0,1,2...Long.MAX_VALUE,Long.MIN_VALUE,Long.MIN_VALUE+1.......-1. See Long.toUnsignedString()
 */
public class Roaring64Bitmap implements Externalizable, LongBitmapDataProvider {

  private HighLowContainer highLowContainer;

  public Roaring64Bitmap() {
    highLowContainer = new HighLowContainer();
  }

  public void addInt(int x) {
    addLong(Util.toUnsignedLong(x));
  }

  /**
   * Add the value to the container (set the value to "true"), whether it already appears or not.
   *
   * Java lacks native unsigned longs but the x argument is considered to be unsigned. Within
   * bitmaps, numbers are ordered according to {@link Long#toUnsignedString}. We order the numbers
   * like 0, 1, ..., 9223372036854775807, -9223372036854775808, -9223372036854775807,..., -1.
   *
   * @param x long value
   */
  @Override
  public void addLong(long x) {
    byte[] high = LongUtils.highPart(x);
    char low = LongUtils.lowPart(x);
    ContainerWithIndex containerWithIndex = highLowContainer.searchContainer(high);
    if (containerWithIndex != null) {
      Container container = containerWithIndex.getContainer();
      Container freshOne = container.add(low);
      highLowContainer.replaceContainer(containerWithIndex.getContainerIdx(), freshOne);
    } else {
      ArrayContainer arrayContainer = new ArrayContainer();
      arrayContainer.add(low);
      highLowContainer.put(high, arrayContainer);
    }
  }

  /**
   * Returns the number of distinct integers added to the bitmap (e.g., number of bits set).
   *
   * @return the cardinality
   */
  @Override
  public long getLongCardinality() {
    if (highLowContainer.isEmpty()) {
      return 0L;
    }
    Iterator<Container> containerIterator = highLowContainer.containerIterator();
    long cardinality = 0L;
    while (containerIterator.hasNext()) {
      Container container = containerIterator.next();
      cardinality += container.getCardinality();
    }
    return cardinality;
  }

  /**
   * @return the cardinality as an int
   * @throws UnsupportedOperationException if the cardinality does not fit in an int
   */
  public int getIntCardinality() throws UnsupportedOperationException {
    long cardinality = getLongCardinality();
    if (cardinality > Integer.MAX_VALUE) {
      // TODO: we should handle cardinality fitting in an unsigned int
      throw new UnsupportedOperationException(
          "Can not call .getIntCardinality as the cardinality is bigger than Integer.MAX_VALUE");
    }
    return (int) cardinality;
  }

  /**
   * Return the jth value stored in this bitmap.
   *
   * @param j index of the value
   * @return the value
   * @throws IllegalArgumentException if j is out of the bounds of the bitmap cardinality
   */
  @Override
  public long select(final long j) throws IllegalArgumentException {
    long left = j;
    LeafNodeIterator leafNodeIterator = highLowContainer.highKeyLeafNodeIterator(false);
    while (leafNodeIterator.hasNext()) {
      LeafNode leafNode = leafNodeIterator.next();
      long containerIdx = leafNode.getContainerIdx();
      Container container = highLowContainer.getContainer(containerIdx);
      int card = container.getCardinality();
      if (left >= card) {
        left = left - card;
      } else {
        byte[] high = leafNode.getKeyBytes();
        int leftAsUnsignedInt = (int) left;
        char low = container.select(leftAsUnsignedInt);
        return LongUtils.toLong(high, low);
      }
    }
    return throwSelectInvalidIndex(j);
  }

  private long throwSelectInvalidIndex(long j) {
    throw new IllegalArgumentException(
        "select " + j + " when the cardinality is " + this.getLongCardinality());
  }

  /**
   * For better performance, consider the Use the {@link #forEach forEach} method.
   *
   * @return a custom iterator over set bits, the bits are traversed in ascending sorted order
   */
  public Iterator<Long> iterator() {
    final LongIterator it = getLongIterator();

    return new Iterator<Long>() {

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public Long next() {
        return it.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public void forEach(final LongConsumer lc) {
    KeyIterator keyIterator = highLowContainer.highKeyIterator();
    while (keyIterator.hasNext()) {
      byte[] high = keyIterator.next();
      long containerIdx = keyIterator.currentContainerIdx();
      Container container = highLowContainer.getContainer(containerIdx);
      PeekableCharIterator charIterator = container.getCharIterator();
      while (charIterator.hasNext()) {
        char low = charIterator.next();
        long v = LongUtils.toLong(high, low);
        lc.accept(v);
      }
    }
  }

  @Override
  public long rankLong(long id) {
    long result = 0;
    byte[] high = LongUtils.highPart(id);
    char low = LongUtils.lowPart(id);
    ContainerWithIndex containerWithIndex = highLowContainer.searchContainer(high);
    KeyIterator keyIterator = highLowContainer.highKeyIterator();
    if (containerWithIndex == null) {
      while (keyIterator.hasNext()) {
        byte[] highKey = keyIterator.next();
        int res = LongUtils.compareHigh(highKey, high);
        if (res > 0) {
          break;
        } else {
          long containerIdx = keyIterator.currentContainerIdx();
          Container container = highLowContainer.getContainer(containerIdx);
          result += container.getCardinality();
        }
      }
    } else {
      while (keyIterator.hasNext()) {
        byte[] key = keyIterator.next();
        long containerIdx = keyIterator.currentContainerIdx();
        Container container = highLowContainer.getContainer(containerIdx);
        if (LongUtils.compareHigh(key, high) == 0) {
          result += container.rank(low);
          break;
        } else {
          result += container.getCardinality();
        }
      }
    }
    return result;
  }

  /**
   * In-place bitwise OR (union) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void or(final Roaring64Bitmap x2) {
    KeyIterator highIte2 = x2.highLowContainer.highKeyIterator();
    while (highIte2.hasNext()) {
      byte[] high = highIte2.next();
      long containerIdx = highIte2.currentContainerIdx();
      Container container2 = x2.highLowContainer.getContainer(containerIdx);
      ContainerWithIndex containerWithIdx = this.highLowContainer.searchContainer(high);
      if (containerWithIdx == null) {
        Container container2clone = container2.clone();
        this.highLowContainer.put(high, container2clone);
      } else {
        Container freshContainer = containerWithIdx.getContainer().ior(container2);
        this.highLowContainer.replaceContainer(containerWithIdx.getContainerIdx(), freshContainer);
      }
    }
  }

  /**
   * In-place bitwise XOR (symmetric difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void xor(final Roaring64Bitmap x2) {
    KeyIterator keyIterator = x2.highLowContainer.highKeyIterator();
    while (keyIterator.hasNext()) {
      byte[] high = keyIterator.next();
      long containerIdx = keyIterator.currentContainerIdx();
      Container container = x2.highLowContainer.getContainer(containerIdx);
      ContainerWithIndex containerWithIndex = this.highLowContainer.searchContainer(high);
      if (containerWithIndex == null) {
        Container containerClone2 = container.clone();
        this.highLowContainer.put(high, containerClone2);
      } else {
        Container freshOne = containerWithIndex.getContainer().ixor(container);
        this.highLowContainer.replaceContainer(containerWithIndex.getContainerIdx(), freshOne);
      }
    }
  }

  /**
   * In-place bitwise AND (intersection) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void and(final Roaring64Bitmap x2) {
    KeyIterator thisIterator = highLowContainer.highKeyIterator();
    while (thisIterator.hasNext()) {
      byte[] highKey = thisIterator.next();
      long containerIdx = thisIterator.currentContainerIdx();
      ContainerWithIndex containerWithIdx = x2.highLowContainer.searchContainer(highKey);
      if (containerWithIdx == null) {
        thisIterator.remove();
      } else {
        Container container1 = highLowContainer.getContainer(containerIdx);
        Container freshContainer = container1.iand(containerWithIdx.getContainer());
        highLowContainer.replaceContainer(containerIdx, freshContainer);
      }
    }
  }


  /**
   * In-place bitwise ANDNOT (difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void andNot(final Roaring64Bitmap x2) {
    KeyIterator thisKeyIterator = highLowContainer.highKeyIterator();
    while (thisKeyIterator.hasNext()) {
      byte[] high = thisKeyIterator.next();
      long containerIdx = thisKeyIterator.currentContainerIdx();
      ContainerWithIndex containerWithIdx2 = x2.highLowContainer.searchContainer(high);
      if (containerWithIdx2 != null) {
        Container thisContainer = highLowContainer.getContainer(containerIdx);
        Container freshContainer = thisContainer.iandNot(containerWithIdx2.getContainer());
        highLowContainer.replaceContainer(containerIdx, freshContainer);
        if (!freshContainer.isEmpty()) {
          highLowContainer.replaceContainer(containerIdx, freshContainer);
        } else {
          thisKeyIterator.remove();
        }
      }
    }
  }

  /**
   * {@link Roaring64NavigableMap} are serializable. However, contrary to RoaringBitmap, the
   * serialization format is not well-defined: for now, it is strongly coupled with Java standard
   * serialization. Just like the serialization may be incompatible between various Java versions,
   * {@link Roaring64NavigableMap} are subject to incompatibilities. Moreover, even on a given Java
   * versions, the serialization format may change from one RoaringBitmap version to another
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    serialize(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    deserialize(in);
  }

  /**
   * A string describing the bitmap.
   *
   * @return the string
   */
  @Override
  public String toString() {
    final StringBuilder answer = new StringBuilder();
    final LongIterator i = this.getLongIterator();
    answer.append("{");
    if (i.hasNext()) {
      answer.append(i.next());
    }
    while (i.hasNext()) {
      answer.append(",");
      // to avoid using too much memory, we limit the size
      if (answer.length() > 0x80000) {
        answer.append("...");
        break;
      }
      answer.append(i.next());
    }
    answer.append("}");
    return answer.toString();
  }


  /**
   * For better performance, consider the Use the {@link #forEach forEach} method.
   *
   * @return a custom iterator over set bits, the bits are traversed in ascending sorted order
   */
  @Override
  public PeekableLongIterator getLongIterator() {
    LeafNodeIterator leafNodeIterator = highLowContainer.highKeyLeafNodeIterator(false);
    return new ForwardPeekableIterator(leafNodeIterator);
  }

  @Override
  public boolean contains(long x) {
    byte[] high = LongUtils.highPart(x);
    ContainerWithIndex containerWithIdx = highLowContainer.searchContainer(high);
    if (containerWithIdx == null) {
      return false;
    }
    char low = LongUtils.lowPart(x);
    return containerWithIdx.getContainer().contains(low);
  }

  @Override
  public int getSizeInBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongSizeInBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    return getLongCardinality() == 0L;
  }

  @Override
  public ImmutableLongBitmapDataProvider limit(long x) {
    throw new UnsupportedOperationException("TODO");
  }

  /**
   * Use a run-length encoding where it is estimated as more space efficient
   *
   * @return whether a change was applied
   */
  public boolean runOptimize() {
    boolean hasChanged = false;
    ContainerIterator containerIterator = highLowContainer.containerIterator();
    while (containerIterator.hasNext()) {
      Container container = containerIterator.next();
      Container freshContainer = container.runOptimize();
      if (freshContainer instanceof RunContainer) {
        hasChanged = true;
        containerIterator.replace(freshContainer);
      }
    }
    return hasChanged;
  }


  /**
   * Serialize this bitmap.
   *
   * Unlike RoaringBitmap, there is no specification for now: it may change from one java version to
   * another, and from one RoaringBitmap version to another.
   *
   * Consider calling {@link #runOptimize} before serialization to improve compression.
   *
   * The current bitmap is not modified.
   *
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Override
  public void serialize(DataOutput out) throws IOException {
    highLowContainer.serialize(out);
  }

  /**
   * Serialize this bitmap, please make sure the size of the serialized bytes is
   * smaller enough that ByteBuffer can hold it.
   * @param byteBuffer the ByteBuffer
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void serialize(ByteBuffer byteBuffer) throws IOException {
    highLowContainer.serialize(byteBuffer);
  }

  /**
   * Deserialize (retrieve) this bitmap.
   *
   * Unlike RoaringBitmap, there is no specification for now: it may change from one java version to
   * another, and from one RoaringBitmap version to another.
   *
   * The current bitmap is overwritten.
   *
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserialize(DataInput in) throws IOException {
    this.clear();
    highLowContainer.deserialize(in);
  }

  /**
   * Deserialize (retrieve) this bitmap.
   *
   * Unlike RoaringBitmap, there is no specification for now: it may change from one java version to
   * another, and from one RoaringBitmap version to another.
   *
   * The current bitmap is overwritten.
   *
   * @param in the ByteBuffer stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserialize(ByteBuffer in) throws IOException {
    this.clear();
    highLowContainer.deserialize(in);
  }

  @Override
  public long serializedSizeInBytes() {
    long nbBytes = highLowContainer.serializedSizeInBytes();
    return nbBytes;
  }

  /**
   * reset to an empty bitmap; result occupies as much space a newly created bitmap.
   */
  public void clear() {
    this.highLowContainer.clear();
  }

  /**
   * Return the set values as an array, if the cardinality is smaller than 2147483648. The long
   * values are in sorted order.
   *
   * @return array representing the set values.
   */
  @Override
  public long[] toArray() {
    long cardinality = this.getLongCardinality();
    if (cardinality > Integer.MAX_VALUE) {
      throw new IllegalStateException("The cardinality does not fit in an array");
    }

    final long[] array = new long[(int) cardinality];

    int pos = 0;
    LongIterator it = getLongIterator();

    while (it.hasNext()) {
      array[pos++] = it.next();
    }
    return array;
  }

  /**
   * Generate a bitmap with the specified values set to true. The provided longs values don't have
   * to be in sorted order, but it may be preferable to sort them from a performance point of view.
   *
   * @param dat set values
   * @return a new bitmap
   */
  public static Roaring64Bitmap bitmapOf(final long... dat) {
    final Roaring64Bitmap ans = new Roaring64Bitmap();
    ans.add(dat);
    return ans;
  }

  /**
   * Set all the specified values to true. This can be expected to be slightly faster than calling
   * "add" repeatedly. The provided integers values don't have to be in sorted order, but it may be
   * preferable to sort them from a performance point of view.
   *
   * @param dat set values
   */
  public void add(long... dat) {
    for (long oneLong : dat) {
      addLong(oneLong);
    }
  }

  /**
   * Add to the current bitmap all longs in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   */
  public void add(final long rangeStart, final long rangeEnd) {
    if (rangeEnd == 0 || Long.compareUnsigned(rangeStart, rangeEnd) >= 0) {
      throw new IllegalArgumentException("Invalid range [" + rangeStart + "," + rangeEnd + ")");
    }

    byte[] startHigh = LongUtils.highPart(rangeStart);
    int startLow = LongUtils.lowPart(rangeStart);
    byte[] endHigh = LongUtils.highPart(rangeEnd - 1);
    int endLow = LongUtils.lowPart(rangeEnd - 1);
    long rangeStartVal = rangeStart;
    byte[] startHighKey = startHigh;
    for (; LongUtils.compareHigh(startHighKey, endHigh) <= 0; ) {
      final int containerStart =
          (LongUtils.compareHigh(startHighKey, startHigh) == 0) ? startLow : 0;
      // last container may contain partial range
      final int containerLast = (LongUtils.compareHigh(startHighKey, endHigh) == 0) ? endLow
          : Util.maxLowBitAsInteger();
      ContainerWithIndex containerWithIndex = highLowContainer.searchContainer(startHighKey);
      if (containerWithIndex != null) {
        long containerIdx = containerWithIndex.getContainerIdx();
        Container freshContainer = highLowContainer.getContainer(containerIdx)
            .iadd(containerStart, containerLast + 1);
        highLowContainer.replaceContainer(containerIdx, freshContainer);
      } else {
        Container freshContainer = Container.rangeOfOnes(containerStart, containerLast + 1);
        highLowContainer.put(startHighKey, freshContainer);
      }
      //increase the high
      rangeStartVal = rangeStartVal + (containerLast - containerStart) + 1;
      startHighKey = LongUtils.highPart(rangeStartVal);
    }
  }

  @Override
  public PeekableLongIterator getReverseLongIterator() {
    LeafNodeIterator leafNodeIterator = highLowContainer.highKeyLeafNodeIterator(true);
    return new ReversePeekableIterator(leafNodeIterator);
  }

  @Override
  public void removeLong(long x) {
    byte[] high = LongUtils.highPart(x);
    ContainerWithIndex containerWithIdx = highLowContainer.searchContainer(high);
    if (containerWithIdx != null) {
      char low = LongUtils.lowPart(x);
      Container container = containerWithIdx.getContainer();
      Container freshContainer = container.remove(low);
      if (freshContainer.isEmpty()) {
        // Attempt to remove empty container to save memory
        highLowContainer.remove(high);
      } else {
        highLowContainer.replaceContainer(containerWithIdx.getContainerIdx(), freshContainer);
      }
    }
  }

  /**
   * remove the allocated unused memory space
   */
  @Override
  public void trim() {
    if (highLowContainer.isEmpty()) {
      return;
    }
    KeyIterator keyIterator = highLowContainer.highKeyIterator();
    while (keyIterator.hasNext()) {
      long containerIdx = keyIterator.currentContainerIdx();
      Container container = highLowContainer.getContainer(containerIdx);
      if (container.isEmpty()) {
        keyIterator.remove();
      } else {
        //TODO
        container.trim();
      }
    }
  }

  @Override
  public int hashCode() {
    return highLowContainer.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Roaring64Bitmap other = (Roaring64Bitmap) obj;
    return Objects.equals(highLowContainer, other.highLowContainer);
  }


  /**
   * Add the value if it is not already present, otherwise remove it.
   *
   * @param x long value
   */
  public void flip(final long x) {
    byte[] high = LongUtils.highPart(x);
    ContainerWithIndex containerWithIndex = highLowContainer.searchContainer(high);
    if (containerWithIndex == null) {
      addLong(x);
    } else {
      char low = LongUtils.lowPart(x);
      Container freshOne = containerWithIndex.getContainer().flip(low);
      highLowContainer.replaceContainer(containerWithIndex.getContainerIdx(), freshOne);
    }
  }

  //mainly used for benchmark
  @Override
  public Roaring64Bitmap clone() {
    long sizeInBytesL = this.serializedSizeInBytes();
    if (sizeInBytesL >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException();
    }
    int sizeInBytesInt = (int) sizeInBytesL;
    ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInBytesInt).order(ByteOrder.LITTLE_ENDIAN);
    try {
      this.serialize(byteBuffer);
      byteBuffer.flip();
      Roaring64Bitmap freshOne = new Roaring64Bitmap();
      freshOne.deserialize(byteBuffer);
      return freshOne;
    } catch (Exception e) {
      throw new RuntimeException("fail to clone thorough the ser/deser", e);
    }
  }


  private abstract class PeekableIterator implements PeekableLongIterator {
    private final LeafNodeIterator keyIte;
    private byte[] high;
    private PeekableCharIterator charIterator;
    private boolean hasNextCalled = false;

    PeekableIterator(final LeafNodeIterator keyIte) {
      this.keyIte = keyIte;
    }
    
    abstract PeekableCharIterator getIterator(Container container);
    abstract boolean compare(long next, long val);

    @Override
    public boolean hasNext() {
      hasNextCalled = true;
      if (charIterator != null && !charIterator.hasNext()) {
        while (keyIte.hasNext()) {
          LeafNode leafNode = keyIte.next();
          high = leafNode.getKeyBytes();
          long containerIdx = leafNode.getContainerIdx();
          Container container = highLowContainer.getContainer(containerIdx);
          charIterator = getIterator(container);
          if(charIterator.hasNext()){
            return true;
          }
        }
        return false;
      }
      if (charIterator != null && charIterator.hasNext()) {
        return true;
      }
      if (charIterator == null) {
        while (keyIte.hasNext()) {
          LeafNode leafNode = keyIte.next();
          high = leafNode.getKeyBytes();
          long containerIdx = leafNode.getContainerIdx();
          Container container = highLowContainer.getContainer(containerIdx);
          charIterator = getIterator(container);
          if(charIterator.hasNext()){
            return true;
          }

        }
        return false;

      }
      return false;
    }

    @Override
    public long next() {
      boolean hasNext = true;
      if (!hasNextCalled) {
        hasNext = hasNext();
        hasNextCalled = false;
      }
      if (hasNext) {
        char low = charIterator.next();
        hasNextCalled = false;
        return LongUtils.toLong(high, low);
      } else {
        throw new IllegalStateException("empty");
      }
    }

    @Override
    public void advanceIfNeeded(long minval) {
      if((hasNextCalled || this.hasNext()) && compare(this.peekNext(), minval)) {
        return;
      }
      //empty bitset
      if(this.high == null) {
        return;
      }
      byte[] high = LongUtils.highPart(minval);

      //need to advance the iterator
      while(LongUtils.compareHigh(this.high, high) != 0 && keyIte.hasNext()) {
        LeafNode leafNode = keyIte.next();
        this.high = leafNode.getKeyBytes();
        if(LongUtils.compareHigh(this.high, high) == 0) {
          long containerIdx = leafNode.getContainerIdx();
          Container container = highLowContainer.getContainer(containerIdx);
          charIterator = getIterator(container);
          if(!charIterator.hasNext()){
            return;
          }  
        }
      }

      char low = LongUtils.lowPart(minval);
      charIterator.advanceIfNeeded(low);
    }

    @Override
    public long peekNext() {
      boolean hasNext = true;
      if (!hasNextCalled) {
        hasNext = hasNext();
        hasNextCalled = true;
      }
      if (hasNext) {
        char low = charIterator.peekNext();
        return LongUtils.toLong(high, low);
      } else {
        throw new IllegalStateException("empty");
      }
    }

    @Override
    public PeekableLongIterator clone() {
      throw new UnsupportedOperationException("TODO");
    }
  }


  private class ForwardPeekableIterator extends PeekableIterator {

    public ForwardPeekableIterator(final LeafNodeIterator keyIte) {
      super(keyIte);
    }
    
    @Override
    PeekableCharIterator getIterator(Container container) {
      return container.getCharIterator();
    }
    
    @Override
    boolean compare(long next, long val) {
      return next >= val;
    }
  }

  private class ReversePeekableIterator extends PeekableIterator {
    public ReversePeekableIterator(final LeafNodeIterator keyIte) {
      super(keyIte);
    }
    
    @Override
    PeekableCharIterator getIterator(Container container) {
      return container.getReverseCharIterator();
    }
    
    @Override
    boolean compare(long next, long val) {
      return next <= val;
    }
  }
}
