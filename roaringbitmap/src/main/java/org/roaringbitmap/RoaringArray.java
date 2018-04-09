/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;


import java.io.*;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.roaringbitmap.Util.compareUnsigned;


/**
 * Specialized array to store the containers used by a RoaringBitmap. This is not meant to be used
 * by end users.
 */
public final class RoaringArray implements Cloneable, Externalizable {
  protected static final short SERIAL_COOKIE_NO_RUNCONTAINER = 12346;
  protected static final short SERIAL_COOKIE = 12347;
  protected static final int NO_OFFSET_THRESHOLD = 4;

  // bumped serialVersionUID with runcontainers, so default serialization
  // will not work...
  private static final long serialVersionUID = 8L;

  static final int INITIAL_CAPACITY = 4;


  short[] keys = null;

  Container[] values = null;

  int size = 0;

  protected RoaringArray() {
    this(new short[INITIAL_CAPACITY], new Container[INITIAL_CAPACITY], 0);
  }


  RoaringArray(short[] keys, Container[] values, int size) {
    this.keys = keys;
    this.values = values;
    this.size = size;
  }

  /**
   * Find the smallest integer index larger than pos such that array[index].key&gt;=x. If none can
   * be found, return size. Based on code by O. Kaser.
   *
   * @param x minimal value
   * @param pos index to exceed
   * @return the smallest index greater than pos such that array[index].key is at least as large as
   *         min, or size if it is not possible.
   */
  protected int advanceUntil(short x, int pos) {
    int lower = pos + 1;

    // special handling for a possibly common sequential case
    if (lower >= size || Util.toIntUnsigned(keys[lower]) >= Util.toIntUnsigned(x)) {
      return lower;
    }

    int spansize = 1; // could set larger
    // bootstrap an upper limit

    while (lower + spansize < size
        && Util.toIntUnsigned(keys[lower + spansize]) < Util.toIntUnsigned(x)) {
      spansize *= 2; // hoping for compiler will reduce to shift
    }
    int upper = (lower + spansize < size) ? lower + spansize : size - 1;

    // maybe we are lucky (could be common case when the seek ahead
    // expected to be small and sequential will otherwise make us look bad)
    if (keys[upper] == x) {
      return upper;
    }

    if (Util.toIntUnsigned(keys[upper]) < Util.toIntUnsigned(x)) {// means array has no item key >=
                                                                  // x
      return size;
    }

    // we know that the next-smallest span was too small
    lower += (spansize / 2);

    // else begin binary search
    // invariant: array[lower]<x && array[upper]>x
    while (lower + 1 != upper) {
      int mid = (lower + upper) / 2;
      if (keys[mid] == x) {
        return mid;
      } else if (Util.toIntUnsigned(keys[mid]) < Util.toIntUnsigned(x)) {
        lower = mid;
      } else {
        upper = mid;
      }
    }
    return upper;
  }

  protected void append(short key, Container value) {
    extendArray(1);
    this.keys[this.size] = key;
    this.values[this.size] = value;
    this.size++;
  }

  void append(RoaringArray roaringArray) {
    assert size == 0 || roaringArray.size == 0
            || compareUnsigned(keys[size - 1], roaringArray.keys[0]) < 0;
    if (roaringArray.size != 0 && size != 0) {
      keys = Arrays.copyOf(keys, size + roaringArray.size);
      values = Arrays.copyOf(values, size + roaringArray.size);
      System.arraycopy(roaringArray.keys, 0, keys, size, roaringArray.size);
      System.arraycopy(roaringArray.values, 0, values, size, roaringArray.size);
      size += roaringArray.size;
    } else if (size == 0 && roaringArray.size != 0) {
      keys = Arrays.copyOf(roaringArray.keys, roaringArray.keys.length);
      values = Arrays.copyOf(roaringArray.values, roaringArray.values.length);
      size = roaringArray.size;
    }
  }

  /**
   * Append copies of the values AFTER a specified key (may or may not be present) to end.
   *
   * @param sa other array
   * @param beforeStart given key is the largest key that we won't copy
   */
  protected void appendCopiesAfter(RoaringArray sa, short beforeStart) {
    int startLocation = sa.getIndex(beforeStart);
    if (startLocation >= 0) {
      startLocation++;
    } else {
      startLocation = -startLocation - 1;
    }
    extendArray(sa.size - startLocation);

    for (int i = startLocation; i < sa.size; ++i) {
      this.keys[this.size] = sa.keys[i];
      this.values[this.size] = sa.values[i].clone();
      this.size++;
    }
  }

  /**
   * Append copies of the values from another array, from the start
   *
   * @param sourceArray The array to copy from
   * @param stoppingKey any equal or larger key in other array will terminate copying
   */
  protected void appendCopiesUntil(RoaringArray sourceArray, short stoppingKey) {
    int stopKey = Util.toIntUnsigned(stoppingKey);
    for (int i = 0; i < sourceArray.size; ++i) {
      if (Util.toIntUnsigned(sourceArray.keys[i]) >= stopKey) {
        break;
      }
      extendArray(1);
      this.keys[this.size] = sourceArray.keys[i];
      this.values[this.size] = sourceArray.values[i].clone();
      this.size++;
    }
  }

  /**
   * Append copy of the one value from another array
   *
   * @param sa other array
   * @param index index in the other array
   */
  protected void appendCopy(RoaringArray sa, int index) {
    extendArray(1);
    this.keys[this.size] = sa.keys[index];
    this.values[this.size] = sa.values[index].clone();
    this.size++;
  }

  /**
   * Append copies of the values from another array
   *
   * @param sa other array
   * @param startingIndex starting index in the other array
   * @param end endingIndex (exclusive) in the other array
   */
  protected void appendCopy(RoaringArray sa, int startingIndex, int end) {
    extendArray(end - startingIndex);
    for (int i = startingIndex; i < end; ++i) {
      this.keys[this.size] = sa.keys[i];
      this.values[this.size] = sa.values[i].clone();
      this.size++;
    }
  }



  /**
   * Append the values from another array, no copy is made (use with care)
   *
   * @param sa other array
   * @param startingIndex starting index in the other array
   * @param end endingIndex (exclusive) in the other array
   */
  protected void append(RoaringArray sa, int startingIndex, int end) {
    extendArray(end - startingIndex);
    for (int i = startingIndex; i < end; ++i) {
      this.keys[this.size] = sa.keys[i];
      this.values[this.size] = sa.values[i];
      this.size++;
    }
  }


  private int binarySearch(int begin, int end, short key) {
    return Util.unsignedBinarySearch(keys, begin, end, key);
  }

  protected void clear() {
    this.keys = null;
    this.values = null;
    this.size = 0;
  }

  /**
   * If possible, recover wasted memory.
   */
  public void trim() {
    keys = Arrays.copyOf(keys, size);
    values = Arrays.copyOf(values, size);
    for (Container c : values) {
      c.trim();
    }
  }

  @Override
  public RoaringArray clone() throws CloneNotSupportedException {
    RoaringArray sa;
    sa = (RoaringArray) super.clone();
    sa.keys = Arrays.copyOf(this.keys, this.size);
    sa.values = Arrays.copyOf(this.values, this.size);
    for (int k = 0; k < this.size; ++k) {
      sa.values[k] = sa.values[k].clone();
    }
    sa.size = this.size;
    return sa;
  }

  protected void copyRange(int begin, int end, int newBegin) {
    // assuming begin <= end and newBegin < begin
    final int range = end - begin;
    System.arraycopy(this.keys, begin, this.keys, newBegin, range);
    System.arraycopy(this.values, begin, this.values, newBegin, range);
  }

  /**
   * Deserialize.
   *
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws InvalidRoaringFormat if a Roaring Bitmap cookie
   *             is missing.
   */
  public void deserialize(DataInput in) throws IOException {
    this.clear();
    // little endian
    final int cookie = Integer.reverseBytes(in.readInt());
    if ((cookie & 0xFFFF) != SERIAL_COOKIE && cookie != SERIAL_COOKIE_NO_RUNCONTAINER) {
      throw new InvalidRoaringFormat("I failed to find a valid cookie.");
    }
    this.size = ((cookie & 0xFFFF) == SERIAL_COOKIE) ? (cookie >>> 16) + 1
        : Integer.reverseBytes(in.readInt());
    // logically we cannot have more than (1<<16) containers.
    if(this.size > (1<<16)) {
      throw new InvalidRoaringFormat("Size too large");
    }
    if ((this.keys == null) || (this.keys.length < this.size)) {
      this.keys = new short[this.size];
      this.values = new Container[this.size];
    }


    byte[] bitmapOfRunContainers = null;
    boolean hasrun = (cookie & 0xFFFF) == SERIAL_COOKIE;
    if (hasrun) {
      bitmapOfRunContainers = new byte[(size + 7) / 8];
      in.readFully(bitmapOfRunContainers);
    }

    final short keys[] = new short[this.size];
    final int cardinalities[] = new int[this.size];
    final boolean isBitmap[] = new boolean[this.size];
    for (int k = 0; k < this.size; ++k) {
      keys[k] = Short.reverseBytes(in.readShort());
      cardinalities[k] = 1 + (0xFFFF & Short.reverseBytes(in.readShort()));

      isBitmap[k] = cardinalities[k] > ArrayContainer.DEFAULT_MAX_SIZE;
      if (bitmapOfRunContainers != null && (bitmapOfRunContainers[k / 8] & (1 << (k % 8))) != 0) {
        isBitmap[k] = false;
      }
    }
    if ((!hasrun) || (this.size >= NO_OFFSET_THRESHOLD)) {
      // skipping the offsets
      in.skipBytes(this.size * 4);
    }
    // Reading the containers
    for (int k = 0; k < this.size; ++k) {
      Container val;
      if (isBitmap[k]) {
        final long[] bitmapArray = new long[BitmapContainer.MAX_CAPACITY / 64];
        // little endian
        for (int l = 0; l < bitmapArray.length; ++l) {
          bitmapArray[l] = Long.reverseBytes(in.readLong());
        }
        val = new BitmapContainer(bitmapArray, cardinalities[k]);
      } else if (bitmapOfRunContainers != null
          && ((bitmapOfRunContainers[k / 8] & (1 << (k % 8))) != 0)) {
        // cf RunContainer.writeArray()
        int nbrruns = Util.toIntUnsigned(Short.reverseBytes(in.readShort()));
        final short lengthsAndValues[] = new short[2 * nbrruns];

        for (int j = 0; j < 2 * nbrruns; ++j) {
          lengthsAndValues[j] = Short.reverseBytes(in.readShort());
        }
        val = new RunContainer(lengthsAndValues, nbrruns);
      } else {
        final short[] shortArray = new short[cardinalities[k]];
        for (int l = 0; l < shortArray.length; ++l) {
          shortArray[l] = Short.reverseBytes(in.readShort());
        }
        val = new ArrayContainer(shortArray);
      }
      this.keys[k] = keys[k];
      this.values[k] = val;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RoaringArray) {
      RoaringArray srb = (RoaringArray) o;
      if (srb.size != this.size) {
        return false;
      }
      for (int i = 0; i < srb.size; ++i) {
        if (this.keys[i] != srb.keys[i] || !this.values[i].equals(srb.values[i])) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  // make sure there is capacity for at least k more elements
  protected void extendArray(int k) {
    // size + 1 could overflow
    if (this.size + k >= this.keys.length) {
      int newCapacity;
      if (this.keys.length < 1024) {
        newCapacity = 2 * (this.size + k);
      } else {
        newCapacity = 5 * (this.size + k) / 4;
      }
      this.keys = Arrays.copyOf(this.keys, newCapacity);
      this.values = Arrays.copyOf(this.values, newCapacity);
    }
  }

  // involves a binary search
  protected Container getContainer(short x) {
    int i = this.binarySearch(0, size, x);
    if (i < 0) {
      return null;
    }
    return this.values[i];
  }

  protected Container getContainerAtIndex(int i) {
    return this.values[i];
  }

  /**
   * Create a ContainerPointer for this RoaringArray
   * @return a ContainerPointer
   */
  public ContainerPointer getContainerPointer() {
    return getContainerPointer(0);
  }

  /**
  * Create a ContainerPointer for this RoaringArray
  * @param startIndex starting index in the container list
  * @return a ContainerPointer
  */
  public ContainerPointer getContainerPointer(final int startIndex) {
    return new ContainerPointer() {
      int k = startIndex;

      @Override
      public void advance() {
        ++k;

      }

      @Override
      public ContainerPointer clone() {
        try {
          return (ContainerPointer) super.clone();
        } catch (CloneNotSupportedException e) {
          return null;// will not happen
        }
      }

      @Override
      public int compareTo(ContainerPointer o) {
        if (key() != o.key()) {
          return Util.toIntUnsigned(key()) - Util.toIntUnsigned(o.key());
        }
        return o.getCardinality() - getCardinality();
      }

      @Override
      public int getCardinality() {
        return getContainer().getCardinality();
      }

      @Override
      public Container getContainer() {
        if (k >= RoaringArray.this.size) {
          return null;
        }
        return RoaringArray.this.values[k];
      }


      @Override
      public boolean isBitmapContainer() {
        return getContainer() instanceof BitmapContainer;
      }

      @Override
      public boolean isRunContainer() {
        return getContainer() instanceof RunContainer;
      }


      @Override
      public short key() {
        return RoaringArray.this.keys[k];

      }
    };
  }

  // involves a binary search
  protected int getIndex(short x) {
    // before the binary search, we optimize for frequent cases
    if ((size == 0) || (keys[size - 1] == x)) {
      return size - 1;
    }
    // no luck we have to go through the list
    return this.binarySearch(0, size, x);
  }

  protected short getKeyAtIndex(int i) {
    return this.keys[i];
  }

  @Override
  public int hashCode() {
    int hashvalue = 0;
    for (int k = 0; k < this.size; ++k) {
      hashvalue = 31 * hashvalue + keys[k] * 0xF0F0F0 + values[k].hashCode();
    }
    return hashvalue;
  }

  boolean hasRunContainer() {
    for (int k = 0; k < size; ++k) {
      Container ck = values[k];
      if (ck instanceof RunContainer) {
        return true;
      }
    }
    return false;
  }

  protected int headerSize() {
    if (hasRunContainer()) {
      if (size < NO_OFFSET_THRESHOLD) {// for small bitmaps, we omit the offsets
        return 4 + (size + 7) / 8 + 4 * size;
      }
      return 4 + (size + 7) / 8 + 8 * size;// - 4 because we pack the size with the cookie
    } else {
      return 4 + 4 + 8 * size;
    }
  }


  // insert a new key, it is assumed that it does not exist
  protected void insertNewKeyValueAt(int i, short key, Container value) {
    extendArray(1);
    System.arraycopy(keys, i, keys, i + 1, size - i);
    keys[i] = key;
    System.arraycopy(values, i, values, i + 1, size - i);
    values[i] = value;
    size++;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    deserialize(in);
  }

  protected void removeAtIndex(int i) {
    System.arraycopy(keys, i + 1, keys, i, size - i - 1);
    keys[size - 1] = 0;
    System.arraycopy(values, i + 1, values, i, size - i - 1);
    values[size - 1] = null;
    size--;
  }

  protected void removeIndexRange(int begin, int end) {
    if (end <= begin) {
      return;
    }
    final int range = end - begin;
    System.arraycopy(keys, end, keys, begin, size - end);
    System.arraycopy(values, end, values, begin, size - end);
    for (int i = 1; i <= range; ++i) {
      keys[size - i] = 0;
      values[size - i] = null;
    }
    size -= range;
  }

  protected void replaceKeyAndContainerAtIndex(int i, short key, Container c) {
    this.keys[i] = key;
    this.values[i] = c;
  }

  protected void resize(int newLength) {
    Arrays.fill(this.keys, newLength, this.size, (short) 0);
    Arrays.fill(this.values, newLength, this.size, null);
    this.size = newLength;
  }



  /**
   * Serialize.
   *
   * The current bitmap is not modified.
   *
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void serialize(DataOutput out) throws IOException {
    int startOffset = 0;
    boolean hasrun = hasRunContainer();
    if (hasrun) {
      out.writeInt(Integer.reverseBytes(SERIAL_COOKIE | ((size - 1) << 16)));
      byte[] bitmapOfRunContainers = new byte[(size + 7) / 8];
      for (int i = 0; i < size; ++i) {
        if (this.values[i] instanceof RunContainer) {
          bitmapOfRunContainers[i / 8] |= (1 << (i % 8));
        }
      }
      out.write(bitmapOfRunContainers);
      if (this.size < NO_OFFSET_THRESHOLD) {
        startOffset = 4 + 4 * this.size + bitmapOfRunContainers.length;
      } else {
        startOffset = 4 + 8 * this.size + bitmapOfRunContainers.length;
      }
    } else { // backwards compatibility
      out.writeInt(Integer.reverseBytes(SERIAL_COOKIE_NO_RUNCONTAINER));
      out.writeInt(Integer.reverseBytes(size));
      startOffset = 4 + 4 + 4 * this.size + 4 * this.size;
    }
    for (int k = 0; k < size; ++k) {
      out.writeShort(Short.reverseBytes(this.keys[k]));
      out.writeShort(Short.reverseBytes((short) (this.values[k].getCardinality() - 1)));
    }
    if ((!hasrun) || (this.size >= NO_OFFSET_THRESHOLD)) {
      // writing the containers offsets
      for (int k = 0; k < this.size; k++) {
        out.writeInt(Integer.reverseBytes(startOffset));
        startOffset = startOffset + this.values[k].getArraySizeInBytes();
      }
    }
    for (int k = 0; k < size; ++k) {
      values[k].writeArray(out);
    }
  }

  /**
   * Report the number of bytes required for serialization.
   *
   * @return the size in bytes
   */
  public int serializedSizeInBytes() {
    int count = headerSize();
    for (int k = 0; k < size; ++k) {
      count += values[k].getArraySizeInBytes();
    }
    return count;
  }

  protected void setContainerAtIndex(int i, Container c) {
    this.values[i] = c;
  }

  protected int size() {
    return this.size;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    serialize(out);
  }

  /**
   * Gets the first value in the array
   * @return the first value in the array
   * @throws NoSuchElementException if empty
   */
  public int first() {
    assertNonEmpty();
    short firstKey = keys[0];
    Container container = values[0];
    return firstKey << 16 | container.first();
  }

  /**
   * Gets the last value in the array
   * @return the last value in the array
   * @throws NoSuchElementException if empty
   */
  public int last() {
    assertNonEmpty();
    short lastKey = keys[size - 1];
    Container container = values[size - 1];
    return lastKey << 16 | container.last();
  }

  private void assertNonEmpty() {
    if(size == 0) {
      throw new NoSuchElementException("Empty RoaringArray");
    }
  }
}
