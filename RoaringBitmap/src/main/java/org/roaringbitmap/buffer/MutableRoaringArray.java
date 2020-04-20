/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;


import org.roaringbitmap.AppendableStorage;
import org.roaringbitmap.InvalidRoaringFormat;
import org.roaringbitmap.Util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static java.nio.ByteOrder.LITTLE_ENDIAN;


/**
 * Specialized array to store the containers used by a RoaringBitmap. This class is similar to
 * org.roaringbitmap.RoaringArray but meant to be used with memory mapping. This is not meant to be
 * used by end users.
 *
 * Objects of this class reside in RAM.
 */
public final class MutableRoaringArray implements Cloneable, Externalizable, PointableRoaringArray,
        AppendableStorage<MappeableContainer> {

  protected static final int INITIAL_CAPACITY = 4;

  protected static final short SERIAL_COOKIE_NO_RUNCONTAINER = 12346;
  protected static final short SERIAL_COOKIE = 12347;

  protected static final int NO_OFFSET_THRESHOLD = 4;

  private static final long serialVersionUID = 5L; // TODO: OFK was 4L, not sure


  char[] keys = null;
  MappeableContainer[] values = null;

  int size = 0;

  protected MutableRoaringArray() {
    this(INITIAL_CAPACITY);
  }

  public MutableRoaringArray(int initialCapacity) {
    this(new char[initialCapacity], new MappeableContainer[initialCapacity], 0);
  }

  MutableRoaringArray(char[] keys, MappeableContainer[] values, int size) {
    this.keys = keys;
    this.values = values;
    this.size = size;
  }


  @Override
  public int advanceUntil(char x, int pos) {
    int lower = pos + 1;

    // special handling for a possibly common sequential case
    if (lower >= size || (keys[lower]) >= (x)) {
      return lower;
    }

    int spansize = 1; // could set larger
    // bootstrap an upper limit

    while (lower + spansize < size
        && (keys[lower + spansize]) < (x)) {
      spansize *= 2; // hoping for compiler will reduce to shift
    }
    int upper = (lower + spansize < size) ? lower + spansize : size - 1;

    // maybe we are lucky (could be common case when the seek ahead
    // expected to be small and sequential will otherwise make us look bad)
    if (keys[upper] == x) {
      return upper;
    }

    if ((keys[upper]) < (x)) {// means array has no
                                                                              // item key >= x
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
      } else if ((keys[mid]) < (x)) {
        lower = mid;
      } else {
        upper = mid;
      }
    }
    return upper;
  }

  @Override
  public void append(char key, MappeableContainer value) {
    if (size > 0 && key < keys[size - 1]) {
      throw new IllegalArgumentException("append only: " + (key)
              + " < " + (keys[size - 1]));
    }
    extendArray(1);
    this.keys[this.size] = key;
    this.values[this.size] = value;
    this.size++;
  }

  void append(MutableRoaringArray appendage) {
    assert size == 0 || appendage.size == 0
            || keys[size - 1] < appendage.keys[0];
    if (appendage.size != 0 && size != 0) {
      keys = Arrays.copyOf(keys, size + appendage.size);
      values = Arrays.copyOf(values, size + appendage.size);
      System.arraycopy(appendage.keys, 0, keys, size, appendage.size);
      System.arraycopy(appendage.values, 0, values, size, appendage.size);
      size += appendage.size;
    } else if (size == 0 && appendage.size != 0) {
      keys = Arrays.copyOf(appendage.keys, appendage.keys.length);
      values = Arrays.copyOf(appendage.values, appendage.values.length);
      size = appendage.size;
    }
  }

  /**
   * Append copies of the values AFTER a specified key (may or may not be present) to end.
   *
   * @param highLowContainer the other array
   * @param beforeStart given key is the largest key that we won't copy
   */
  protected void appendCopiesAfter(PointableRoaringArray highLowContainer, char beforeStart) {

    int startLocation = highLowContainer.getIndex(beforeStart);
    if (startLocation >= 0) {
      startLocation++;
    } else {
      startLocation = -startLocation - 1;
    }
    extendArray(highLowContainer.size() - startLocation);

    for (int i = startLocation; i < highLowContainer.size(); ++i) {
      this.keys[this.size] = highLowContainer.getKeyAtIndex(i);
      this.values[this.size] = highLowContainer.getContainerAtIndex(i).clone();
      this.size++;
    }
  }

  /**
   * Append copies of the values from another array, from the start
   *
   * @param highLowContainer the other array
   * @param stoppingKey any equal or larger key in other array will terminate copying
   */
  protected void appendCopiesUntil(PointableRoaringArray highLowContainer, char stoppingKey) {
    final int stopKey = (stoppingKey);
    MappeableContainerPointer cp = highLowContainer.getContainerPointer();
    while (cp.hasContainer()) {
      if ((cp.key()) >= stopKey) {
        break;
      }
      extendArray(1);
      this.keys[this.size] = cp.key();
      this.values[this.size] = cp.getContainer().clone();
      this.size++;
      cp.advance();
    }
  }

  /**
   * Append copies of the values from another array
   *
   * @param highLowContainer other array
   * @param startingIndex starting index in the other array
   * @param end last index array in the other array
   */
  protected void appendCopy(PointableRoaringArray highLowContainer, int startingIndex, int end) {
    extendArray(end - startingIndex);
    for (int i = startingIndex; i < end; ++i) {
      this.keys[this.size] = highLowContainer.getKeyAtIndex(i);
      this.values[this.size] = highLowContainer.getContainerAtIndex(i).clone();
      this.size++;
    }
  }

  protected void appendCopy(char key, MappeableContainer value) {
    extendArray(1);
    this.keys[this.size] = key;
    this.values[this.size] = value.clone();
    this.size++;
  }

  private int binarySearch(int begin, int end, char key) {
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
    for (MappeableContainer c : values) {
      c.trim();
    }
  }

  @Override
  public MutableRoaringArray clone() {
    MutableRoaringArray sa;
    try {
      sa = (MutableRoaringArray) super.clone();

      // OFK: do we need runcontainer bitmap? Guess not, this is just a directory
      // and each container knows what kind it is.
      sa.keys = Arrays.copyOf(this.keys, this.size);
      sa.values = Arrays.copyOf(this.values, this.size);
      for (int k = 0; k < this.size; ++k) {
        sa.values[k] = sa.values[k].clone();
      }
      sa.size = this.size;
      return sa;

    } catch (CloneNotSupportedException e) {
      return null;
    }
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
      this.keys = new char[this.size];
      this.values = new MappeableContainer[this.size];
    }

    byte[] bitmapOfRunContainers = null;
    boolean hasrun = (cookie & 0xFFFF) == SERIAL_COOKIE;
    if (hasrun) {
      bitmapOfRunContainers = new byte[(size + 7) / 8];
      in.readFully(bitmapOfRunContainers);
    }

    final char keys[] = new char[this.size];
    final int cardinalities[] = new int[this.size];
    final boolean isBitmap[] = new boolean[this.size];
    for (int k = 0; k < this.size; ++k) {
      keys[k] = Character.reverseBytes(in.readChar());
      cardinalities[k] = 1 + (0xFFFF & Character.reverseBytes(in.readChar()));
      isBitmap[k] = cardinalities[k] > MappeableArrayContainer.DEFAULT_MAX_SIZE;
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
      MappeableContainer val;
      if (isBitmap[k]) {
        final LongBuffer bitmapArray =
            LongBuffer.allocate(MappeableBitmapContainer.MAX_CAPACITY / 64);
        // little endian
        for (int l = 0; l < bitmapArray.limit(); ++l) {
          bitmapArray.put(l, Long.reverseBytes(in.readLong()));
        }
        val = new MappeableBitmapContainer(bitmapArray, cardinalities[k]);
      } else if (bitmapOfRunContainers != null
          && ((bitmapOfRunContainers[k / 8] & (1 << (k % 8))) != 0)) {
        int nbrruns = (Character.reverseBytes(in.readChar()));
        final CharBuffer charArray = CharBuffer.allocate(2 * nbrruns);
        for (int l = 0; l < charArray.limit(); ++l) {
          charArray.put(l, Character.reverseBytes(in.readChar()));
        }
        val = new MappeableRunContainer(charArray, nbrruns);
      } else {
        final CharBuffer charArray = CharBuffer.allocate(cardinalities[k]);
        for (int l = 0; l < charArray.limit(); ++l) {
          charArray.put(l, Character.reverseBytes(in.readChar()));
        }
        val = new MappeableArrayContainer(charArray, cardinalities[k]);
      }
      this.keys[k] = keys[k];
      this.values[k] = val;
    }
  }

  /**
   * Deserialize (retrieve) this bitmap. See format specification at
   * https://github.com/RoaringBitmap/RoaringFormatSpec
   *
   * The current bitmap is overwritten.
   *
   * It is not necessary that limit() on the input ByteBuffer indicates the end of the serialized
   * data.
   *
   * After loading this RoaringBitmap, you can advance to the rest of the data (if there
   * is more) by setting bbf.position(bbf.position() + bitmap.serializedSizeInBytes());
   *
   * Note that the input ByteBuffer is effectively copied (with the slice operation) so you should
   * expect the provided ByteBuffer position/mark/limit/order to remain unchanged.
   *
   * @param bbf the byte buffer (can be mapped, direct, array backed etc.
   */
  public void deserialize(ByteBuffer bbf) {
    this.clear();

    ByteBuffer buffer = bbf.order() == LITTLE_ENDIAN ? bbf : bbf.slice().order(LITTLE_ENDIAN);
    final int cookie = buffer.getInt();
    if ((cookie & 0xFFFF) != SERIAL_COOKIE && cookie != SERIAL_COOKIE_NO_RUNCONTAINER) {
      throw new InvalidRoaringFormat("I failed to find one of the right cookies. " + cookie);
    }
    boolean hasRunContainers = (cookie & 0xFFFF) == SERIAL_COOKIE;
    this.size = hasRunContainers ? (cookie >>> 16) + 1 : buffer.getInt();

    if(this.size > (1<<16)) {
      throw new InvalidRoaringFormat("Size too large");
    }
    if ((this.keys == null) || (this.keys.length < this.size)) {
      this.keys = new char[this.size];
      this.values = new MappeableContainer[this.size];
    }


    byte[] bitmapOfRunContainers = null;
    boolean hasrun = (cookie & 0xFFFF) == SERIAL_COOKIE;
    if (hasrun) {
      bitmapOfRunContainers = new byte[(size + 7) / 8];
      buffer.get(bitmapOfRunContainers);
    }

    final char[] keys = new char[this.size];
    final int[] cardinalities = new int[this.size];
    final boolean[] isBitmap = new boolean[this.size];
    for (int k = 0; k < this.size; ++k) {
      keys[k] = buffer.getChar();
      cardinalities[k] = 1 + buffer.getChar();

      isBitmap[k] = cardinalities[k] > MappeableArrayContainer.DEFAULT_MAX_SIZE;
      if (bitmapOfRunContainers != null && (bitmapOfRunContainers[k / 8] & (1 << (k & 7))) != 0) {
        isBitmap[k] = false;
      }
    }
    if ((!hasrun) || (this.size >= NO_OFFSET_THRESHOLD)) {
      // skipping the offsets
      buffer.position(buffer.position() + this.size * 4);
    }

    // Reading the containers
    for (int k = 0; k < this.size; ++k) {
      MappeableContainer container;
      if (isBitmap[k]) {
        long[] array = new long[MappeableBitmapContainer.MAX_CAPACITY / 64];
        buffer.asLongBuffer().get(array);
        container = new MappeableBitmapContainer(cardinalities[k], LongBuffer.wrap(array));
        buffer.position(buffer.position() + 1024 * 8);
      } else if (bitmapOfRunContainers != null
              && ((bitmapOfRunContainers[k / 8] & (1 << (k & 7))) != 0)) {

        int nbrruns = (buffer.getChar());
        int length = 2 * nbrruns;
        char[] array = new char[length];
        buffer.asCharBuffer().get(array);
        container = new MappeableRunContainer(CharBuffer.wrap(array), nbrruns);
        buffer.position(buffer.position() + length * 2);
      } else {
        int cardinality = cardinalities[k];
        char[] array = new char[cardinality];
        buffer.asCharBuffer().get(array);
        container = new MappeableArrayContainer(CharBuffer.wrap(array), cardinality);
        buffer.position(buffer.position() + cardinality * 2);
      }
      this.keys[k] = keys[k];
      this.values[k] = container;
    }
  }

  // make sure there is capacity for at least k more elements
  protected void extendArray(int k) {
    // size + 1 could overflow
    if (this.size + k > this.keys.length) {
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

  @Override
  public int getCardinality(int i) {
    return getContainerAtIndex(i).getCardinality();
  }

  // retired method (inefficient)
  // involves a binary search
  /*@Override
  public MappeableContainer getContainer(char x) {
    final int i = this.binarySearch(0, size, x);
    if (i < 0) {
      return null;
    }
    return this.values[i];
  }*/
  
  @Override
  public int getContainerIndex(char x) {
    return this.binarySearch(0, size, x);
  }  
  

  @Override
  public MappeableContainer getContainerAtIndex(int i) {
    return this.values[i];
  }

  @Override
  public MappeableContainerPointer getContainerPointer() {
    return getContainerPointer(0);
  }

  @Override
  public MappeableContainerPointer getContainerPointer(final int startIndex) {
    return new MappeableContainerPointer() {
      int k = startIndex;

      @Override
      public void advance() {
        ++k;
      }

      @Override
      public MappeableContainerPointer clone() {
        try {
          return (MappeableContainerPointer) super.clone();
        } catch (CloneNotSupportedException e) {
          return null;// will not happen
        }
      }

      @Override
      public int compareTo(MappeableContainerPointer o) {
        if (key() != o.key()) {
          return (key()) - (o.key());
        }
        return o.getCardinality() - this.getCardinality();
      }

      @Override
      public int getCardinality() {
        return getContainer().getCardinality();
      }

      @Override
      public MappeableContainer getContainer() {
        if (k >= MutableRoaringArray.this.size) {
          return null;
        }
        return MutableRoaringArray.this.values[k];
      }

      @Override
      public int getSizeInBytes() {
        return getContainer().getArraySizeInBytes();
      }

      @Override
      public boolean hasContainer() {
        return 0 <= k & k < MutableRoaringArray.this.size;
      }

      @Override
      public boolean isBitmapContainer() {
        return getContainer() instanceof MappeableBitmapContainer;
      }

      @Override
      public boolean isRunContainer() {
        return getContainer() instanceof MappeableRunContainer;
      }

      @Override
      public char key() {
        return MutableRoaringArray.this.keys[k];

      }


      @Override
      public void previous() {
        --k;
      }
    };

  }

  // involves a binary search
  @Override
  public int getIndex(char x) {
    // before the binary search, we optimize for frequent cases
    if ((size == 0) || (keys[size - 1] == x)) {
      return size - 1;
    }
    // no luck we have to go through the list
    return this.binarySearch(0, size, x);
  }

  @Override
  public char getKeyAtIndex(int i) {
    return this.keys[i];
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ImmutableRoaringArray) {
      ImmutableRoaringArray srb = (ImmutableRoaringArray)o;
      if (srb.size() != this.size()) {
        return false;
      }
      MappeableContainerPointer cp = this.getContainerPointer();
      MappeableContainerPointer cpo = srb.getContainerPointer();
      while(cp.hasContainer() && cpo.hasContainer()) {
        if(cp.key() != cpo.key()) {
          return false;
        }
        if(!cp.getContainer().equals(cpo.getContainer())) {
          return false;
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashvalue = 0;
    for (int k = 0; k < this.size; ++k) {
      hashvalue = 31 * hashvalue + keys[k] * 0xF0F0F0 + values[k].hashCode();
    }
    return hashvalue;
  }

  @Override
  public boolean hasRunCompression() {
    for (int k = 0; k < size; ++k) {
      MappeableContainer ck = values[k];
      if (ck instanceof MappeableRunContainer) {
        return true;
      }
    }
    return false;
  }

  protected int headerSize() {
    if (hasRunCompression()) {
      if (size < NO_OFFSET_THRESHOLD) {// for small bitmaps, we omit the offsets
        return 4 + (size + 7) / 8 + 4 * size;
      }
      return 4 + (size + 7) / 8 + 8 * size;// - 4 because we pack the size with the cookie
    } else {
      return 4 + 4 + 8 * size;
    }
  }

  // insert a new key, it is assumed that it does not exist
  protected void insertNewKeyValueAt(int i, char key, MappeableContainer value) {
    extendArray(1);
    System.arraycopy(keys, i, keys, i + 1, size - i);
    System.arraycopy(values, i, values, i + 1, size - i);
    keys[i] = key;
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

  protected void replaceKeyAndContainerAtIndex(int i, char key, MappeableContainer c) {
    this.keys[i] = key;
    this.values[i] = c;
  }


  protected void resize(int newLength) {
    Arrays.fill(this.keys, newLength, this.size, (char) 0);
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
  @Override
  public void serialize(DataOutput out) throws IOException {
    int startOffset = 0;
    boolean hasrun = hasRunCompression();
    if (hasrun) {
      out.writeInt(Integer.reverseBytes(SERIAL_COOKIE | ((this.size - 1) << 16)));
      byte[] bitmapOfRunContainers = new byte[(size + 7) / 8];
      for (int i = 0; i < size; ++i) {
        if (this.values[i] instanceof MappeableRunContainer) {
          bitmapOfRunContainers[i / 8] |= (1 << (i % 8));
        }
      }
      out.write(bitmapOfRunContainers);
      if (this.size < NO_OFFSET_THRESHOLD) {
        startOffset = 4 + 4 * this.size + bitmapOfRunContainers.length;
      } else {
        startOffset = 4 + 8 * this.size + bitmapOfRunContainers.length;
      }
    } else { // backwards compatibilility
      out.writeInt(Integer.reverseBytes(SERIAL_COOKIE_NO_RUNCONTAINER));
      out.writeInt(Integer.reverseBytes(size));
      startOffset = 4 + 4 + this.size * 4 + this.size * 4;
    }
    for (int k = 0; k < size; ++k) {
      out.writeShort(Character.reverseBytes(this.keys[k]));
      out.writeShort(Character.reverseBytes((char) (this.values[k].getCardinality() - 1)));
    }
    if ((!hasrun) || (this.size >= NO_OFFSET_THRESHOLD)) {
      for (int k = 0; k < this.size; k++) {
        out.writeInt(Integer.reverseBytes(startOffset));
        startOffset = startOffset + values[k].getArraySizeInBytes();
      }
    }
    for (int k = 0; k < size; ++k) {
      values[k].writeArray(out);
    }

  }

  /**
   * Serialize.
   *
   * The current bitmap is not modified.
   *
   * @param buffer the ByteBuffer to write to
   */
  @Override
  public void serialize(ByteBuffer buffer) {
    ByteBuffer buf = buffer.order() == LITTLE_ENDIAN ? buffer : buffer.slice().order(LITTLE_ENDIAN);
    int startOffset;
    boolean hasrun = hasRunCompression();
    if (hasrun) {
      buf.putInt(SERIAL_COOKIE | ((size - 1) << 16));
      int offset = buf.position();
      for (int i = 0; i < size; i += 8) {
        int runMarker = 0;
        for (int j = 0; j < 8 && i + j < size; ++j) {
          if (values[i + j] instanceof MappeableRunContainer) {
            runMarker |= (1 << j);
          }
        }
        buf.put((byte)runMarker);
      }
      int runMarkersLength = buf.position() - offset;
      if (this.size < NO_OFFSET_THRESHOLD) {
        startOffset = 4 + 4 * this.size + runMarkersLength;
      } else {
        startOffset = 4 + 8 * this.size + runMarkersLength;
      }
    } else { // backwards compatibility
      buf.putInt(SERIAL_COOKIE_NO_RUNCONTAINER);
      buf.putInt(size);
      startOffset = 4 + 4 + 4 * this.size + 4 * this.size;
    }
    for (int k = 0; k < size; ++k) {
      buf.putChar(this.keys[k]);
      buf.putChar((char) (this.values[k].getCardinality() - 1));
    }
    if ((!hasrun) || (this.size >= NO_OFFSET_THRESHOLD)) {
      // writing the containers offsets
      for (int k = 0; k < this.size; ++k) {
        buf.putInt(startOffset);
        startOffset = startOffset + this.values[k].getArraySizeInBytes();
      }
    }
    for (int k = 0; k < size; ++k) {
      values[k].writeArray(buf);
    }
    if (buf != buffer) {
      buffer.position(buffer.position() + buf.position());
    }
  }

  /**
   * Report the number of bytes required for serialization.
   *
   * @return the size in bytes
   */
  @Override
  public int serializedSizeInBytes() {
    int count = headerSize();
    // for each container, we store cardinality (16 bits), key (16 bits) and location offset (32
    // bits).
    for (int k = 0; k < this.size; ++k) {
      count += values[k].getArraySizeInBytes();
    }
    return count;
  }

  protected void setContainerAtIndex(int i, MappeableContainer c) {
    this.values[i] = c;
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    serialize(out);
  }


  @Override
  public boolean containsForContainerAtIndex(int i, char x) {
    return getContainerAtIndex(i).contains(x);// no faster way
  }

  @Override
  public int first() {
    assertNonEmpty();
    char firstKey = getKeyAtIndex(0);
    MappeableContainer container = getContainerAtIndex(0);
    return firstKey << 16 | container.first();
  }

  @Override
  public int last() {
    assertNonEmpty();
    char lastKey = getKeyAtIndex(size - 1);
    MappeableContainer container = getContainerAtIndex(size - 1);
    return lastKey << 16 | container.last();
  }

  private void assertNonEmpty() {
    if(size == 0) {
      throw new NoSuchElementException("Empty MutableRoaringArray");
    }
  }

}
