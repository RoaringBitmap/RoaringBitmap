/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Iterator;

import static org.roaringbitmap.buffer.BufferUtil.toIntUnsigned;

/**
 * Simple container made of an array of 16-bit integers. Unlike org.roaringbitmap.ArrayContainer,
 * this class uses a ShortBuffer to store data.
 */
public final class MappeableArrayContainer extends MappeableContainer implements Cloneable {
  private static final int DEFAULT_INIT_SIZE = 4;
  private static final int ARRAY_LAZY_LOWERBOUND = 1024;
  protected static final int DEFAULT_MAX_SIZE = 4096; // containers with DEFAULT_MAX_SZE or less
                                                      // integers should be ArrayContainers

  private static final long serialVersionUID = 1L;

  protected static int getArraySizeInBytes(int cardinality) {
    return cardinality * 2;
  }

  protected static int serializedSizeInBytes(int cardinality) {
    return cardinality * 2 + 2;
  }

  protected int cardinality = 0;


  protected ShortBuffer content;



  /**
   * Create an array container with default capacity
   */
  public MappeableArrayContainer() {
    this(DEFAULT_INIT_SIZE);
  }

  public static MappeableArrayContainer empty() {
    return new MappeableArrayContainer();
  }

  /**
   * Creates a new container from a non-mappeable one. This copies the data.
   *
   * @param bc the original container
   */
  public MappeableArrayContainer(ArrayContainer bc) {
    this.cardinality = bc.getCardinality();
    this.content = bc.toShortBuffer();
  }


  /**
   * Create an array container with specified capacity
   *
   * @param capacity The capacity of the container
   */
  public MappeableArrayContainer(final int capacity) {
    content = ShortBuffer.allocate(capacity);
  }

  /**
   * Create an array container with a run of ones from firstOfRun to lastOfRun, exclusive. Caller is
   * responsible for making sure the range is small enough that ArrayContainer is appropriate.
   *
   * @param firstOfRun first index
   * @param lastOfRun last index (range is exclusive)
   */
  public MappeableArrayContainer(final int firstOfRun, final int lastOfRun) {
    // TODO: this can be optimized for performance
    final int valuesInRange = lastOfRun - firstOfRun;
    content = ShortBuffer.allocate(valuesInRange);
    short[] sarray = content.array();
    for (int i = 0; i < valuesInRange; ++i) {
      sarray[i] = (short) (firstOfRun + i);
    }
    cardinality = valuesInRange;
  }

  private MappeableArrayContainer(int newCard, ShortBuffer newContent) {
    this.cardinality = newCard;
    ShortBuffer tmp = newContent.duplicate();// for thread-safety
    this.content = ShortBuffer.allocate(Math.max(newCard, tmp.limit()));
    tmp.rewind();
    this.content.put(tmp);
  }

  /**
   * Construct a new ArrayContainer backed by the provided ShortBuffer. Note that if you modify the
   * ArrayContainer a new ShortBuffer may be produced.
   *
   * @param array ShortBuffer where the data is stored
   * @param cardinality cardinality (number of values stored)
   */
  public MappeableArrayContainer(final ShortBuffer array, final int cardinality) {
    if (array.limit() != cardinality) {
      throw new RuntimeException("Mismatch between buffer and cardinality");
    }
    this.cardinality = cardinality;
    this.content = array;
  }

  @Override
  public MappeableContainer add(int begin, int end) {
    // TODO: may need to convert to a RunContainer
    if (end == begin) {
      return clone();
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int indexstart = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) begin);
    if (indexstart < 0) {
      indexstart = -indexstart - 1;
    }
    int indexend = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) (end - 1));
    if (indexend < 0) {
      indexend = -indexend - 1;
    } else {
      indexend++;
    }
    int rangelength = end - begin;
    int newcardinality = indexstart + (cardinality - indexend) + rangelength;
    if (newcardinality > DEFAULT_MAX_SIZE) {
      MappeableBitmapContainer a = this.toBitmapContainer();
      return a.iadd(begin, end);
    }
    MappeableArrayContainer answer = new MappeableArrayContainer(newcardinality, content);
    if (!BufferUtil.isBackedBySimpleArray(answer.content)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    BufferUtil.arraycopy(content, indexend, answer.content, indexstart + rangelength,
        cardinality - indexend);
    short[] answerarray = answer.content.array();
    for (int k = 0; k < rangelength; ++k) {
      answerarray[k + indexstart] = (short) (begin + k);
    }
    answer.cardinality = newcardinality;
    return answer;
  }

  /**
   * running time is in O(n) time if insert is not in order.
   */
  @Override
  // not thread-safe
  public MappeableContainer add(final short x) {
    if (BufferUtil.isBackedBySimpleArray(this.content)) {
      short[] sarray = content.array();

      if (cardinality == 0 || (cardinality > 0
              && toIntUnsigned(x) > toIntUnsigned(sarray[cardinality - 1]))) {
        if (cardinality >= DEFAULT_MAX_SIZE) {
          return toBitmapContainer().add(x);
        }
        if (cardinality >= sarray.length) {
          increaseCapacity();
          sarray = content.array();
        }
        sarray[cardinality++] = x;
      } else {

        int loc = Util.unsignedBinarySearch(sarray, 0, cardinality, x);
        if (loc < 0) {
          // Transform the ArrayContainer to a BitmapContainer
          // when cardinality exceeds DEFAULT_MAX_SIZE
          if (cardinality >= DEFAULT_MAX_SIZE) {
            return toBitmapContainer().add(x);
          }
          if (cardinality >= sarray.length) {
            increaseCapacity();
            sarray = content.array();
          }
          // insertion : shift the elements > x by one
          // position to
          // the right
          // and put x in it's appropriate place
          System.arraycopy(sarray, -loc - 1, sarray, -loc, cardinality + loc + 1);
          sarray[-loc - 1] = x;
          ++cardinality;
        }
      }
    } else {
      if (cardinality == 0 || (cardinality > 0
              && toIntUnsigned(x) > toIntUnsigned(content.get(cardinality - 1)))) {
        if (cardinality >= DEFAULT_MAX_SIZE) {
          return toBitmapContainer().add(x);
        }
        if (cardinality >= content.limit()) {
          increaseCapacity();
        }
        content.put(cardinality++, x);
      }

      final int loc = BufferUtil.unsignedBinarySearch(content, 0, cardinality, x);
      if (loc < 0) {
        // Transform the ArrayContainer to a BitmapContainer
        // when cardinality exceeds DEFAULT_MAX_SIZE
        if (cardinality >= DEFAULT_MAX_SIZE) {
          final MappeableBitmapContainer a = this.toBitmapContainer();
          a.add(x);
          return a;
        }
        if (cardinality >= this.content.limit()) {
          increaseCapacity();
        }
        // insertion : shift the elements > x by one
        // position to
        // the right
        // and put x in it's appropriate place
        for (int k = cardinality; k > -loc - 1; --k) {
          content.put(k, content.get(k - 1));
        }
        content.put(-loc - 1, x);

        ++cardinality;
      }
    }
    return this;
  }

  @Override
  public boolean isEmpty() {
    return cardinality == 0;
  }

  private int advance(ShortIterator it) {
    if (it.hasNext()) {
      return toIntUnsigned(it.next());
    } else {
      return -1;
    }
  }


  @Override
  public MappeableArrayContainer and(final MappeableArrayContainer value2) {

    MappeableArrayContainer value1 = this;
    final int desiredCapacity = Math.min(value1.getCardinality(), value2.getCardinality());
    MappeableArrayContainer answer = new MappeableArrayContainer(desiredCapacity);
    if (BufferUtil.isBackedBySimpleArray(this.content)
        && BufferUtil.isBackedBySimpleArray(value2.content)) {
      answer.cardinality = org.roaringbitmap.Util.unsignedIntersect2by2(value1.content.array(),
          value1.getCardinality(), value2.content.array(), value2.getCardinality(),
          answer.content.array());
    } else {
      answer.cardinality = BufferUtil.unsignedIntersect2by2(value1.content, value1.getCardinality(),
          value2.content, value2.getCardinality(), answer.content.array());
    }
    return answer;
  }

  @Override
  public MappeableContainer and(MappeableBitmapContainer x) {
    return x.and(this);
  }

  @Override
  public MappeableContainer and(final MappeableRunContainer value2) {
    return value2.and(this);
  }


  @Override
  public MappeableArrayContainer andNot(final MappeableArrayContainer value2) {
    final MappeableArrayContainer value1 = this;
    final int desiredCapacity = value1.getCardinality();
    final MappeableArrayContainer answer = new MappeableArrayContainer(desiredCapacity);
    if (BufferUtil.isBackedBySimpleArray(value1.content)
        && BufferUtil.isBackedBySimpleArray(value2.content)) {
      answer.cardinality =
          org.roaringbitmap.Util.unsignedDifference(value1.content.array(), value1.getCardinality(),
              value2.content.array(), value2.getCardinality(), answer.content.array());
    } else {
      answer.cardinality = BufferUtil.unsignedDifference(value1.content, value1.getCardinality(),
          value2.content, value2.getCardinality(), answer.content.array());
    }
    return answer;
  }

  @Override
  public MappeableArrayContainer andNot(MappeableBitmapContainer value2) {

    final MappeableArrayContainer answer = new MappeableArrayContainer(content.limit());
    int pos = 0;
    short[] sarray = answer.content.array();
    if (BufferUtil.isBackedBySimpleArray(this.content)) {
      short[] c = content.array();
      for (int k = 0; k < cardinality; ++k) {
        short v = c[k];
        sarray[pos] = v;
        pos += 1 - value2.bitValue(v);
      }
    } else {
      for (int k = 0; k < cardinality; ++k) {
        short v = this.content.get(k);
        sarray[pos] = v;
        pos += 1 - value2.bitValue(v);
      }
    }
    answer.cardinality = pos;
    return answer;
  }

  @Override
  public MappeableContainer andNot(final MappeableRunContainer x) {
    if (x.numberOfRuns() == 0) {
      return clone();
    } else if (x.isFull()) {
      return MappeableArrayContainer.empty();
    }
    int write = 0;
    int read = 0;
    MappeableArrayContainer answer = new MappeableArrayContainer(cardinality);
    for (int i = 0; i < x.numberOfRuns() && read < cardinality; ++i) {
      int runStart = BufferUtil.toIntUnsigned(x.getValue(i));
      int runEnd = runStart + BufferUtil.toIntUnsigned(x.getLength(i));
      if (BufferUtil.toIntUnsigned(content.get(read)) > runEnd) {
        continue;
      }
      int firstInRun = BufferUtil.iterateUntil(content, read, cardinality, runStart);
      int toWrite = firstInRun - read;
      BufferUtil.arraycopy(content, read, answer.content, write, toWrite);
      write += toWrite;

      read = BufferUtil.iterateUntil(content, firstInRun, cardinality, runEnd + 1);
    }
    BufferUtil.arraycopy(content, read, answer.content, write, cardinality - read);
    write += cardinality - read;
    answer.cardinality = write;
    return answer;

  }

  @Override
  public void clear() {
    cardinality = 0;
  }

  @Override
  public MappeableArrayContainer clone() {
    return new MappeableArrayContainer(this.cardinality, this.content);
  }

  @Override
  public boolean contains(final short x) {
    return BufferUtil.unsignedBinarySearch(content, 0, cardinality, x) >= 0;
  }

  /**
   * Checks whether the container contains the value x.
   *
   * @param buf underlying buffer
   * @param position starting position of the container in the ByteBuffer
   * @param x target value x
   * @param cardinality container cardinality
   * @return whether the container contains the value x
   */
  public static boolean contains(ByteBuffer buf, int position, final short x, int cardinality) {
    return BufferUtil.unsignedBinarySearch(buf, position, 0, cardinality, x) >= 0;
  }


  // in order
  // not thread-safe
  private void emit(short val) {
    if (cardinality == content.limit()) {
      increaseCapacity(true);
    }
    content.put(cardinality++, val);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MappeableArrayContainer) {
      final MappeableArrayContainer srb = (MappeableArrayContainer) o;
      if (srb.cardinality != this.cardinality) {
        return false;
      }
      if (BufferUtil.isBackedBySimpleArray(this.content)
          && BufferUtil.isBackedBySimpleArray(srb.content)) {
        short[] t = this.content.array();
        short[] sr = srb.content.array();

        for (int i = 0; i < this.cardinality; ++i) {
          if (t[i] != sr[i]) {
            return false;
          }
        }

      } else {
        for (int i = 0; i < this.cardinality; ++i) {
          if (this.content.get(i) != srb.content.get(i)) {
            return false;
          }
        }
      }
      return true;
    } else if (o instanceof MappeableRunContainer) {
      return o.equals(this);
    }
    return false;
  }


  @Override
  public void fillLeastSignificant16bits(int[] x, int i, int mask) {
    if (BufferUtil.isBackedBySimpleArray(this.content)) {
      short[] c = this.content.array();
      for (int k = 0; k < this.cardinality; ++k) {
        x[k + i] = toIntUnsigned(c[k]) | mask;
      }

    } else {
      for (int k = 0; k < this.cardinality; ++k) {
        x[k + i] = toIntUnsigned(this.content.get(k)) | mask;
      }
    }
  }

  @Override
  // not thread-safe
  public MappeableContainer flip(short x) {
    if (BufferUtil.isBackedBySimpleArray(this.content)) {
      short[] sarray = content.array();
      int loc = Util.unsignedBinarySearch(sarray, 0, cardinality, x);
      if (loc < 0) {
        // Transform the ArrayContainer to a BitmapContainer
        // when cardinality = DEFAULT_MAX_SIZE
        if (cardinality >= DEFAULT_MAX_SIZE) {
          MappeableBitmapContainer a = this.toBitmapContainer();
          a.add(x);
          return a;
        }
        if (cardinality >= sarray.length) {
          increaseCapacity();
          sarray = content.array();
        }
        // insertion : shift the elements > x by one position to
        // the right
        // and put x in it's appropriate place
        System.arraycopy(sarray, -loc - 1, sarray, -loc, cardinality + loc + 1);
        sarray[-loc - 1] = x;
        ++cardinality;
      } else {
        System.arraycopy(sarray, loc + 1, sarray, loc, cardinality - loc - 1);
        --cardinality;
      }
      return this;

    } else {
      int loc = BufferUtil.unsignedBinarySearch(content, 0, cardinality, x);
      if (loc < 0) {
        // Transform the ArrayContainer to a BitmapContainer
        // when cardinality = DEFAULT_MAX_SIZE
        if (cardinality >= DEFAULT_MAX_SIZE) {
          MappeableBitmapContainer a = this.toBitmapContainer();
          a.add(x);
          return a;
        }
        if (cardinality >= content.limit()) {
          increaseCapacity();
        }
        // insertion : shift the elements > x by one position to
        // the right
        // and put x in it's appropriate place
        for (int k = cardinality; k > -loc - 1; --k) {
          content.put(k, content.get(k - 1));
        }
        content.put(-loc - 1, x);
        ++cardinality;
      } else {
        for (int k = loc + 1; k < cardinality; --k) {
          content.put(k - 1, content.get(k));
        }
        --cardinality;
      }
      return this;
    }
  }

  @Override
  protected int getArraySizeInBytes() {
    return getArraySizeInBytes(cardinality);
  }

  @Override
  public int getCardinality() {
    return cardinality;
  }

  @Override
  public ShortIterator getReverseShortIterator() {
    if (this.isArrayBacked()) {
      return new RawReverseArrayContainerShortIterator(this);
    }
    return new ReverseMappeableArrayContainerShortIterator(this);
  }

  @Override
  public PeekableShortIterator getShortIterator() {
    if (this.isArrayBacked()) {
      return new RawArrayContainerShortIterator(this);
    }
    return new MappeableArrayContainerShortIterator(this);
  }

  @Override
  public ContainerBatchIterator getBatchIterator() {
    return new ArrayBatchIterator(this);
  }

  @Override
  public int getSizeInBytes() {
    return this.cardinality * 2;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (int k = 0; k < cardinality; ++k) {
      hash += 31 * hash + content.get(k);
    }
    return hash;
  }

  @Override
  // not thread-safe
  public MappeableContainer iadd(int begin, int end) {
    // TODO: may need to convert to a RunContainer
    if (end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int indexstart = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) begin);
    if (indexstart < 0) {
      indexstart = -indexstart - 1;
    }
    int indexend = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) (end - 1));
    if (indexend < 0) {
      indexend = -indexend - 1;
    } else {
      indexend++;
    }
    int rangelength = end - begin;
    int newcardinality = indexstart + (cardinality - indexend) + rangelength;
    if (newcardinality > DEFAULT_MAX_SIZE) {
      MappeableBitmapContainer a = this.toBitmapContainer();
      return a.iadd(begin, end);
    }
    /*
     * b - index of begin(indexstart), e - index of end(indexend), |--| is current sequential
     * indexes in content. Total 6 cases are possible, listed as below:
     *
     * case-1) |--------|b-e case-2) |----b---|e case-3) |---b---e---| case-4) b|----e---| case-5)
     * b-e|------| case-6) b|-----|e
     *
     * In case of old approach, we did (1a) Array.copyOf in increaseCapacity ( # of elements copied
     * -> cardinality), (1b) then we moved elements using System.arrayCopy ( # of elements copied ->
     * cardinality -indexend), (1c) then we set all elements from begin to end ( # of elements set
     * -> end - begin)
     *
     * With new approach, (2a) we set all elements from begin to end ( # of elements set -> end-
     * begin), (2b) we only copy elements in current set which are not in range begin-end ( # of
     * elements copied -> cardinality - (end-begin) )
     *
     * why is it faster? Logically we are doing less # of copies. Mathematically proof as below: ->
     * 2a is same as 1c, so we can avoid. Assume, 2b < (1a+1b), lets prove this assumption.
     * Substitute the values. (cardinality - (end-begin)) < ( 2*cardinality - indexend) , lowest
     * possible value of indexend is 0 and equation holds true , hightest possible value of indexend
     * is cardinality and equation holds true , hence "<" equation holds true always
     */
    if (newcardinality >= this.content.limit()) {
      ShortBuffer destination = ShortBuffer.allocate(newcardinality);
      BufferUtil.arraycopy(content, 0, destination, 0, indexstart);
      if (BufferUtil.isBackedBySimpleArray(content)) {
        short[] destinationarray = destination.array();
        for (int k = 0; k < rangelength; ++k) {
          destinationarray[k + indexstart] = (short) (begin + k);
        }
      } else {
        for (int k = 0; k < rangelength; ++k) {
          destination.put(k + indexstart, (short) (begin + k));
        }
      }
      BufferUtil.arraycopy(content, indexend, destination, indexstart + rangelength, cardinality
          - indexend);
      this.content = destination;
    } else {
      BufferUtil.arraycopy(content, indexend, content, indexstart + rangelength, cardinality
          - indexend);
      if (BufferUtil.isBackedBySimpleArray(content)) {
        short[] contentarray = content.array();
        for (int k = 0; k < rangelength; ++k) {
          contentarray[k + indexstart] = (short) (begin + k);
        }
      } else {
        for (int k = 0; k < rangelength; ++k) {
          content.put(k + indexstart, (short) (begin + k));
        }
      }
    }
    cardinality = newcardinality;
    return this;
  }

  @Override
  public MappeableArrayContainer iand(final MappeableArrayContainer value2) {
    final MappeableArrayContainer value1 = this;
    if (!BufferUtil.isBackedBySimpleArray(value1.content)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    value1.cardinality = BufferUtil.unsignedIntersect2by2(value1.content, value1.getCardinality(),
        value2.content, value2.getCardinality(), value1.content.array());
    return this;
  }


  @Override
  public MappeableContainer iand(MappeableBitmapContainer value2) {
    int pos = 0;
    for (int k = 0; k < cardinality; ++k) {
      short v = this.content.get(k);
      this.content.put(pos, v);
      pos += value2.bitValue(v);
    }
    cardinality = pos;
    return this;
  }


  // Note it is never inplace, may wish to fix
  @Override
  public MappeableContainer iand(final MappeableRunContainer value2) {
    return value2.and(this);
  }

  @Override
  public MappeableArrayContainer iandNot(final MappeableArrayContainer value2) {
    if (!BufferUtil.isBackedBySimpleArray(this.content)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    if (BufferUtil.isBackedBySimpleArray(value2.content)) {
      this.cardinality =
          org.roaringbitmap.Util.unsignedDifference(this.content.array(), this.getCardinality(),
              value2.content.array(), value2.getCardinality(), this.content.array());
    } else {
      this.cardinality = BufferUtil.unsignedDifference(this.content, this.getCardinality(),
          value2.content, value2.getCardinality(), this.content.array());
    }

    return this;
  }

  @Override
  public MappeableArrayContainer iandNot(MappeableBitmapContainer value2) {
    if (!BufferUtil.isBackedBySimpleArray(this.content)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    short[] c = this.content.array();
    int pos = 0;
    for (int k = 0; k < cardinality; ++k) {
      short v = c[k];
      c[pos] = v;
      pos += 1 - value2.bitValue(v);
    }
    this.cardinality = pos;
    return this;
  }

  @Override
  public MappeableContainer iandNot(final MappeableRunContainer value2) { // not inplace, revisit?
    return andNot(value2);
  }

  private void increaseCapacity() {
    increaseCapacity(false);
  }

  // temporarily allow an illegally large size, as long as the operation creating
  // the illegal container does not return it.
  // not thread safe!
  private void increaseCapacity(boolean allowIllegalSize) {
    int len = this.content.limit();
    int newCapacity = (len == 0) ? DEFAULT_INIT_SIZE
        : len < 64 ? len * 2 : this.content.limit() < 1067 ? len * 3 / 2 : len * 5 / 4;
    // do not allocate more than we will ever need
    if (newCapacity > MappeableArrayContainer.DEFAULT_MAX_SIZE && !allowIllegalSize) {
      newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
    }
    // if we are within 1/16th of the max., go to max right away to avoid further reallocations
    if (newCapacity > MappeableArrayContainer.DEFAULT_MAX_SIZE
        - MappeableArrayContainer.DEFAULT_MAX_SIZE / 16 && !allowIllegalSize) {
      newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
    }
    final ShortBuffer newContent = ShortBuffer.allocate(newCapacity);
    this.content.rewind();
    newContent.put(this.content);
    this.content = newContent;
  }

  private int calculateCapacity(int min){
    int len = this.content.limit();
    int newCapacity = (len == 0) ? DEFAULT_INIT_SIZE
        : len < 64 ? len * 2 : len < 1024 ? len * 3 / 2 : len * 5 / 4;
    if (newCapacity < min) {
      newCapacity = min;
    }
    if (newCapacity > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
    }
    if (newCapacity > MappeableArrayContainer.DEFAULT_MAX_SIZE
        - MappeableArrayContainer.DEFAULT_MAX_SIZE / 16) {
      newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
    }
    return newCapacity;
  }

  @Override
  // not thread safe! (duh!)
  public MappeableContainer inot(final int firstOfRange, final int lastOfRange) {
    // TODO: may need to convert to a RunContainer
    // TODO: this can be optimized for performance
    // determine the span of array indices to be affected
    int startIndex = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) firstOfRange);
    if (startIndex < 0) {
      startIndex = -startIndex - 1;
    }
    int lastIndex =
        BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) (lastOfRange - 1));
    if (lastIndex < 0) {
      lastIndex = -lastIndex - 1 - 1;
    }
    final int currentValuesInRange = lastIndex - startIndex + 1;
    final int spanToBeFlipped = lastOfRange - firstOfRange;
    final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
    final ShortBuffer buffer = ShortBuffer.allocate(newValuesInRange);
    final int cardinalityChange = newValuesInRange - currentValuesInRange;
    final int newCardinality = cardinality + cardinalityChange;

    if (cardinalityChange > 0) { // expansion, right shifting needed
      if (newCardinality > content.limit()) {
        // so big we need a bitmap?
        if (newCardinality > DEFAULT_MAX_SIZE) {
          return toBitmapContainer().inot(firstOfRange, lastOfRange);
        }
        final ShortBuffer co = ShortBuffer.allocate(newCardinality);
        content.rewind();
        co.put(content);
        content = co;
      }
      // slide right the contents after the range
      for (int pos = cardinality - 1; pos > lastIndex; --pos) {
        content.put(pos + cardinalityChange, content.get(pos));
      }
      negateRange(buffer, startIndex, lastIndex, firstOfRange, lastOfRange);
    } else { // no expansion needed
      negateRange(buffer, startIndex, lastIndex, firstOfRange, lastOfRange);
      if (cardinalityChange < 0) {
        // Leave array oversize
        for (int i = startIndex + newValuesInRange; i < newCardinality; ++i) {
          content.put(i, content.get(i - cardinalityChange));
        }
      }
    }
    cardinality = newCardinality;
    return this;
  }

  @Override
  public boolean intersects(MappeableArrayContainer value2) {
    MappeableArrayContainer value1 = this;
    return BufferUtil.unsignedIntersects(value1.content, value1.getCardinality(), value2.content,
        value2.getCardinality());
  }

  @Override
  public boolean intersects(MappeableBitmapContainer x) {
    return x.intersects(this);
  }

  @Override
  public boolean intersects(MappeableRunContainer x) {
    return x.intersects(this);
  }

  @Override
  public MappeableContainer ior(final MappeableArrayContainer value2) {
    final int totalCardinality = getCardinality() + value2.getCardinality();
    if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
      final MappeableBitmapContainer bc = new MappeableBitmapContainer();
      if (!BufferUtil.isBackedBySimpleArray(bc.bitmap)) {
        throw new RuntimeException("Should not happen. Internal bug.");
      }
      long[] bitArray = bc.bitmap.array();
      if (BufferUtil.isBackedBySimpleArray(value2.content)) {
        short[] sarray = value2.content.array();
        for (int k = 0; k < value2.cardinality; ++k) {
          short v = sarray[k];
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] |= (1L << v);
        }
      } else {
        for (int k = 0; k < value2.cardinality; ++k) {
          short v2 = value2.content.get(k);
          final int i = toIntUnsigned(v2) >>> 6;
          bitArray[i] |= (1L << v2);
        }
      }
      if (BufferUtil.isBackedBySimpleArray(content)) {
        short[] sarray = content.array();
        for (int k = 0; k < cardinality; ++k) {
          short v = sarray[k];
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] |= (1L << v);
        }
      } else {
        for (int k = 0; k < cardinality; ++k) {
          short v = content.get(k);
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] |= (1L << v);
        }
      }
      bc.cardinality = 0;
      int len = bc.bitmap.limit();
      for (int index = 0; index < len; ++index) {
        bc.cardinality += Long.bitCount(bitArray[index]);
      }
      if (bc.cardinality <= DEFAULT_MAX_SIZE) {
        return bc.toArrayContainer();
      } else if (bc.isFull()) {
        return MappeableRunContainer.full();
      }
      return bc;
    }
    if (totalCardinality >= content.limit()) {
      int newCapacity = calculateCapacity(totalCardinality);
      ShortBuffer destination = ShortBuffer.allocate(newCapacity);

      if (BufferUtil.isBackedBySimpleArray(content)
          && BufferUtil.isBackedBySimpleArray(value2.content)) {
        cardinality =
            Util.unsignedUnion2by2(content.array(), 0, cardinality, value2.content.array(), 0,
                value2.cardinality, destination.array());
      } else {
        cardinality =
            BufferUtil.unsignedUnion2by2(content, 0, cardinality, value2.content, 0,
                value2.cardinality, destination.array());
      }
      this.content = destination;
    } else {
      BufferUtil.arraycopy(content, 0, content, value2.cardinality, cardinality);

      if (BufferUtil.isBackedBySimpleArray(content)
          && BufferUtil.isBackedBySimpleArray(value2.content)) {
        cardinality =
            Util.unsignedUnion2by2(content.array(), value2.cardinality, cardinality,
                value2.content.array(), 0, value2.cardinality, content.array());
      } else {
        cardinality =
            BufferUtil.unsignedUnion2by2(content, value2.cardinality, cardinality, value2.content,
                0, value2.cardinality, content.array());
      }
    }
    return this;
  }

  @Override
  public MappeableContainer ior(MappeableBitmapContainer x) {
    return x.or(this);
  }

  @Override
  public MappeableContainer ior(final MappeableRunContainer value2) {
    // not inplace
    return value2.or(this);
  }

  @Override
  public MappeableContainer iremove(int begin, int end) {
    if (end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int indexstart = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) begin);
    if (indexstart < 0) {
      indexstart = -indexstart - 1;
    }
    int indexend = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) (end - 1));
    if (indexend < 0) {
      indexend = -indexend - 1;
    } else {
      indexend++;
    }
    int rangelength = indexend - indexstart;
    BufferUtil.arraycopy(content, indexstart + rangelength, content, indexstart,
        cardinality - indexstart - rangelength);
    cardinality -= rangelength;
    return this;
  }

  @Override
  protected boolean isArrayBacked() {
    return BufferUtil.isBackedBySimpleArray(this.content);
  }


  @Override
  public Iterator<Short> iterator() {

    return new Iterator<Short>() {
      short pos = 0;

      @Override
      public boolean hasNext() {
        return pos < MappeableArrayContainer.this.cardinality;
      }

      @Override
      public Short next() {
        return MappeableArrayContainer.this.content.get(pos++);
      }

      @Override
      public void remove() {
        MappeableArrayContainer.this.removeAtIndex(pos - 1);
        pos--;
      }
    };
  }

  @Override
  public MappeableContainer ixor(final MappeableArrayContainer value2) {
    return this.xor(value2);
  }


  @Override
  public MappeableContainer ixor(MappeableBitmapContainer x) {
    return x.xor(this);
  }

  @Override
  public MappeableContainer ixor(final MappeableRunContainer value2) {
    return value2.xor(this);
  }

  @Override
  public MappeableContainer limit(int maxcardinality) {
    if (maxcardinality < this.getCardinality()) {
      return new MappeableArrayContainer(maxcardinality, this.content);
    } else {
      return clone();
    }
  }


  protected void loadData(final MappeableBitmapContainer bitmapContainer) {
    this.cardinality = bitmapContainer.cardinality;
    if (!BufferUtil.isBackedBySimpleArray(this.content)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    bitmapContainer.fillArray(content.array());
  }

  // for use in inot range known to be nonempty
  private void negateRange(final ShortBuffer buffer, final int startIndex, final int lastIndex,
      final int startRange, final int lastRange) {
    // compute the negation into buffer

    int outPos = 0;
    int inPos = startIndex; // value here always >= valInRange,
    // until it is exhausted
    // n.b., we can start initially exhausted.

    int valInRange = startRange;
    for (; valInRange < lastRange && inPos <= lastIndex; ++valInRange) {
      if ((short) valInRange != content.get(inPos)) {
        buffer.put(outPos++, (short) valInRange);
      } else {
        ++inPos;
      }
    }

    // if there are extra items (greater than the biggest
    // pre-existing one in range), buffer them
    for (; valInRange < lastRange; ++valInRange) {
      buffer.put(outPos++, (short) valInRange);
    }

    if (outPos != buffer.limit()) {
      throw new RuntimeException(
          "negateRange: outPos " + outPos + " whereas buffer.length=" + buffer.limit());
    }
    assert outPos == buffer.limit();
    // copy back from buffer...caller must ensure there is room
    int i = startIndex;
    int len = buffer.limit();
    for (int k = 0; k < len; ++k) {
      final short item = buffer.get(k);
      content.put(i++, item);
    }
  }

  // shares lots of code with inot; candidate for refactoring
  @Override
  public MappeableContainer not(final int firstOfRange, final int lastOfRange) {
    // TODO: may need to convert to a RunContainer
    // TODO: this can be optimized for performance
    if (firstOfRange >= lastOfRange) {
      return clone(); // empty range
    }

    // determine the span of array indices to be affected
    int startIndex = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) firstOfRange);
    if (startIndex < 0) {
      startIndex = -startIndex - 1;
    }
    int lastIndex =
        BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) (lastOfRange - 1));
    if (lastIndex < 0) {
      lastIndex = -lastIndex - 2;
    }
    final int currentValuesInRange = lastIndex - startIndex + 1;
    final int spanToBeFlipped = lastOfRange - firstOfRange;
    final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
    final int cardinalityChange = newValuesInRange - currentValuesInRange;
    final int newCardinality = cardinality + cardinalityChange;

    if (newCardinality > DEFAULT_MAX_SIZE) {
      return toBitmapContainer().not(firstOfRange, lastOfRange);
    }

    final MappeableArrayContainer answer = new MappeableArrayContainer(newCardinality);
    if (!BufferUtil.isBackedBySimpleArray(answer.content)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    short[] sarray = answer.content.array();

    for (int i = 0; i < startIndex; ++i) {
      // copy stuff before the active area
      sarray[i] = content.get(i);
    }

    int outPos = startIndex;
    int inPos = startIndex; // item at inPos always >= valInRange

    int valInRange = firstOfRange;
    for (; valInRange < lastOfRange && inPos <= lastIndex; ++valInRange) {
      if ((short) valInRange != content.get(inPos)) {
        sarray[outPos++] = (short) valInRange;
      } else {
        ++inPos;
      }
    }

    for (; valInRange < lastOfRange; ++valInRange) {
      answer.content.put(outPos++, (short) valInRange);
    }

    // content after the active range
    for (int i = lastIndex + 1; i < cardinality; ++i) {
      answer.content.put(outPos++, content.get(i));
    }
    answer.cardinality = newCardinality;
    return answer;
  }

  @Override
  int numberOfRuns() {
    if (cardinality == 0) {
      return 0; // should never happen
    }

    if (BufferUtil.isBackedBySimpleArray(content)) {
      short[] c = content.array();
      int numRuns = 1;
      int oldv = toIntUnsigned(c[0]);
      for (int i = 1; i < cardinality; i++) {
        int newv = toIntUnsigned(c[i]);
        if (oldv + 1 != newv) {
          ++numRuns;
        }
        oldv = newv;
      }
      return numRuns;
    } else {
      int numRuns = 1;
      int previous = toIntUnsigned(content.get(0));
      // we do not proceed like above for fear that calling "get" twice per loop would be too much
      for (int i = 1; i < cardinality; i++) {
        int val = toIntUnsigned(content.get(i));
        if (val != previous + 1) {
          ++numRuns;
        }
        previous = val;
      }
      return numRuns;
    }
  }


  @Override
  public MappeableContainer or(final MappeableArrayContainer value2) {
    final MappeableArrayContainer value1 = this;
    final int totalCardinality = value1.getCardinality() + value2.getCardinality();
    if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
      final MappeableBitmapContainer bc = new MappeableBitmapContainer();
      if (!BufferUtil.isBackedBySimpleArray(bc.bitmap)) {
        throw new RuntimeException("Should not happen. Internal bug.");
      }
      long[] bitArray = bc.bitmap.array();
      if (BufferUtil.isBackedBySimpleArray(value2.content)) {
        short[] sarray = value2.content.array();
        for (int k = 0; k < value2.cardinality; ++k) {
          short v = sarray[k];
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] |= (1L << v);
        }
      } else {
        for (int k = 0; k < value2.cardinality; ++k) {
          short v2 = value2.content.get(k);
          final int i = toIntUnsigned(v2) >>> 6;
          bitArray[i] |= (1L << v2);
        }
      }
      if (BufferUtil.isBackedBySimpleArray(this.content)) {
        short[] sarray = this.content.array();
        for (int k = 0; k < this.cardinality; ++k) {
          short v = sarray[k];
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] |= (1L << v);
        }
      } else {
        for (int k = 0; k < this.cardinality; ++k) {
          short v = this.content.get(k);
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] |= (1L << v);
        }
      }
      bc.cardinality = 0;
      int len = bc.bitmap.limit();
      for (int index = 0; index < len; ++index) {
        bc.cardinality += Long.bitCount(bitArray[index]);
      }
      if (bc.cardinality <= DEFAULT_MAX_SIZE) {
        return bc.toArrayContainer();
      } else if (bc.isFull()) {
        return MappeableRunContainer.full();
      }
      return bc;
    }
    final MappeableArrayContainer answer = new MappeableArrayContainer(totalCardinality);
    if (BufferUtil.isBackedBySimpleArray(value1.content)
        && BufferUtil.isBackedBySimpleArray(value2.content)) {
      answer.cardinality =
          Util.unsignedUnion2by2(
                  value1.content.array(), 0, value1.getCardinality(),
                  value2.content.array(), 0, value2.getCardinality(),
                  answer.content.array()
          );
    } else {
      answer.cardinality =
              BufferUtil.unsignedUnion2by2(
                      value1.content, 0, value1.getCardinality(),
                      value2.content, 0, value2.getCardinality(),
                      answer.content.array()
              );
    }
    return answer;
  }

  protected MappeableContainer lazyor(final MappeableArrayContainer value2) {
    final MappeableArrayContainer value1 = this;
    final int totalCardinality = value1.getCardinality() + value2.getCardinality();
    if (totalCardinality > ARRAY_LAZY_LOWERBOUND) {// it could be a bitmap!
      final MappeableBitmapContainer bc = new MappeableBitmapContainer();
      if (!BufferUtil.isBackedBySimpleArray(bc.bitmap)) {
        throw new RuntimeException("Should not happen. Internal bug.");
      }
      long[] bitArray = bc.bitmap.array();
      if (BufferUtil.isBackedBySimpleArray(value2.content)) {
        short[] sarray = value2.content.array();
        for (int k = 0; k < value2.cardinality; ++k) {
          short v = sarray[k];
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] |= (1L << v);
        }
      } else {
        for (int k = 0; k < value2.cardinality; ++k) {
          short v2 = value2.content.get(k);
          final int i = toIntUnsigned(v2) >>> 6;
          bitArray[i] |= (1L << v2);
        }
      }
      if (BufferUtil.isBackedBySimpleArray(this.content)) {
        short[] sarray = this.content.array();
        for (int k = 0; k < this.cardinality; ++k) {
          short v = sarray[k];
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] |= (1L << v);
        }
      } else {
        for (int k = 0; k < this.cardinality; ++k) {
          short v = this.content.get(k);
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] |= (1L << v);
        }
      }
      bc.cardinality = -1;
      return bc;
    }
    final MappeableArrayContainer answer = new MappeableArrayContainer(totalCardinality);
    if (BufferUtil.isBackedBySimpleArray(value1.content)
        && BufferUtil.isBackedBySimpleArray(value2.content)) {
      answer.cardinality =
              Util.unsignedUnion2by2(
                      value1.content.array(), 0, value1.getCardinality(),
                      value2.content.array(), 0, value2.getCardinality(),
                      answer.content.array()
              );
    } else {
      answer.cardinality =
              BufferUtil.unsignedUnion2by2(
                      value1.content, 0, value1.getCardinality(),
                      value2.content, 0, value2.getCardinality(),
                      answer.content.array()
              );
    }
    return answer;
  }


  @Override
  public MappeableContainer or(MappeableBitmapContainer x) {
    return x.or(this);
  }

  @Override
  public MappeableContainer or(final MappeableRunContainer value2) {
    return value2.or(this);
  }

  protected MappeableContainer or(ShortIterator it) {
    return or(it, false);
  }

  /**
   * it must return items in (unsigned) sorted order. Possible candidate for Container interface?
   **/
  private MappeableContainer or(ShortIterator it, boolean exclusive) {
    MappeableArrayContainer ac = new MappeableArrayContainer();
    int myItPos = 0;
    ac.cardinality = 0;
    // do a merge. int -1 denotes end of input.
    int myHead = (myItPos == cardinality) ? -1 : toIntUnsigned(content.get(myItPos++));
    int hisHead = advance(it);

    while (myHead != -1 && hisHead != -1) {
      if (myHead < hisHead) {
        ac.emit((short) myHead);
        myHead = (myItPos == cardinality) ? -1 : toIntUnsigned(content.get(myItPos++));
      } else if (myHead > hisHead) {
        ac.emit((short) hisHead);
        hisHead = advance(it);
      } else {
        if (!exclusive) {
          ac.emit((short) hisHead);
        }
        hisHead = advance(it);
        myHead = (myItPos == cardinality) ? -1 : toIntUnsigned(content.get(myItPos++));
      }
    }

    while (myHead != -1) {
      ac.emit((short) myHead);
      myHead = (myItPos == cardinality) ? -1 : toIntUnsigned(content.get(myItPos++));
    }

    while (hisHead != -1) {
      ac.emit((short) hisHead);
      hisHead = advance(it);
    }

    if (ac.cardinality > DEFAULT_MAX_SIZE) {
      return ac.toBitmapContainer();
    } else {
      return ac;
    }
  }

  @Override
  public int rank(short lowbits) {
    int answer = BufferUtil.unsignedBinarySearch(content, 0, cardinality, lowbits);
    if (answer >= 0) {
      return answer + 1;
    } else {
      return -answer - 1;
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // little endian
    this.cardinality = 0xFFFF & Short.reverseBytes(in.readShort());
    if (this.content.limit() < this.cardinality) {
      this.content = ShortBuffer.allocate(this.cardinality);
    }
    for (int k = 0; k < this.cardinality; ++k) {
      this.content.put(k, Short.reverseBytes(in.readShort()));
    }
  }

  @Override
  public MappeableContainer remove(int begin, int end) {
    if (end == begin) {
      return clone();
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int indexstart = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) begin);
    if (indexstart < 0) {
      indexstart = -indexstart - 1;
    }
    int indexend = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short) (end - 1));
    if (indexend < 0) {
      indexend = -indexend - 1;
    } else {
      indexend++;
    }
    int rangelength = indexend - indexstart;
    MappeableArrayContainer answer = clone();
    BufferUtil.arraycopy(content, indexstart + rangelength, answer.content, indexstart,
        cardinality - indexstart - rangelength);
    answer.cardinality = cardinality - rangelength;
    return answer;
  }

  protected void removeAtIndex(final int loc) {
    System.arraycopy(content.array(), loc + 1, content.array(), loc, cardinality - loc - 1);
    --cardinality;
  }


  @Override
  public MappeableContainer remove(final short x) {
    if (BufferUtil.isBackedBySimpleArray(this.content)) {
      final int loc = Util.unsignedBinarySearch(content.array(), 0, cardinality, x);
      if (loc >= 0) {
        removeAtIndex(loc);
      }
      return this;
    } else {
      final int loc = BufferUtil.unsignedBinarySearch(content, 0, cardinality, x);
      if (loc >= 0) {
        // insertion
        for (int k = loc + 1; k < cardinality; --k) {
          content.put(k - 1, content.get(k));
        }
        --cardinality;
      }
      return this;

    }
  }

  @Override
  public MappeableContainer repairAfterLazy() {
    return this;
  }


  @Override
  public MappeableContainer runOptimize() {
    int numRuns = numberOfRuns();
    int sizeAsRunContainer = MappeableRunContainer.getArraySizeInBytes(numRuns);
    if (getArraySizeInBytes() > sizeAsRunContainer) {
      return new MappeableRunContainer(this, numRuns); // this could be
                                                       // maybe faster if
                                                       // initial
                                                       // container is a
                                                       // bitmap
    } else {
      return this;
    }
  }

  @Override
  public short select(int j) {
    return this.content.get(j);
  }

  @Override
  public int serializedSizeInBytes() {
    return serializedSizeInBytes(cardinality);
  }

  /**
   * Copies the data in a bitmap container.
   *
   * @return the bitmap container
   */
  @Override
  public MappeableBitmapContainer toBitmapContainer() {
    final MappeableBitmapContainer bc = new MappeableBitmapContainer();
    bc.loadData(this);
    return bc;
  }

  @Override
  public int first() {
    assertNonEmpty(cardinality == 0);
    return toIntUnsigned(this.select(0));
  }

  @Override
  public int last() {
    assertNonEmpty(cardinality == 0);
    return toIntUnsigned(select(cardinality - 1));
  }

  @Override
  public int nextValue(short fromValue) {
    int index = BufferUtil.advanceUntil(content, -1, cardinality, fromValue);
    if (index == cardinality) {
      return fromValue == content.get(cardinality - 1) ? toIntUnsigned(fromValue) : -1;
    }
    return toIntUnsigned(content.get(index));
  }

  @Override
  public int previousValue(short fromValue) {
    int index = BufferUtil.advanceUntil(content, -1, cardinality, fromValue);
    if (index != cardinality && content.get(index) == fromValue) {
      return toIntUnsigned(content.get(index));
    }
    return index == 0 ? -1 : toIntUnsigned(content.get(index - 1));
  }

  @Override
  public int nextAbsentValue(short fromValue) {
    int index = BufferUtil.advanceUntil(content, -1, cardinality, fromValue);
    int value = toIntUnsigned(fromValue);
    if (index >= cardinality) {
      return value;
    }
    if (index == cardinality - 1) {
      return fromValue == content.get(cardinality - 1) ? value + 1 : value;
    }
    if (content.get(index) != fromValue) {
      return value;
    }
    if (content.get(index + 1) > fromValue + 1) {
      return value + 1;
    }

    int low = index;
    int high = cardinality;

    while (low + 1 < high) {
      int mid = (high + low) >>> 1;
      if (mid - index < toIntUnsigned(content.get(mid)) - value) {
        high = mid;
      } else {
        low = mid;
      }
    }

    if (low == cardinality - 1) {
      return toIntUnsigned(content.get(cardinality - 1)) + 1;
    }

    assert toIntUnsigned(content.get(low)) + 1 < toIntUnsigned(content.get(high));
    assert toIntUnsigned(content.get(low)) == value + (low - index);
    return toIntUnsigned(content.get(low)) + 1;
  }

  @Override
  public int previousAbsentValue(short fromValue) {
    int index = BufferUtil.advanceUntil(content, -1, cardinality, fromValue);
    int value = toIntUnsigned(fromValue);
    if (index >= cardinality) {
      return value;
    }
    if (index == 0) {
      return fromValue == content.get(0) ? value - 1 : value;
    }
    if (content.get(index) != fromValue) {
      return value;
    }
    if (content.get(index - 1) < fromValue - 1) {
      return value - 1;
    }

    int low = -1;
    int high = index;

    // Binary search for the first index which differs by at least 2 from its
    // successor
    while (low + 1 < high) {
      int mid = (high + low) >>> 1;
      if (index - mid < value - toIntUnsigned(content.get(mid))) {
        low = mid;
      } else {
        high = mid;
      }
    }

    if (high == 0) {
      return toIntUnsigned(content.get(0)) - 1;
    }

    assert toIntUnsigned(content.get(low)) + 1 < toIntUnsigned(content.get(high));
    assert toIntUnsigned(content.get(high)) == value - (index - high);
    return toIntUnsigned(content.get(high)) - 1;
  }

  @Override
  public Container toContainer() {
    return new ArrayContainer(this);
  }

  /**
   * Create a copy of the content of this container as a short array. This creates a copy.
   *
   * @return copy of the content as a short array
   */
  public short[] toShortArray() {
    short[] answer = new short[cardinality];
    content.rewind();
    content.get(answer);
    return answer;
  }

  @Override
  public String toString() {
    if (this.cardinality == 0) {
      return "{}";
    }
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (int i = 0; i < this.cardinality - 1; i++) {
      sb.append(toIntUnsigned(this.content.get(i)));
      sb.append(",");
    }
    sb.append(toIntUnsigned(this.content.get(this.cardinality - 1)));
    sb.append("}");
    return sb.toString();
  }

  @Override
  public void trim() {
    if (this.content.limit() == this.cardinality) {
      return;
    }
    if (BufferUtil.isBackedBySimpleArray(content)) {
      this.content = ShortBuffer.wrap(Arrays.copyOf(content.array(), cardinality));
    } else {
      final ShortBuffer co = ShortBuffer.allocate(this.cardinality);
      // can assume that new co is array backed
      short[] x = co.array();
      for (int k = 0; k < this.cardinality; ++k) {
        x[k] = this.content.get(k);
      }
      this.content = co;
    }
  }

  @Override
  protected void writeArray(DataOutput out) throws IOException {
    // little endian
    if (BufferUtil.isBackedBySimpleArray(content)) {
      short[] a = content.array();
      for (int k = 0; k < this.cardinality; ++k) {
        out.writeShort(Short.reverseBytes(a[k]));
      }
    } else {
      for (int k = 0; k < this.cardinality; ++k) {
        out.writeShort(Short.reverseBytes(content.get(k)));
      }
    }
  }

  @Override
  protected void writeArray(ByteBuffer buffer) {
    assert buffer.order() == ByteOrder.LITTLE_ENDIAN;
    ShortBuffer target = buffer.asShortBuffer();
    ShortBuffer source = content.duplicate();
    source.position(0);
    source.limit(cardinality);
    target.put(source);
    int bytesWritten = 2 * cardinality;
    buffer.position(buffer.position() + bytesWritten);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.write(this.cardinality & 0xFF);
    out.write((this.cardinality >>> 8) & 0xFF);
    if (BufferUtil.isBackedBySimpleArray(content)) {
      short[] a = content.array();
      for (int k = 0; k < this.cardinality; ++k) {
        out.writeShort(Short.reverseBytes(a[k]));
      }
    } else {
      for (int k = 0; k < this.cardinality; ++k) {
        out.writeShort(Short.reverseBytes(content.get(k)));
      }
    }
  }

  @Override
  public MappeableContainer xor(final MappeableArrayContainer value2) {
    final MappeableArrayContainer value1 = this;
    final int totalCardinality = value1.getCardinality() + value2.getCardinality();
    if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
      final MappeableBitmapContainer bc = new MappeableBitmapContainer();
      if (!BufferUtil.isBackedBySimpleArray(bc.bitmap)) {
        throw new RuntimeException("Should not happen. Internal bug.");
      }
      long[] bitArray = bc.bitmap.array();
      if (BufferUtil.isBackedBySimpleArray(value2.content)) {
        short[] sarray = value2.content.array();
        for (int k = 0; k < value2.cardinality; ++k) {
          short v = sarray[k];
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] ^= (1L << v);
        }
      } else {
        for (int k = 0; k < value2.cardinality; ++k) {
          short v2 = value2.content.get(k);
          final int i = toIntUnsigned(v2) >>> 6;
          bitArray[i] ^= (1L << v2);
        }
      }
      if (BufferUtil.isBackedBySimpleArray(this.content)) {
        short[] sarray = this.content.array();
        for (int k = 0; k < this.cardinality; ++k) {
          short v = sarray[k];
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] ^= (1L << v);
        }
      } else {
        for (int k = 0; k < this.cardinality; ++k) {
          short v = this.content.get(k);
          final int i = toIntUnsigned(v) >>> 6;
          bitArray[i] ^= (1L << v);
        }
      }

      bc.cardinality = 0;
      int len = bc.bitmap.limit();
      for (int index = 0; index < len; ++index) {
        bc.cardinality += Long.bitCount(bitArray[index]);
      }
      if (bc.cardinality <= DEFAULT_MAX_SIZE) {
        return bc.toArrayContainer();
      }
      return bc;
    }
    final MappeableArrayContainer answer = new MappeableArrayContainer(totalCardinality);
    if (BufferUtil.isBackedBySimpleArray(value1.content)
        && BufferUtil.isBackedBySimpleArray(value2.content)) {
      answer.cardinality = org.roaringbitmap.Util.unsignedExclusiveUnion2by2(value1.content.array(),
          value1.getCardinality(), value2.content.array(), value2.getCardinality(),
          answer.content.array());
    } else {
      answer.cardinality = BufferUtil.unsignedExclusiveUnion2by2(value1.content,
          value1.getCardinality(), value2.content, value2.getCardinality(), answer.content.array());
    }
    return answer;
  }

  @Override
  public MappeableContainer xor(MappeableBitmapContainer x) {
    return x.xor(this);
  }

  @Override
  public MappeableContainer xor(final MappeableRunContainer value2) {
    return value2.xor(this);
  }


  protected MappeableContainer xor(ShortIterator it) {
    return or(it, true);
  }

  @Override
  public void forEach(short msb, IntConsumer ic) {
    int high = ((int) msb) << 16;
    if (BufferUtil.isBackedBySimpleArray(content)) {
      short[] c = content.array();
      for (int k = 0; k < cardinality; ++k) {
        ic.accept((c[k] & 0xFFFF) | high);
      }
    } else {
      for (int k = 0; k < cardinality; ++k) {
        ic.accept((content.get(k) & 0xFFFF) | high);
      }
    }
  }

  @Override
  public int andCardinality(MappeableArrayContainer value2) {
    if (BufferUtil.isBackedBySimpleArray(content)
        && BufferUtil.isBackedBySimpleArray(value2.content)) {
      return Util.unsignedLocalIntersect2by2Cardinality(content.array(), cardinality,
          value2.content.array(), value2.getCardinality());
    }
    return BufferUtil.unsignedLocalIntersect2by2Cardinality(content, cardinality,
        value2.content, value2.getCardinality());
  }

  @Override
  public int andCardinality(MappeableBitmapContainer x) {
    return x.andCardinality(this);
  }

  @Override
  // see andNot for an approach that might be better.
  public int andCardinality(MappeableRunContainer x) {
    return x.andCardinality(this);
  }

  @Override
  protected boolean contains(MappeableRunContainer runContainer) {
    if (runContainer.getCardinality() > cardinality) {
      return false;
    }

    for (int i = 0; i < runContainer.numberOfRuns(); ++i) {
      int start = toIntUnsigned(runContainer.getValue(i));
      int length = toIntUnsigned(runContainer.getLength(i));
      if (!contains(start, start + length)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean contains(MappeableArrayContainer arrayContainer) {
    if (cardinality < arrayContainer.cardinality) {
      return false;
    }
    int i1 = 0, i2 = 0;
    while(i1 < cardinality && i2 < arrayContainer.cardinality) {
      if(content.get(i1) == arrayContainer.content.get(i2)) {
        ++i1;
        ++i2;
      } else if(BufferUtil.compareUnsigned(content.get(i1), arrayContainer.content.get(i2)) < 0) {
        ++i1;
      } else {
        return false;
      }
    }
    return i2 == arrayContainer.cardinality;
  }

  @Override
  protected boolean contains(MappeableBitmapContainer bitmapContainer) {
    return false;
  }

  @Override
  public boolean intersects(int minimum, int supremum) {
    int pos = BufferUtil.unsignedBinarySearch(content, 0, cardinality, (short)minimum);
    int index = pos >= 0 ? pos : -pos - 1;
    return index < cardinality && BufferUtil.toIntUnsigned(content.get(index)) < supremum;
  }

  @Override
  public boolean contains(int minimum, int supremum) {
    int maximum = supremum - 1;
    int start = BufferUtil.advanceUntil(content, -1, cardinality, (short)minimum);
    int end = BufferUtil.advanceUntil(content, start - 1, cardinality, (short)maximum);
    return start < cardinality
            && end < cardinality
            && end - start == maximum - minimum
            && content.get(start) == (short)minimum
            && content.get(end) == (short)maximum;
  }

}


final class MappeableArrayContainerShortIterator implements PeekableShortIterator {
  int pos;
  MappeableArrayContainer parent;

  MappeableArrayContainerShortIterator() {}


  MappeableArrayContainerShortIterator(MappeableArrayContainer p) {
    wrap(p);
  }

  @Override
  public void advanceIfNeeded(short minval) {
    pos = BufferUtil.advanceUntil(parent.content, pos - 1, parent.cardinality, minval);
  }

  @Override
  public PeekableShortIterator clone() {
    try {
      return (PeekableShortIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return pos < parent.cardinality;
  }

  @Override
  public short next() {
    return parent.content.get(pos++);
  }


  @Override
  public int nextAsInt() {
    return toIntUnsigned(parent.content.get(pos++));
  }

  @Override
  public short peekNext() {
    return parent.content.get(pos);
  }


  @Override
  public void remove() {
    parent.removeAtIndex(pos - 1);
    pos--;
  }

  void wrap(MappeableArrayContainer p) {
    parent = p;
    pos = 0;
  }



}


final class RawArrayContainerShortIterator implements PeekableShortIterator {
  int pos;
  MappeableArrayContainer parent;
  short[] content;


  RawArrayContainerShortIterator(MappeableArrayContainer p) {
    parent = p;
    if (!p.isArrayBacked()) {
      throw new RuntimeException("internal bug");
    }
    content = p.content.array();
    pos = 0;
  }


  @Override
  public void advanceIfNeeded(short minval) {
    pos = Util.advanceUntil(content, pos - 1, parent.cardinality, minval);
  }

  @Override
  public PeekableShortIterator clone() {
    try {
      return (PeekableShortIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return pos < parent.cardinality;
  }

  @Override
  public short next() {
    return content[pos++];
  }

  @Override
  public int nextAsInt() {
    return toIntUnsigned(content[pos++]);
  }

  @Override
  public short peekNext() {
    return content[pos];
  }

  @Override
  public void remove() {
    parent.removeAtIndex(pos - 1);
    pos--;
  }

}


final class RawReverseArrayContainerShortIterator implements ShortIterator {
  int pos;
  MappeableArrayContainer parent;
  short[] content;


  RawReverseArrayContainerShortIterator(MappeableArrayContainer p) {
    parent = p;
    if (!p.isArrayBacked()) {
      throw new RuntimeException("internal bug");
    }
    content = p.content.array();
    pos = parent.cardinality - 1;
  }

  @Override
  public ShortIterator clone() {
    try {
      return (ShortIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return pos >= 0;
  }


  @Override
  public short next() {
    return content[pos--];
  }

  @Override
  public int nextAsInt() {
    return toIntUnsigned(content[pos--]);
  }

  @Override
  public void remove() {
    parent.removeAtIndex(pos + 1);
    pos++;
  }

}


final class ReverseMappeableArrayContainerShortIterator implements ShortIterator {

  int pos;

  MappeableArrayContainer parent;

  ReverseMappeableArrayContainerShortIterator() {}


  ReverseMappeableArrayContainerShortIterator(MappeableArrayContainer p) {
    wrap(p);
  }

  @Override
  public ShortIterator clone() {
    try {
      return (ShortIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return pos >= 0;
  }

  @Override
  public short next() {
    return parent.content.get(pos--);
  }


  @Override
  public int nextAsInt() {
    return toIntUnsigned(parent.content.get(pos--));
  }

  @Override
  public void remove() {
    parent.removeAtIndex(pos + 1);
    pos++;
  }

  void wrap(MappeableArrayContainer p) {
    parent = p;
    pos = parent.cardinality - 1;
  }

}
