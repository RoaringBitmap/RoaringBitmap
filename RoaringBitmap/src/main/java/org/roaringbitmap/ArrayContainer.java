/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import org.roaringbitmap.buffer.MappeableArrayContainer;
import org.roaringbitmap.buffer.MappeableContainer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Iterator;




/**
 * Simple container made of an array of 16-bit integers
 */
public final class ArrayContainer extends Container implements Cloneable {
  private static final int DEFAULT_INIT_SIZE = 4;
  private static final int ARRAY_LAZY_LOWERBOUND = 1024;

  static final int DEFAULT_MAX_SIZE = 4096;// containers with DEFAULT_MAX_SZE or less integers
                                           // should be ArrayContainers

  private static final long serialVersionUID = 1L;

  protected static int serializedSizeInBytes(int cardinality) {
    return cardinality * 2 + 2;
  }

  protected int cardinality = 0;

  char[] content;


  /**
   * Create an array container with default capacity
   */
  public ArrayContainer() {
    this(DEFAULT_INIT_SIZE);
  }

  public static ArrayContainer empty() {
    return new ArrayContainer();
  }
  /**
   * Create an array container with specified capacity
   *
   * @param capacity The capacity of the container
   */
  public ArrayContainer(final int capacity) {
    content = new char[capacity];
  }

  /**
   * Create an array container with a run of ones from firstOfRun to lastOfRun, inclusive. Caller is
   * responsible for making sure the range is small enough that ArrayContainer is appropriate.
   *
   * @param firstOfRun first index
   * @param lastOfRun last index (range is exclusive)
   */
  public ArrayContainer(final int firstOfRun, final int lastOfRun) {
    final int valuesInRange = lastOfRun - firstOfRun;
    this.content = new char[valuesInRange];
    for (int i = 0; i < valuesInRange; ++i) {
      content[i] = (char) (firstOfRun + i);
    }
    cardinality = valuesInRange;
  }

  /**
   * Create a new container from existing values array. This copies the data.
   *
   * @param newCard desired cardinality
   * @param newContent actual values (length should equal or exceed cardinality)
   */
  public ArrayContainer(int newCard, char[] newContent) {
    this.cardinality = newCard;
    this.content = Arrays.copyOf(newContent, newCard);
  }

  /**
   * Creates a new non-mappeable container from a mappeable one. This copies the data.
   *
   * @param bc the original container
   */
  public ArrayContainer(MappeableArrayContainer bc) {
    this.cardinality = bc.getCardinality();
    this.content = bc.toShortArray();
  }

  public ArrayContainer(char[] newContent) {
    this.cardinality = newContent.length;
    this.content = newContent;
  }

  @Override
  public Container add(int begin, int end) {
    if(end == begin) {
      return clone();
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    // TODO: may need to convert to a RunContainer
    int indexstart = Util.unsignedBinarySearch(content, 0, cardinality, (char) begin);
    if (indexstart < 0) {
      indexstart = -indexstart - 1;
    }
    int indexend = Util.unsignedBinarySearch(content, 0, cardinality, (char) (end - 1));
    if (indexend < 0) {
      indexend = -indexend - 1;
    } else {
      indexend++;
    }
    int rangelength = end - begin;
    int newcardinality = indexstart + (cardinality - indexend) + rangelength;
    if (newcardinality > DEFAULT_MAX_SIZE) {
      BitmapContainer a = this.toBitmapContainer();
      return a.iadd(begin, end);
    }
    ArrayContainer answer = new ArrayContainer(newcardinality, content);
    System.arraycopy(content, indexend, answer.content, indexstart + rangelength,
        cardinality - indexend);
    for (int k = 0; k < rangelength; ++k) {
      answer.content[k + indexstart] = (char) (begin + k);
    }
    answer.cardinality = newcardinality;
    return answer;
  }



  /**
   * running time is in O(n) time if insert is not in order.
   */
  @Override
  public Container add(final char x) {
    if (cardinality == 0 || (cardinality > 0
            && (x) > (content[cardinality - 1]))) {
      if (cardinality >= DEFAULT_MAX_SIZE) {
        return toBitmapContainer().add(x);
      }
      if (cardinality >= this.content.length) {
        increaseCapacity();
      }
      content[cardinality++] = x;
    } else {
      int loc = Util.unsignedBinarySearch(content, 0, cardinality, x);
      if (loc < 0) {
        // Transform the ArrayContainer to a BitmapContainer
        // when cardinality = DEFAULT_MAX_SIZE
        if (cardinality >= DEFAULT_MAX_SIZE) {
          return toBitmapContainer().add(x);
        }
        if (cardinality >= this.content.length) {
          increaseCapacity();
        }
        // insertion : shift the elements > x by one position to
        // the right
        // and put x in it's appropriate place
        System.arraycopy(content, -loc - 1, content, -loc, cardinality + loc + 1);
        content[-loc - 1] = x;
        ++cardinality;
      }
    }
    return this;
  }

  private int advance(CharIterator it) {
    if (it.hasNext()) {
      return (it.next());
    } else {
      return -1;
    }
  }

  @Override
  public ArrayContainer and(final ArrayContainer value2) {
    ArrayContainer value1 = this;
    final int desiredCapacity = Math.min(value1.getCardinality(), value2.getCardinality());
    ArrayContainer answer = new ArrayContainer(desiredCapacity);
    answer.cardinality = Util.unsignedIntersect2by2(value1.content, value1.getCardinality(),
        value2.content, value2.getCardinality(), answer.content);
    return answer;
  }

  @Override
  public Container and(BitmapContainer x) {
    return x.and(this);
  }

  @Override
  // see andNot for an approach that might be better.
  public Container and(RunContainer x) {
    return x.and(this);
  }

  @Override
  public int andCardinality(final ArrayContainer value2) {
    return Util.unsignedLocalIntersect2by2Cardinality(content, cardinality, value2.content,
        value2.getCardinality());
  }

  @Override
  public int andCardinality(BitmapContainer x) {
    return x.andCardinality(this);
  }

  @Override
  // see andNot for an approach that might be better.
  public int andCardinality(RunContainer x) {
    return x.andCardinality(this);
  }

  @Override
  public ArrayContainer andNot(final ArrayContainer value2) {
    ArrayContainer value1 = this;
    final int desiredCapacity = value1.getCardinality();
    ArrayContainer answer = new ArrayContainer(desiredCapacity);
    answer.cardinality = Util.unsignedDifference(value1.content, value1.getCardinality(),
        value2.content, value2.getCardinality(), answer.content);
    return answer;
  }

  @Override
  public ArrayContainer andNot(BitmapContainer value2) {
    final ArrayContainer answer = new ArrayContainer(content.length);
    int pos = 0;
    for (int k = 0; k < cardinality; ++k) {
      char val = this.content[k];
      answer.content[pos] = val;
      pos += 1 - value2.bitValue(val);
    }
    answer.cardinality = pos;
    return answer;
  }

  @Override
  public ArrayContainer andNot(RunContainer x) {
    if (x.numberOfRuns() == 0) {
      return clone();
    } else if (x.isFull()) {
      return ArrayContainer.empty();
    }
    int write = 0;
    int read = 0;
    ArrayContainer answer = new ArrayContainer(cardinality);
    for (int i = 0; i < x.numberOfRuns() && read < cardinality; ++i) {
      int runStart = (x.getValue(i));
      int runEnd = runStart + (x.getLength(i));
      if ((content[read]) > runEnd) {
        continue;
      }
      int firstInRun = Util.iterateUntil(content, read, cardinality, runStart);
      int toWrite = firstInRun - read;
      System.arraycopy(content, read, answer.content, write, toWrite);
      write += toWrite;

      read = Util.iterateUntil(content, firstInRun, cardinality, runEnd + 1);
    }
    System.arraycopy(content, read, answer.content, write, cardinality - read);
    write += cardinality - read;
    answer.cardinality = write;
    return answer;
  }

  @Override
  public void clear() {
    cardinality = 0;
  }

  @Override
  public ArrayContainer clone() {
    return new ArrayContainer(this.cardinality, this.content);
  }

  @Override
  public boolean isEmpty() {
    return cardinality == 0;
  }

  @Override
  public boolean isFull() {
    return false;
  }

  @Override
  public boolean contains(final char x) {
    return Util.unsignedBinarySearch(content, 0, cardinality, x) >= 0;
  }

  @Override
  public boolean contains(int minimum, int supremum) {
    int maximum = supremum - 1;
    int start = Util.advanceUntil(content, -1, cardinality, (char)minimum);
    int end = Util.advanceUntil(content, start - 1, cardinality, (char)maximum);
    return start < cardinality
            && end < cardinality
            && end - start == maximum - minimum
            && content[start] == (char)minimum
            && content[end] == (char)maximum;
  }


  @Override
  protected boolean contains(RunContainer runContainer) {
    if (runContainer.getCardinality() > cardinality) {
      return false;
    }

    for (int i = 0; i < runContainer.numberOfRuns(); ++i) {
      int start = (runContainer.getValue(i));
      int length = (runContainer.getLength(i));
      if (!contains(start, start + length)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean contains(ArrayContainer arrayContainer) {
    if (cardinality < arrayContainer.cardinality) {
      return false;
    }
    int i1 = 0, i2 = 0;
    while(i1 < cardinality && i2 < arrayContainer.cardinality) {
      if(content[i1] == arrayContainer.content[i2]) {
        ++i1;
        ++i2;
      } else if(content[i1] < arrayContainer.content[i2]) {
        ++i1;
      } else {
        return false;
      }
    }
    return i2 == arrayContainer.cardinality;
  }

  @Override
  protected boolean contains(BitmapContainer bitmapContainer) {
    return false;
  }

  @Override
  public void deserialize(DataInput in) throws IOException {
    this.cardinality = 0xFFFF & Character.reverseBytes(in.readChar());
    if (this.content.length < this.cardinality) {
      this.content = new char[this.cardinality];
    }
    for (int k = 0; k < this.cardinality; ++k) {
      this.content[k] = Character.reverseBytes(in.readChar());
    }
  }

  // in order
  private void emit(char val) {
    if (cardinality == content.length) {
      increaseCapacity(true);
    }
    content[cardinality++] = val;
  }


  @Override
  public boolean equals(Object o) {
    if (o instanceof ArrayContainer) {
      ArrayContainer srb = (ArrayContainer) o;
      return ArraysShim.equals(this.content, 0, cardinality, srb.content, 0, srb.cardinality);
    } else if (o instanceof RunContainer) {
      return o.equals(this);
    }
    return false;
  }

  @Override
  public void fillLeastSignificant16bits(int[] x, int i, int mask) {
    for (int k = 0; k < this.cardinality; ++k) {
      x[k + i] = (this.content[k]) | mask;
    }

  }

  @Override
  public Container flip(char x) {
    int loc = Util.unsignedBinarySearch(content, 0, cardinality, x);
    if (loc < 0) {
      // Transform the ArrayContainer to a BitmapContainer
      // when cardinality = DEFAULT_MAX_SIZE
      if (cardinality >= DEFAULT_MAX_SIZE) {
        BitmapContainer a = this.toBitmapContainer();
        a.add(x);
        return a;
      }
      if (cardinality >= this.content.length) {
        increaseCapacity();
      }
      // insertion : shift the elements > x by one position to
      // the right
      // and put x in it's appropriate place
      System.arraycopy(content, -loc - 1, content, -loc, cardinality + loc + 1);
      content[-loc - 1] = x;
      ++cardinality;
    } else {
      System.arraycopy(content, loc + 1, content, loc, cardinality - loc - 1);
      --cardinality;
    }
    return this;
  }

  @Override
  public int getArraySizeInBytes() {
    return cardinality * 2;
  }

  @Override
  public int getCardinality() {
    return cardinality;
  }

  @Override
  public PeekableCharIterator getReverseCharIterator() {
    return new ReverseArrayContainerCharIterator(this);
  }

  @Override
  public PeekableCharIterator getCharIterator() {
    return new ArrayContainerCharIterator(this);
  }

  @Override
  public PeekableCharRankIterator getCharRankIterator() {
    // for ArrayContainer there is no additional work, pos is known in advance
    return new ArrayContainerCharIterator(this);
  }

  @Override
  public ContainerBatchIterator getBatchIterator() {
    return new ArrayBatchIterator(this);
  }


  @Override
  public int getSizeInBytes() {
    return this.cardinality * 2 + 4;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (int k = 0; k < cardinality; ++k) {
      hash += 31 * hash + content[k];
    }
    return hash;
  }

  @Override
  public Container iadd(int begin, int end) {
    // TODO: may need to convert to a RunContainer
    if(end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int indexstart = Util.unsignedBinarySearch(content, 0, cardinality, (char) begin);
    if (indexstart < 0) {
      indexstart = -indexstart - 1;
    }
    int indexend = Util.unsignedBinarySearch(content, 0, cardinality, (char) (end - 1));
    if (indexend < 0) {
      indexend = -indexend - 1;
    } else {
      indexend++;
    }
    int rangelength = end - begin;
    int newcardinality = indexstart + (cardinality - indexend) + rangelength;
    if (newcardinality > DEFAULT_MAX_SIZE) {
      BitmapContainer a = this.toBitmapContainer();
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
    if (newcardinality >= this.content.length) {
      char[] destination = new char[calculateCapacity(newcardinality)];
      // if b > 0, we copy from 0 to b. Do nothing otherwise.
      System.arraycopy(content, 0, destination, 0, indexstart);
      // set values from b to e
      for (int k = 0; k < rangelength; ++k) {
        destination[k + indexstart] = (char) (begin + k);
      }
      /*
       * so far cases - 1,2 and 6 are done Now, if e < cardinality, we copy from e to
       * cardinality.Otherwise do noting this covers remaining 3,4 and 5 cases
       */
      System.arraycopy(content, indexend, destination, indexstart + rangelength, cardinality
          - indexend);
      this.content = destination;
    } else {
      System
          .arraycopy(content, indexend, content, indexstart + rangelength, cardinality - indexend);
      for (int k = 0; k < rangelength; ++k) {
        content[k + indexstart] = (char) (begin + k);
      }
    }
    cardinality = newcardinality;
    return this;
  }


  @Override
  public ArrayContainer iand(final ArrayContainer value2) {
    ArrayContainer value1 = this;
    value1.cardinality = Util.unsignedIntersect2by2(value1.content, value1.getCardinality(),
        value2.content, value2.getCardinality(), value1.content);
    return this;
  }

  @Override
  public Container iand(BitmapContainer value2) {
    int pos = 0;
    for (int k = 0; k < cardinality; ++k) {
      char v = this.content[k];
      this.content[pos] = v;
      pos += (int)value2.bitValue(v);
    }
    cardinality = pos;
    return this;
  }

  @Override
  public Container iand(RunContainer x) {
    // possible performance issue, not taking advantage of possible inplace
    return x.and(this);
  }


  @Override
  public ArrayContainer iandNot(final ArrayContainer value2) {
    this.cardinality = Util.unsignedDifference(this.content, this.getCardinality(), value2.content,
        value2.getCardinality(), this.content);
    return this;
  }

  @Override
  public ArrayContainer iandNot(BitmapContainer value2) {
    int pos = 0;
    for (int k = 0; k < cardinality; ++k) {
      char v = this.content[k];
      this.content[pos] = v;
      pos += 1 - (int)value2.bitValue(v);
    }
    this.cardinality = pos;
    return this;
  }

  @Override
  public Container iandNot(RunContainer x) {
    // possible performance issue, not taking advantage of possible inplace
    // could adapt algo above
    return andNot(x);
  }

  private void increaseCapacity() {
    increaseCapacity(false);
  }


  // temporarily allow an illegally large size, as long as the operation creating
  // the illegal container does not return it.
  private void increaseCapacity(boolean allowIllegalSize) {
    int newCapacity = (this.content.length == 0) ? DEFAULT_INIT_SIZE
        : this.content.length < 64 ? this.content.length * 2
            : this.content.length < 1067 ? this.content.length * 3 / 2
                : this.content.length * 5 / 4;
    // never allocate more than we will ever need
    if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE && !allowIllegalSize) {
      newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
    }
    // if we are within 1/16th of the max, go to max
    if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE - ArrayContainer.DEFAULT_MAX_SIZE / 16
        && !allowIllegalSize) {
      newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
    }
    this.content = Arrays.copyOf(this.content, newCapacity);
  }

  private int calculateCapacity(int min) {
    int newCapacity =
        (this.content.length == 0) ? DEFAULT_INIT_SIZE
            : this.content.length < 64 ? this.content.length * 2
                : this.content.length < 1024 ? this.content.length * 3 / 2
                    : this.content.length * 5 / 4;
    if (newCapacity < min) {
      newCapacity = min;
    }
    // never allocate more than we will ever need
    if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE) {
      newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
    }
    // if we are within 1/16th of the max, go to max
    if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE - ArrayContainer.DEFAULT_MAX_SIZE / 16) {
      newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
    }
    return newCapacity;
  }

  @Override
  public Container inot(final int firstOfRange, final int lastOfRange) {
    // TODO: may need to convert to a RunContainer
    // determine the span of array indices to be affected
    int startIndex = Util.unsignedBinarySearch(content, 0, cardinality, (char) firstOfRange);
    if (startIndex < 0) {
      startIndex = -startIndex - 1;
    }
    int lastIndex = Util.unsignedBinarySearch(content, 0, cardinality, (char) (lastOfRange - 1));
    if (lastIndex < 0) {
      lastIndex = -lastIndex - 1 - 1;
    }
    final int currentValuesInRange = lastIndex - startIndex + 1;
    final int spanToBeFlipped = lastOfRange - firstOfRange;
    final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
    final char[] buffer = new char[newValuesInRange];
    final int cardinalityChange = newValuesInRange - currentValuesInRange;
    final int newCardinality = cardinality + cardinalityChange;

    if (cardinalityChange > 0) { // expansion, right shifting needed
      if (newCardinality > content.length) {
        // so big we need a bitmap?
        if (newCardinality > DEFAULT_MAX_SIZE) {
          return toBitmapContainer().inot(firstOfRange, lastOfRange);
        }
        content = Arrays.copyOf(content, newCardinality);
      }
      // slide right the contents after the range
      System.arraycopy(content, lastIndex + 1, content, lastIndex + 1 + cardinalityChange,
          cardinality - 1 - lastIndex);
      negateRange(buffer, startIndex, lastIndex, firstOfRange, lastOfRange);
    } else { // no expansion needed
      negateRange(buffer, startIndex, lastIndex, firstOfRange, lastOfRange);
      if (cardinalityChange < 0) {
        // contraction, left sliding.
        // Leave array oversize
        System.arraycopy(content, startIndex + newValuesInRange - cardinalityChange, content,
            startIndex + newValuesInRange, newCardinality - (startIndex + newValuesInRange));
      }
    }
    cardinality = newCardinality;
    return this;
  }

  @Override
  public boolean intersects(ArrayContainer value2) {
    ArrayContainer value1 = this;
    return Util.unsignedIntersects(value1.content, value1.getCardinality(), value2.content,
        value2.getCardinality());
  }


  @Override
  public boolean intersects(BitmapContainer x) {
    return x.intersects(this);
  }

  @Override
  public boolean intersects(RunContainer x) {
    return x.intersects(this);
  }

  @Override
  public boolean intersects(int minimum, int supremum) {
    if((minimum < 0) || (supremum < minimum) || (supremum > (1<<16))) {
      throw new RuntimeException("This should never happen (bug).");
    }
    int pos = Util.unsignedBinarySearch(content, 0, cardinality, (char)minimum);
    int index = pos >= 0 ? pos : -pos - 1;
    return index < cardinality && (content[index]) < supremum;
  }


  @Override
  public Container ior(final ArrayContainer value2) {
    int totalCardinality = this.getCardinality() + value2.getCardinality();
    if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
      BitmapContainer bc = new BitmapContainer();
      for (int k = 0; k < value2.cardinality; ++k) {
        char v = value2.content[k];
        final int i = (v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      for (int k = 0; k < this.cardinality; ++k) {
        char v = this.content[k];
        final int i = (v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      bc.cardinality = 0;
      for (long k : bc.bitmap) {
        bc.cardinality += Long.bitCount(k);
      }
      if (bc.cardinality <= DEFAULT_MAX_SIZE) {
        return bc.toArrayContainer();
      } else if (bc.isFull()) {
        return RunContainer.full();
      }
      return bc;
    }
    if (totalCardinality >= content.length) {
      int newCapacity = calculateCapacity(totalCardinality);
      char[] destination = new char[newCapacity];
      cardinality =
          Util.unsignedUnion2by2(content, 0, cardinality, value2.content, 0, value2.cardinality,
              destination);
      this.content = destination;
    } else {
      System.arraycopy(content, 0, content, value2.cardinality, cardinality);
      cardinality =
          Util.unsignedUnion2by2(content, value2.cardinality, cardinality, value2.content, 0,
              value2.cardinality, content);
    }
    return this;
  }

  @Override
  public Container ior(BitmapContainer x) {
    return x.or(this);
  }

  @Override
  public Container ior(RunContainer x) {
    // possible performance issue, not taking advantage of possible inplace
    return x.or(this);
  }

  @Override
  public Container iremove(int begin, int end) {
    if(end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int indexstart = Util.unsignedBinarySearch(content, 0, cardinality, (char) begin);
    if (indexstart < 0) {
      indexstart = -indexstart - 1;
    }
    int indexend = Util.unsignedBinarySearch(content, 0, cardinality, (char) (end - 1));
    if (indexend < 0) {
      indexend = -indexend - 1;
    } else {
      indexend++;
    }
    int rangelength = indexend - indexstart;
    System.arraycopy(content, indexstart + rangelength, content, indexstart,
        cardinality - indexstart - rangelength);
    cardinality -= rangelength;
    return this;
  }

  @Override
  public Iterator<Character> iterator() {
    return new Iterator<Character>() {
      short pos = 0;

      @Override
      public boolean hasNext() {
        return pos < ArrayContainer.this.cardinality;
      }

      @Override
      public Character next() {
        return ArrayContainer.this.content[pos++];
      }

      @Override
      public void remove() {
        ArrayContainer.this.removeAtIndex(pos - 1);
        pos--;
      }
    };
  }

  @Override
  public Container ixor(final ArrayContainer value2) {
    return this.xor(value2);
  }

  @Override
  public Container ixor(BitmapContainer x) {
    return x.xor(this);
  }


  @Override
  public Container ixor(RunContainer x) {
    // possible performance issue, not taking advantage of possible inplace
    return x.xor(this);
  }


  @Override
  public Container limit(int maxcardinality) {
    if (maxcardinality < this.getCardinality()) {
      return new ArrayContainer(maxcardinality, this.content);
    } else {
      return clone();
    }
  }

  void loadData(final BitmapContainer bitmapContainer) {
    this.cardinality = bitmapContainer.cardinality;
    bitmapContainer.fillArray(content);
  }

  // for use in inot range known to be nonempty
  private void negateRange(final char[] buffer, final int startIndex, final int lastIndex,
      final int startRange, final int lastRange) {
    // compute the negation into buffer

    int outPos = 0;
    int inPos = startIndex; // value here always >= valInRange,
    // until it is exhausted
    // n.b., we can start initially exhausted.

    int valInRange = startRange;
    for (; valInRange < lastRange && inPos <= lastIndex; ++valInRange) {
      if ((char) valInRange != content[inPos]) {
        buffer[outPos++] = (char) valInRange;
      } else {
        ++inPos;
      }
    }

    // if there are extra items (greater than the biggest
    // pre-existing one in range), buffer them
    for (; valInRange < lastRange; ++valInRange) {
      buffer[outPos++] = (char) valInRange;
    }

    if (outPos != buffer.length) {
      throw new RuntimeException(
          "negateRange: outPos " + outPos + " whereas buffer.length=" + buffer.length);
    }
    // copy back from buffer...caller must ensure there is room
    int i = startIndex;
    for (char item : buffer) {
      content[i++] = item;
    }
  }

  // shares lots of code with inot; candidate for refactoring
  @Override
  public Container not(final int firstOfRange, final int lastOfRange) {
    // TODO: may need to convert to a RunContainer
    if (firstOfRange >= lastOfRange) {
      return clone(); // empty range
    }

    // determine the span of array indices to be affected
    int startIndex = Util.unsignedBinarySearch(content, 0, cardinality, (char) firstOfRange);
    if (startIndex < 0) {
      startIndex = -startIndex - 1;
    }
    int lastIndex = Util.unsignedBinarySearch(content, 0, cardinality, (char) (lastOfRange - 1));
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

    ArrayContainer answer = new ArrayContainer(newCardinality);

    // copy stuff before the active area
    System.arraycopy(content, 0, answer.content, 0, startIndex);

    int outPos = startIndex;
    int inPos = startIndex; // item at inPos always >= valInRange

    int valInRange = firstOfRange;
    for (; valInRange < lastOfRange && inPos <= lastIndex; ++valInRange) {
      if ((char) valInRange != content[inPos]) {
        answer.content[outPos++] = (char) valInRange;
      } else {
        ++inPos;
      }
    }

    for (; valInRange < lastOfRange; ++valInRange) {
      answer.content[outPos++] = (char) valInRange;
    }

    // content after the active range
    for (int i = lastIndex + 1; i < cardinality; ++i) {
      answer.content[outPos++] = content[i];
    }
    answer.cardinality = newCardinality;
    return answer;
  }

  @Override
  int numberOfRuns() {
    if (cardinality == 0) {
      return 0; // should never happen
    }
    int numRuns = 1;
    int oldv = (content[0]);
    for (int i = 1; i < cardinality; i++) {
      int newv = (content[i]);
      if (oldv + 1 != newv) {
        ++numRuns;
      }
      oldv = newv;
    }
    return numRuns;
  }

  @Override
  public Container or(final ArrayContainer value2) {
    final ArrayContainer value1 = this;
    int totalCardinality = value1.getCardinality() + value2.getCardinality();
    if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
      BitmapContainer bc = new BitmapContainer();
      for (int k = 0; k < value2.cardinality; ++k) {
        char v = value2.content[k];
        final int i = (v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      for (int k = 0; k < this.cardinality; ++k) {
        char v = this.content[k];
        final int i = (v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      bc.cardinality = 0;
      for (long k : bc.bitmap) {
        bc.cardinality += Long.bitCount(k);
      }
      if (bc.cardinality <= DEFAULT_MAX_SIZE) {
        return bc.toArrayContainer();
      } else if (bc.isFull()) {
        return RunContainer.full();
      }
      return bc;
    }
    ArrayContainer answer = new ArrayContainer(totalCardinality);
    answer.cardinality =
            Util.unsignedUnion2by2(
                    value1.content, 0, value1.getCardinality(),
                    value2.content, 0, value2.getCardinality(),
                    answer.content
            );
    return answer;
  }

  @Override
  public Container or(BitmapContainer x) {
    return x.or(this);
  }

  @Override
  public Container or(RunContainer x) {
    return x.or(this);
  }

  protected Container or(CharIterator it) {
    return or(it, false);
  }

  /**
   * it must return items in (unsigned) sorted order. Possible candidate for Container interface?
   **/
  private Container or(CharIterator it, final boolean exclusive) {
    ArrayContainer ac = new ArrayContainer();
    int myItPos = 0;
    ac.cardinality = 0;
    // do a merge. int -1 denotes end of input.
    int myHead = (myItPos == cardinality) ? -1 : (content[myItPos++]);
    int hisHead = advance(it);

    while (myHead != -1 && hisHead != -1) {
      if (myHead < hisHead) {
        ac.emit((char) myHead);
        myHead = (myItPos == cardinality) ? -1 : (content[myItPos++]);
      } else if (myHead > hisHead) {
        ac.emit((char) hisHead);
        hisHead = advance(it);
      } else {
        if (!exclusive) {
          ac.emit((char) hisHead);
        }
        hisHead = advance(it);
        myHead = (myItPos == cardinality) ? -1 : (content[myItPos++]);
      }
    }

    while (myHead != -1) {
      ac.emit((char) myHead);
      myHead = (myItPos == cardinality) ? -1 : (content[myItPos++]);
    }

    while (hisHead != -1) {
      ac.emit((char) hisHead);
      hisHead = advance(it);
    }

    if (ac.cardinality > DEFAULT_MAX_SIZE) {
      return ac.toBitmapContainer();
    } else {
      return ac;
    }
  }

  @Override
  public int rank(char lowbits) {
    int answer = Util.unsignedBinarySearch(content, 0, cardinality, lowbits);
    if (answer >= 0) {
      return answer + 1;
    } else {
      return -answer - 1;
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    deserialize(in);
  }

  @Override
  public Container remove(int begin, int end) {
    if(end == begin) {
      return clone();
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int indexstart = Util.unsignedBinarySearch(content, 0, cardinality, (char) begin);
    if (indexstart < 0) {
      indexstart = -indexstart - 1;
    }
    int indexend = Util.unsignedBinarySearch(content, 0, cardinality, (char) (end - 1));
    if (indexend < 0) {
      indexend = -indexend - 1;
    } else {
      indexend++;
    }
    int rangelength = indexend - indexstart;
    ArrayContainer answer = clone();
    System.arraycopy(content, indexstart + rangelength, answer.content, indexstart,
        cardinality - indexstart - rangelength);
    answer.cardinality = cardinality - rangelength;
    return answer;
  }

  void removeAtIndex(final int loc) {
    System.arraycopy(content, loc + 1, content, loc, cardinality - loc - 1);
    --cardinality;
  }


  @Override
  public Container remove(final char x) {
    final int loc = Util.unsignedBinarySearch(content, 0, cardinality, x);
    if (loc >= 0) {
      removeAtIndex(loc);
    }
    return this;
  }

  @Override
  public Container repairAfterLazy() {
    return this;
  }

  @Override
  public Container runOptimize() {
    // TODO: consider borrowing the BitmapContainer idea of early
    // abandonment
    // with ArrayContainers, when the number of runs in the arrayContainer
    // passes some threshold based on the cardinality.
    int numRuns = numberOfRuns();
    int sizeAsRunContainer = RunContainer.serializedSizeInBytes(numRuns);
    if (getArraySizeInBytes() > sizeAsRunContainer) {
      return new RunContainer(this, numRuns); // this could be maybe
                                              // faster if initial
                                              // container is a bitmap
    } else {
      return this;
    }
  }

  @Override
  public char select(int j) {
    return this.content[j];
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeShort(Character.reverseBytes((char) this.cardinality));
    // little endian
    for (int k = 0; k < this.cardinality; ++k) {
      out.writeShort(Character.reverseBytes(this.content[k]));
    }
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
  public BitmapContainer toBitmapContainer() {
    BitmapContainer bc = new BitmapContainer();
    bc.loadData(this);
    return bc;
  }

  @Override
  public int nextValue(char fromValue) {
    int index = Util.advanceUntil(content, -1, cardinality, fromValue);
    if (index == cardinality) {
      return fromValue == content[cardinality - 1] ? (fromValue) : -1;
    }
    return (content[index]);
  }

  @Override
  public int previousValue(char fromValue) {
    int index = Util.advanceUntil(content, -1, cardinality, fromValue);
    if (index != cardinality && content[index] == fromValue) {
      return (content[index]);
    }
    return index == 0 ? -1 : (content[index - 1]);
  }

  @Override
  public int nextAbsentValue(char fromValue) {
    int index = Util.advanceUntil(content, -1, cardinality, fromValue);
    if (index >= cardinality) {
      return (int) (fromValue);
    }
    if (index == cardinality - 1) {
      return fromValue == content[cardinality - 1] ? (int) (fromValue) + 1 : (int) (fromValue);
    }
    if (content[index] != fromValue) {
      return (int) (fromValue);
    }
    if (content[index + 1] > fromValue + 1) {
      return (int) (fromValue) + 1;
    }

    int low = index;
    int high = cardinality;

    while (low + 1 < high) {
      int mid = (high + low) >>> 1;
      if (mid - index < (content[mid]) - (int) (fromValue)) {
        high = mid;
      } else {
        low = mid;
      }
    }

    if (low == cardinality - 1) {
      return (content[cardinality - 1]) + 1;
    }

    assert (content[low]) + 1 < (content[high]);
    assert (content[low]) == (int) (fromValue) + (low - index);
    return (content[low]) + 1;
  }

  @Override
  public int previousAbsentValue(char fromValue) {
    int index = Util.advanceUntil(content, -1, cardinality, fromValue);
    if (index >= cardinality) {
      return (int) (fromValue);
    }
    if (index == 0) {
      return fromValue == content[0] ? (int) (fromValue) - 1 : (int) (fromValue);
    }
    if (content[index] != fromValue) {
      return (int) (fromValue);
    }
    if (content[index - 1] < fromValue - 1) {
      return (int) (fromValue) - 1;
    }

    int low = -1;
    int high = index;

    // Binary search for the first index which differs by at least 2 from its
    // successor
    while (low + 1 < high) {
      int mid = (high + low) >>> 1;
      if (index - mid < (int) (fromValue) - (content[mid])) {
        low = mid;
      } else {
        high = mid;
      }
    }

    if (high == 0) {
      return (content[0]) - 1;
    }

    assert (content[low]) + 1 < (content[high]);
    assert (content[high]) == (int) (fromValue) - (index - high);
    return (content[high]) - 1;
  }

  @Override
  public int first() {
    assertNonEmpty(cardinality == 0);
    return (content[0]);
  }

  @Override
  public int last() {
    assertNonEmpty(cardinality == 0);
    return (content[cardinality - 1]);
  }

  @Override
  public MappeableContainer toMappeableContainer() {
    return new MappeableArrayContainer(this);
  }

  /**
   * Return the content of this container as a ShortBuffer. This creates a copy and might be
   * relatively slow.
   *
   * @return the ShortBuffer
   */
  public CharBuffer toCharBuffer() {
    CharBuffer cb = CharBuffer.allocate(this.cardinality);
    cb.put(this.content, 0, this.cardinality);
    return cb;
  }

  @Override
  public String toString() {
    if (this.cardinality == 0) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (int i = 0; i < this.cardinality - 1; i++) {
      sb.append((int)(this.content[i]));
      sb.append(",");
    }
    sb.append((int)(this.content[this.cardinality - 1]));
    sb.append("}");
    return sb.toString();
  }

  @Override
  public void trim() {
    if (this.content.length == this.cardinality) {
      return;
    }
    this.content = Arrays.copyOf(this.content, this.cardinality);
  }

  @Override
  public void writeArray(DataOutput out) throws IOException {
    // little endian
    for (int k = 0; k < this.cardinality; ++k) {
      char v = this.content[k];
      out.writeChar(Character.reverseBytes(v));
    }
  }

  @Override
  public void writeArray(ByteBuffer buffer) {
    assert buffer.order() == ByteOrder.LITTLE_ENDIAN;
    CharBuffer buf = buffer.asCharBuffer();
    buf.put(content, 0, cardinality);
    int bytesWritten = 2 * cardinality;
    buffer.position(buffer.position() + bytesWritten);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    serialize(out);
  }

  @Override
  public Container xor(final ArrayContainer value2) {
    final ArrayContainer value1 = this;
    final int totalCardinality = value1.getCardinality() + value2.getCardinality();
    if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
      BitmapContainer bc = new BitmapContainer();
      for (int k = 0; k < value2.cardinality; ++k) {
        char v = value2.content[k];
        final int i = (v) >>> 6;
        bc.bitmap[i] ^= (1L << v);
      }
      for (int k = 0; k < this.cardinality; ++k) {
        char v = this.content[k];
        final int i = (v) >>> 6;
        bc.bitmap[i] ^= (1L << v);
      }
      bc.cardinality = 0;
      for (long k : bc.bitmap) {
        bc.cardinality += Long.bitCount(k);
      }
      if (bc.cardinality <= DEFAULT_MAX_SIZE) {
        return bc.toArrayContainer();
      }
      return bc;
    }
    ArrayContainer answer = new ArrayContainer(totalCardinality);
    answer.cardinality = Util.unsignedExclusiveUnion2by2(value1.content, value1.getCardinality(),
        value2.content, value2.getCardinality(), answer.content);
    return answer;
  }

  @Override
  public Container xor(BitmapContainer x) {
    return x.xor(this);
  }

  @Override
  public Container xor(RunContainer x) {
    return x.xor(this);
  }


  protected Container xor(CharIterator it) {
    return or(it, true);
  }

  @Override
  public void forEach(char msb, IntConsumer ic) {
    int high = msb << 16;
    for(int k = 0; k < cardinality; ++k) {
      ic.accept(content[k] | high);
    }
  }

  protected Container lazyor(ArrayContainer value2) {
    final ArrayContainer value1 = this;
    int totalCardinality = value1.getCardinality() + value2.getCardinality();
    if (totalCardinality > ARRAY_LAZY_LOWERBOUND) {// it could be a bitmap!
      BitmapContainer bc = new BitmapContainer();
      for (int k = 0; k < value2.cardinality; ++k) {
        char v = value2.content[k];
        final int i = (v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      for (int k = 0; k < this.cardinality; ++k) {
        char v = this.content[k];
        final int i = (v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      bc.cardinality = -1;
      return bc;
    }
    ArrayContainer answer = new ArrayContainer(totalCardinality);
    answer.cardinality =
            Util.unsignedUnion2by2(
                    value1.content, 0, value1.getCardinality(),
                    value2.content, 0, value2.getCardinality(),
                    answer.content
            );
    return answer;

  }

}


final class ArrayContainerCharIterator implements PeekableCharRankIterator {
  int pos;
  private ArrayContainer parent;

  ArrayContainerCharIterator() {

  }

  ArrayContainerCharIterator(ArrayContainer p) {
    wrap(p);
  }

  @Override
  public void advanceIfNeeded(char minval) {
    pos = Util.advanceUntil(parent.content, pos - 1, parent.cardinality, minval);
  }

  @Override
  public int peekNextRank() {
    return pos + 1;
  }

  @Override
  public PeekableCharRankIterator clone() {
    try {
      return (PeekableCharRankIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return pos < parent.cardinality;
  }

  @Override
  public char next() {
    return parent.content[pos++];
  }

  @Override
  public int nextAsInt() {
    return (parent.content[pos++]);
  }

  @Override
  public char peekNext() {
    return parent.content[pos];
  }


  @Override
  public void remove() {
    parent.removeAtIndex(pos - 1);
    pos--;
  }

  void wrap(ArrayContainer p) {
    parent = p;
    pos = 0;
  }

}


final class ReverseArrayContainerCharIterator implements PeekableCharIterator {
  int pos;
  private ArrayContainer parent;

  ReverseArrayContainerCharIterator() {

  }

  ReverseArrayContainerCharIterator(ArrayContainer p) {
    wrap(p);
  }

  @Override
  public void advanceIfNeeded(char maxval) {
    pos = Util.reverseUntil(parent.content, pos + 1, parent.cardinality, maxval);
  }

  @Override
  public PeekableCharIterator clone() {
    try {
      return (PeekableCharIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return pos >= 0;
  }

  @Override
  public char next() {
    return parent.content[pos--];
  }

  @Override
  public int nextAsInt() {
    return (parent.content[pos--]);
  }

  @Override
  public char peekNext() {
    return parent.content[pos];
  }

  @Override
  public void remove() {
    parent.removeAtIndex(pos + 1);
    pos++;
  }

  void wrap(ArrayContainer p) {
    parent = p;
    pos = parent.cardinality - 1;
  }
}
