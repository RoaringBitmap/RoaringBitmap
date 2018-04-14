/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Iterator;

import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Long.numberOfTrailingZeros;

/**
 * Simple bitset-like container. Unlike org.roaringbitmap.BitmapContainer, this class uses a
 * LongBuffer to store data.
 */
public final class MappeableBitmapContainer extends MappeableContainer implements Cloneable {
  protected static final int MAX_CAPACITY = 1 << 16;

  
  private static final long serialVersionUID = 2L;

  // bail out early when the number of runs is excessive, without
  // an exact count (just a decent lower bound)
  private static final int BLOCKSIZE = 128;
  // 64 words can have max 32 runs per word, max 2k runs

  /**
   * optimization flag: whether the cardinality of the bitmaps is maintained through branchless
   * operation
   */
  public static final boolean USE_BRANCHLESS = true;


  // the parameter is for overloading and symmetry with ArrayContainer
  protected static int serializedSizeInBytes(int unusedCardinality) {
    return MAX_CAPACITY / 8;
  }

  LongBuffer bitmap;

  int cardinality;

  // nruns value for which RunContainer.serializedSizeInBytes ==
  // BitmapContainer.getArraySizeInBytes()
  private final int MAXRUNS = (getArraySizeInBytes() - 2) / 4;

  /**
   * Create a bitmap container with all bits set to false
   */
  public MappeableBitmapContainer() {
    this.cardinality = 0;
    this.bitmap = LongBuffer.allocate(MAX_CAPACITY / 64);
  }

  /**
   * Creates a new bitmap container from a non-mappeable one. This copies the data.
   * 
   * @param bc the original container
   */
  public MappeableBitmapContainer(BitmapContainer bc) {
    this.cardinality = bc.getCardinality();
    this.bitmap = bc.toLongBuffer();
  }

  /**
   * Create a bitmap container with a run of ones from firstOfRun to lastOfRun, inclusive caller
   * must ensure that the range isn't so small that an ArrayContainer should have been created
   * instead
   * 
   * @param firstOfRun first index
   * @param lastOfRun last index (range is exclusive)
   */
  public MappeableBitmapContainer(final int firstOfRun, final int lastOfRun) {
    // TODO: this can be optimized for performance
    this.cardinality = lastOfRun - firstOfRun;
    this.bitmap = LongBuffer.allocate(MAX_CAPACITY / 64);
    Util.setBitmapRange(bitmap.array(), firstOfRun, lastOfRun);
  }



  MappeableBitmapContainer(int newCardinality, LongBuffer newBitmap) {
    this.cardinality = newCardinality;
    LongBuffer tmp = newBitmap.duplicate(); // for thread safety
    this.bitmap = LongBuffer.allocate(tmp.limit());
    tmp.rewind();
    this.bitmap.put(tmp);
  }

  /**
   * Construct a new BitmapContainer backed by the provided LongBuffer.
   * 
   * @param array LongBuffer where the data is stored
   * @param initCardinality cardinality (number of values stored)
   */
  public MappeableBitmapContainer(final LongBuffer array, final int initCardinality) {
    if (array.limit() != MAX_CAPACITY / 64) {
      throw new RuntimeException("Mismatch between buffer and storage requirements: "
          + array.limit() + " vs. " + MAX_CAPACITY / 64);
    }
    this.cardinality = initCardinality;
    this.bitmap = array;
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
    MappeableBitmapContainer answer = clone();
    int prevOnesInRange = answer.cardinalityInRange(begin, end);
    BufferUtil.setBitmapRange(answer.bitmap, begin, end);
    answer.updateCardinality(prevOnesInRange, end - begin);
    return answer;
  }


  @Override
  public MappeableContainer add(final short i) {
    final int x = BufferUtil.toIntUnsigned(i);
    final long previous = bitmap.get(x / 64);
    final long newv = previous | (1L << x);
    bitmap.put(x / 64, newv);
    if (USE_BRANCHLESS) {
      cardinality += (previous ^ newv) >>> x;
    } else if (previous != newv) {
      cardinality++;
    }
    return this;
  }

  @Override
  public boolean isEmpty() {
    return cardinality == 0;
  }

  @Override
  public MappeableArrayContainer and(final MappeableArrayContainer value2) {

    final MappeableArrayContainer answer = new MappeableArrayContainer(value2.content.limit());
    if (!BufferUtil.isBackedBySimpleArray(answer.content)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    short[] sarray = answer.content.array();
    if (BufferUtil.isBackedBySimpleArray(value2.content)) {
      short[] c = value2.content.array();
      int ca = value2.cardinality;
      for (int k = 0; k < ca; ++k) {
        short v = c[k];
        sarray[answer.cardinality] = v;
        answer.cardinality += this.bitValue(v);
      }

    } else {
      int ca = value2.cardinality;
      for (int k = 0; k < ca; ++k) {
        short v = value2.content.get(k);
        sarray[answer.cardinality] = v;
        answer.cardinality += this.bitValue(v);
      }
    }
    return answer;
  }

  @Override
  public MappeableContainer and(final MappeableBitmapContainer value2) {
    int newCardinality = 0;
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)
        && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
      long[] tb = this.bitmap.array();
      long[] v2b = value2.bitmap.array();
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(tb[k] & v2b[k]);
      }
    } else {
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(this.bitmap.get(k) & value2.bitmap.get(k));
      }
    }
    if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      final MappeableBitmapContainer answer = new MappeableBitmapContainer();
      if (!BufferUtil.isBackedBySimpleArray(answer.bitmap)) {
        throw new RuntimeException("Should not happen. Internal bug.");
      }
      long[] bitArray = answer.bitmap.array();
      if (BufferUtil.isBackedBySimpleArray(this.bitmap)
          && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
        long[] tb = this.bitmap.array();
        long[] v2b = value2.bitmap.array();
        int len = this.bitmap.limit();
        for (int k = 0; k < len; ++k) {
          bitArray[k] = tb[k] & v2b[k];
        }
      } else {
        int len = this.bitmap.limit();
        for (int k = 0; k < len; ++k) {
          bitArray[k] = this.bitmap.get(k) & value2.bitmap.get(k);
        }
      }
      answer.cardinality = newCardinality;
      return answer;
    }
    final MappeableArrayContainer ac = new MappeableArrayContainer(newCardinality);
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)
        && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
      org.roaringbitmap.Util.fillArrayAND(ac.content.array(), this.bitmap.array(),
          value2.bitmap.array());
    } else {
      BufferUtil.fillArrayAND(ac.content.array(), this.bitmap, value2.bitmap);
    }
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public MappeableContainer and(final MappeableRunContainer value2) {
    return value2.and(this);
  }



  @Override
  public MappeableContainer andNot(final MappeableArrayContainer value2) {
    final MappeableBitmapContainer answer = clone();
    if (!BufferUtil.isBackedBySimpleArray(answer.bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] bitArray = answer.bitmap.array();
    if (BufferUtil.isBackedBySimpleArray(value2.content)
        && BufferUtil.isBackedBySimpleArray(this.bitmap)) {
      short[] v2 = value2.content.array();
      int c = value2.cardinality;
      for (int k = 0; k < c; ++k) {
        short v = v2[k];
        final int i = BufferUtil.toIntUnsigned(v) >>> 6;
        long w = bitArray[i];
        long aft = w & (~(1L << v));
        bitArray[i] = aft;
        answer.cardinality -= (w ^ aft) >>> v;
      }
    } else {
      int c = value2.cardinality;
      for (int k = 0; k < c; ++k) {
        short v2 = value2.content.get(k);
        final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
        long w = bitArray[i];
        long aft = bitArray[i] & (~(1L << v2));
        bitArray[i] = aft;
        answer.cardinality -= (w ^ aft) >>> v2;
      }
    }
    if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return answer.toArrayContainer();
    }
    return answer;
  }



  @Override
  public MappeableContainer andNot(final MappeableBitmapContainer value2) {

    int newCardinality = 0;
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)
        && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
      long[] b = this.bitmap.array();
      long[] v2 = value2.bitmap.array();
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(b[k] & (~v2[k]));
      }

    } else {
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(this.bitmap.get(k) & (~value2.bitmap.get(k)));
      }
    }
    if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      final MappeableBitmapContainer answer = new MappeableBitmapContainer();
      if (!BufferUtil.isBackedBySimpleArray(answer.bitmap)) {
        throw new RuntimeException("Should not happen. Internal bug.");
      }
      long[] bitArray = answer.bitmap.array();
      if (BufferUtil.isBackedBySimpleArray(this.bitmap)
          && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
        long[] b = this.bitmap.array();
        long[] v2 = value2.bitmap.array();
        int len = answer.bitmap.limit();
        for (int k = 0; k < len; ++k) {
          bitArray[k] = b[k] & (~v2[k]);
        }
      } else {
        int len = answer.bitmap.limit();
        for (int k = 0; k < len; ++k) {
          bitArray[k] = this.bitmap.get(k) & (~value2.bitmap.get(k));
        }
      }
      answer.cardinality = newCardinality;
      return answer;
    }
    final MappeableArrayContainer ac = new MappeableArrayContainer(newCardinality);
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)
        && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
      org.roaringbitmap.Util.fillArrayANDNOT(ac.content.array(), this.bitmap.array(),
          value2.bitmap.array());
    } else {
      BufferUtil.fillArrayANDNOT(ac.content.array(), this.bitmap, value2.bitmap);
    }
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public MappeableContainer andNot(final MappeableRunContainer value2) {
    MappeableBitmapContainer answer = this.clone();
    if (BufferUtil.isBackedBySimpleArray(answer.bitmap)) {
      long[] b = answer.bitmap.array();
      for (int rlepos = 0; rlepos < value2.nbrruns; ++rlepos) {
        int start = BufferUtil.toIntUnsigned(value2.getValue(rlepos));
        int end = BufferUtil.toIntUnsigned(value2.getValue(rlepos))
            + BufferUtil.toIntUnsigned(value2.getLength(rlepos)) + 1;
        int prevOnesInRange = Util.cardinalityInBitmapRange(b, start, end);
        Util.resetBitmapRange(b, start, end);
        answer.updateCardinality(prevOnesInRange, 0);
      }
    } else {
      for (int rlepos = 0; rlepos < value2.nbrruns; ++rlepos) {
        int start = BufferUtil.toIntUnsigned(value2.getValue(rlepos));
        int end = BufferUtil.toIntUnsigned(value2.getValue(rlepos))
            + BufferUtil.toIntUnsigned(value2.getLength(rlepos)) + 1;
        int prevOnesInRange = answer.cardinalityInRange(start, end);
        BufferUtil.resetBitmapRange(answer.bitmap, start, end);
        answer.updateCardinality(prevOnesInRange, 0);
      }
    }
    if (answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayContainer();
    }
  }

  @Override
  public void clear() {
    if (cardinality != 0) {
      cardinality = 0;
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        bitmap.put(k, 0);
      }
    }
  }



  @Override
  public MappeableBitmapContainer clone() {
    return new MappeableBitmapContainer(this.cardinality, this.bitmap);
  }

  /**
   * Recomputes the cardinality of the bitmap.
   */
  protected void computeCardinality() {
    this.cardinality = 0;
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] b = this.bitmap.array();
      for (int k = 0; k < b.length; k++) {
        this.cardinality += Long.bitCount(b[k]);
      }
    } else {
      int m = this.bitmap.limit();
      for (int k = 0; k < m; k++) {
        this.cardinality += Long.bitCount(this.bitmap.get(k));
      }
    }
  }

  protected int cardinalityInRange(int start, int end) {
    assert (cardinality != -1);
    if (end - start > MAX_CAPACITY / 2) {
      int before = BufferUtil.cardinalityInBitmapRange(bitmap, 0, start);
      int after = BufferUtil.cardinalityInBitmapRange(bitmap, end, MAX_CAPACITY);
      return cardinality - before - after;
    }
    return BufferUtil.cardinalityInBitmapRange(bitmap, start, end);
  }

  protected void updateCardinality(int prevOnes, int newOnes) {
    int oldCardinality = this.cardinality;
    this.cardinality = oldCardinality - prevOnes + newOnes;
  }

  @Override
  public boolean contains(final short i) {
    final int x = BufferUtil.toIntUnsigned(i);
    return (bitmap.get(x / 64) & (1L << x)) != 0;
  }

  protected long bitValue(final short i) {
    final int x = BufferUtil.toIntUnsigned(i);
    return (bitmap.get(x / 64) >>> x ) & 1;
  }
  
  
  /**
   * Checks whether the container contains the value i.
   * 
   * @param buf underlying buffer
   * @param position position of the container in the buffer
   * @param i index
   * @return whether the container contains the value i
   */
  public static boolean contains(ByteBuffer buf, int position, final short i) {
    final int x = BufferUtil.toIntUnsigned(i);
    return (buf.getLong(x / 64 * 8 + position) & (1L << x)) != 0;
  }

  @Override

  public boolean equals(Object o) {
    if (o instanceof MappeableBitmapContainer) {
      final MappeableBitmapContainer srb = (MappeableBitmapContainer) o;
      if (srb.cardinality != this.cardinality) {
        return false;
      }
      if (BufferUtil.isBackedBySimpleArray(this.bitmap)
          && BufferUtil.isBackedBySimpleArray(srb.bitmap)) {
        long[] b = this.bitmap.array();
        long[] s = srb.bitmap.array();
        int len = this.bitmap.limit();
        for (int k = 0; k < len; ++k) {
          if (b[k] != s[k]) {
            return false;
          }
        }
      } else {
        int len = this.bitmap.limit();
        for (int k = 0; k < len; ++k) {
          if (this.bitmap.get(k) != srb.bitmap.get(k)) {
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

  /**
   * Fill the array with set bits
   * 
   * @param array container (should be sufficiently large)
   */
  protected void fillArray(final short[] array) {
    int pos = 0;
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] b = bitmap.array();
      int base = 0;
      for (int k = 0; k < b.length; ++k) {
        long bitset = b[k];
        while (bitset != 0) {
          array[pos++] = (short) (base + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
        base += 64;
      }

    } else {
      int len = this.bitmap.limit();
      int base = 0;
      for (int k = 0; k < len; ++k) {
        long bitset = bitmap.get(k);
        while (bitset != 0) {
          array[pos++] = (short) (base + numberOfLeadingZeros(bitset));
          bitset &= (bitset - 1);
        }
        base += 64;
      }
    }
  }

  @Override
  public void fillLeastSignificant16bits(int[] x, int i, int mask) {
    int pos = i;
    int base = mask;
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] b = bitmap.array();
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        long bitset = b[k];
        while (bitset != 0) {
          x[pos++] = base + numberOfTrailingZeros(bitset);
          bitset &= (bitset - 1);
        }
        base += 64;
      }

    } else {
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        long bitset = bitmap.get(k);
        while (bitset != 0) {
          x[pos++] = base + numberOfTrailingZeros(bitset);
          bitset &= (bitset - 1);
        }
        base += 64;
      }
    }
  }

  @Override
  public MappeableContainer flip(short i) {
    final int x = BufferUtil.toIntUnsigned(i);
    final long bef = bitmap.get(x / 64);
    final long mask = 1L << x;
    if (cardinality == MappeableArrayContainer.DEFAULT_MAX_SIZE + 1) {// this
                                                                      // is
      // the
      // uncommon
      // path
      if ((bef & mask) != 0) {
        --cardinality;
        bitmap.put(x / 64, bef & ~mask);
        return this.toArrayContainer();
      }
    }
    long aft = bef ^ mask;
    // TODO: check whether a branchy version could be faster
    cardinality += 1 - 2 * ((bef & mask) >>> x);
    bitmap.put(x / 64, aft);
    return this;
  }


  @Override
  protected int getArraySizeInBytes() {
    return MAX_CAPACITY / 8;
  }



  @Override
  public int getCardinality() {
    return cardinality;
  }

  @Override
  public ShortIterator getReverseShortIterator() {
    if (this.isArrayBacked()) {
      return BitmapContainer.getReverseShortIterator(bitmap.array());
    }
    return new ReverseMappeableBitmapContainerShortIterator(this);
  }

  @Override
  public PeekableShortIterator getShortIterator() {
    if (this.isArrayBacked()) {
      return BitmapContainer.getShortIterator(bitmap.array());
    }
    return new MappeableBitmapContainerShortIterator(this);
  }

  @Override
  public ContainerBatchIterator getBatchIterator() {
    return new BitmapBatchIterator(this);
  }

  @Override
  public int getSizeInBytes() {
    return this.bitmap.limit() * 8;
  }

  @Override
  public int hashCode() {
    long hash = 0;
    int len = this.bitmap.limit();
    for (int k = 0; k < len; ++k) {
      hash += 31 * hash + this.bitmap.get(k);
    }
    return (int) hash;
  }

  @Override
  public MappeableContainer iadd(int begin, int end) {
    // TODO: may need to convert to a RunContainer
    if (end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int prevOnesInRange = cardinalityInRange(begin, end);
    BufferUtil.setBitmapRange(bitmap, begin, end);
    updateCardinality(prevOnesInRange, end - begin);
    return this;
  }

  @Override
  public MappeableContainer iand(final MappeableArrayContainer b2) {
    return b2.and(this);// no inplace possible
  }


  @Override
  public MappeableContainer iand(final MappeableBitmapContainer b2) {
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)
        && BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
      int newCardinality = 0;
      long[] tb = this.bitmap.array();
      long[] tb2 = b2.bitmap.array();
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(tb[k] & tb2[k]);
      }
      if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
        for (int k = 0; k < len; ++k) {
          tb[k] &= tb2[k];
        }
        this.cardinality = newCardinality;
        return this;
      }
      final MappeableArrayContainer ac = new MappeableArrayContainer(newCardinality);
      BufferUtil.fillArrayAND(ac.content.array(), this.bitmap, b2.bitmap);
      ac.cardinality = newCardinality;
      return ac;
    } else {
      int newCardinality = 0;
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(this.bitmap.get(k) & b2.bitmap.get(k));
      }
      if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
        for (int k = 0; k < len; ++k) {
          this.bitmap.put(k, this.bitmap.get(k) & b2.bitmap.get(k));
        }
        this.cardinality = newCardinality;
        return this;
      }
      final MappeableArrayContainer ac = new MappeableArrayContainer(newCardinality);
      BufferUtil.fillArrayAND(ac.content.array(), this.bitmap, b2.bitmap);
      ac.cardinality = newCardinality;
      return ac;
    }
  }

  @Override
  public MappeableContainer iand(final MappeableRunContainer x) {
    final int card = x.getCardinality();
    if (card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      // no point in doing it in-place
      MappeableArrayContainer answer = new MappeableArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
        int runStart = BufferUtil.toIntUnsigned(x.getValue(rlepos));
        int runEnd = runStart + BufferUtil.toIntUnsigned(x.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          answer.content.put(answer.cardinality, (short) runValue);
          answer.cardinality += this.bitValue((short) runValue);
        }
      }
      return answer;
    }
    int start = 0;
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int end = BufferUtil.toIntUnsigned(x.getValue(rlepos));
      int prevOnes = cardinalityInRange(start, end);
      BufferUtil.resetBitmapRange(this.bitmap, start, end);
      updateCardinality(prevOnes, 0);
      start = end + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
    }
    int ones = cardinalityInRange(start, MAX_CAPACITY);
    BufferUtil.resetBitmapRange(this.bitmap, start, MAX_CAPACITY);
    updateCardinality(ones, 0);
    if (getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return this;
    } else {
      return toArrayContainer();
    }
  }

  @Override
  public MappeableContainer iandNot(final MappeableArrayContainer b2) {
    for (int k = 0; k < b2.cardinality; ++k) {
      this.remove(b2.content.get(k));
    }
    if (cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return this.toArrayContainer();
    }
    return this;
  }



  @Override
  public MappeableContainer iandNot(final MappeableBitmapContainer b2) {
    int newCardinality = 0;
    if (!BufferUtil.isBackedBySimpleArray(bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] b = this.bitmap.array();
    if (BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
      long[] b2Arr = b2.bitmap.array();
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(b[k] & (~b2Arr[k]));
      }
      if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
        for (int k = 0; k < len; ++k) {
          this.bitmap.put(k, b[k] & (~b2Arr[k]));
        }
        this.cardinality = newCardinality;
        return this;
      }
      final MappeableArrayContainer ac = new MappeableArrayContainer(newCardinality);
      org.roaringbitmap.Util.fillArrayANDNOT(ac.content.array(), b, b2Arr);
      ac.cardinality = newCardinality;
      return ac;

    }
    int len = this.bitmap.limit();
    for (int k = 0; k < len; ++k) {
      newCardinality += Long.bitCount(b[k] & (~b2.bitmap.get(k)));
    }
    if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      for (int k = 0; k < len; ++k) {
        b[k] &= (~b2.bitmap.get(k));
      }
      this.cardinality = newCardinality;
      return this;
    }
    final MappeableArrayContainer ac = new MappeableArrayContainer(newCardinality);

    BufferUtil.fillArrayANDNOT(ac.content.array(), this.bitmap, b2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public MappeableContainer iandNot(final MappeableRunContainer x) {
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
      long[] b = this.bitmap.array();
      for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
        int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
        int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
        int prevOnesInRange = Util.cardinalityInBitmapRange(b, start, end);
        Util.resetBitmapRange(b, start, end);
        updateCardinality(prevOnesInRange, 0);
      }
      if (getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
        return this;
      } else {
        return toArrayContainer();
      }
    }
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
      int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
      int prevOnesInRange = cardinalityInRange(start, end);
      BufferUtil.resetBitmapRange(this.bitmap, start, end);
      updateCardinality(prevOnesInRange, 0);
    }
    if (getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return this;
    } else {
      return toArrayContainer();
    }

  }

  protected MappeableContainer ilazyor(MappeableArrayContainer value2) {
    this.cardinality = -1;// invalid
    if (!BufferUtil.isBackedBySimpleArray(bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] b = this.bitmap.array();
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v2 = value2.content.get(k);
      final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
      b[i] |= (1L << v2);
    }
    return this;
  }

  protected MappeableContainer ilazyor(MappeableBitmapContainer x) {
    if (BufferUtil.isBackedBySimpleArray(x.bitmap)) {
      long[] b = this.bitmap.array();
      long[] b2 = x.bitmap.array();
      for (int k = 0; k < b.length; k++) {
        b[k] |= b2[k];
      }
    } else {
      final int m = this.bitmap.limit();
      for (int k = 0; k < m; k++) {
        this.bitmap.put(k, this.bitmap.get(k) | x.bitmap.get(k));
      }
    }
    this.cardinality = -1;// invalid
    return this;
  }


  protected MappeableContainer ilazyor(MappeableRunContainer x) {
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
      int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
      BufferUtil.setBitmapRange(this.bitmap, start, end);
    }
    this.cardinality = -1;
    return this;
  }

  @Override
  public MappeableContainer inot(final int firstOfRange, final int lastOfRange) {
    int prevOnes = cardinalityInRange(firstOfRange, lastOfRange);
    BufferUtil.flipBitmapRange(bitmap, firstOfRange, lastOfRange);
    updateCardinality(prevOnes, lastOfRange - firstOfRange - prevOnes);
    if (cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return toArrayContainer();
    }
    return this;
  }


  @Override
  public boolean intersects(MappeableArrayContainer value2) {
    if (BufferUtil.isBackedBySimpleArray(value2.content)) {
      short[] c = value2.content.array();
      int ca = value2.cardinality;
      for (int k = 0; k < ca; ++k) {
        if (this.contains(c[k])) {
          return true;
        }
      }

    } else {
      int ca = value2.cardinality;
      for (int k = 0; k < ca; ++k) {
        short v = value2.content.get(k);
        if (this.contains(v)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean intersects(MappeableBitmapContainer value2) {
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)
        && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
      long[] tb = this.bitmap.array();
      long[] v2b = value2.bitmap.array();
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        if ((tb[k] & v2b[k]) != 0) {
          return true;
        }
      }
    } else {
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        if ((this.bitmap.get(k) & value2.bitmap.get(k)) != 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean intersects(MappeableRunContainer x) {
    return x.intersects(this);
  }



  @Override
  public MappeableBitmapContainer ior(final MappeableArrayContainer value2) {
    if (!BufferUtil.isBackedBySimpleArray(this.bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] b = this.bitmap.array();
    if (BufferUtil.isBackedBySimpleArray(value2.content)) {
      short[] v2 = value2.content.array();
      int c = value2.cardinality;
      for (int k = 0; k < c; ++k) {
        final int i = BufferUtil.toIntUnsigned(v2[k]) >>> 6;
        long bef = b[i];
        long aft = bef | (1L << v2[k]);
        b[i] = aft;
        if (USE_BRANCHLESS) {
          cardinality += (bef - aft) >>> 63;
        } else {
          if (aft != bef) {
            cardinality++;
          }
        }
      }
      return this;
    }
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v2 = value2.content.get(k);
      final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
      long bef = b[i];
      long aft = bef | (1L << v2);
      b[i] = aft;
      if (USE_BRANCHLESS) {
        cardinality += (bef - aft) >>> 63;
      } else {
        if (aft != bef) {
          cardinality++;
        }
      }
    }
    return this;
  }

  @Override
  public MappeableContainer ior(final MappeableBitmapContainer b2) {
    if (!BufferUtil.isBackedBySimpleArray(bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] b = this.bitmap.array();
    this.cardinality = 0;
    if (BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
      long[] b2Arr = b2.bitmap.array();
      int len = this.bitmap.limit();
      for (int k = 0; k < len; k++) {
        long w = b[k] | b2Arr[k];
        b[k] = w;
        this.cardinality += Long.bitCount(w);
      }
      if (isFull()) {
        return MappeableRunContainer.full();
      }
      return this;
    }
    int len = this.bitmap.limit();
    for (int k = 0; k < len; k++) {
      long w = b[k] | b2.bitmap.get(k);
      b[k] = w;
      this.cardinality += Long.bitCount(w);
    }
    if (isFull()) {
      return MappeableRunContainer.full();
    }
    return this;
  }

  protected boolean isFull() {
    return this.cardinality == MAX_CAPACITY;
  }

  @Override
  public MappeableContainer ior(final MappeableRunContainer x) {
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
      long[] b = this.bitmap.array();
      for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
        int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
        int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
        int prevOnesInRange = Util.cardinalityInBitmapRange(b, start, end);
        Util.setBitmapRange(b, start, end);
        updateCardinality(prevOnesInRange, end - start);
      }
    } else {
      for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
        int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
        int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
        int prevOnesInRange = cardinalityInRange(start, end);
        BufferUtil.setBitmapRange(this.bitmap, start, end);
        updateCardinality(prevOnesInRange, end - start);
      }
    }
    if (isFull()) {
      return MappeableRunContainer.full();
    }
    return this;
  }

  @Override
  public MappeableContainer iremove(int begin, int end) {
    if (end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int prevOnesInRange = cardinalityInRange(begin, end);
    BufferUtil.resetBitmapRange(bitmap, begin, end);
    updateCardinality(prevOnesInRange, 0);
    if (getCardinality() < MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return toArrayContainer();
    }
    return this;
  }

  @Override
  protected boolean isArrayBacked() {
    return BufferUtil.isBackedBySimpleArray(this.bitmap);
  }

  @Override
  public Iterator<Short> iterator() {
    return new Iterator<Short>() {
      final ShortIterator si = MappeableBitmapContainer.this.getShortIterator();

      @Override
      public boolean hasNext() {
        return si.hasNext();
      }

      @Override
      public Short next() {
        return si.next();
      }

      @Override
      public void remove() {
        // TODO: implement
        throw new RuntimeException("unsupported operation: remove");
      }
    };
  }

  @Override
  public MappeableContainer ixor(final MappeableArrayContainer value2) {
    if (!BufferUtil.isBackedBySimpleArray(bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] b = bitmap.array();
    if (BufferUtil.isBackedBySimpleArray(value2.content)) {
      short[] v2 = value2.content.array();
      int c = value2.cardinality;
      for (int k = 0; k < c; ++k) {
        short vc = v2[k];
        long mask = 1L << v2[k];
        final int index = BufferUtil.toIntUnsigned(vc) >>> 6;
        long ba = b[index];
        // TODO: check whether a branchy version could be faster
        this.cardinality += 1 - 2 * ((ba & mask) >>> vc);
        b[index] = ba ^ mask;
      }

    } else {
      int c = value2.cardinality;
      for (int k = 0; k < c; ++k) {
        short v2 = value2.content.get(k);
        long mask = 1L << v2;
        final int index = BufferUtil.toIntUnsigned(v2) >>> 6;
        long ba = b[index];
        // TODO: check whether a branchy version could be faster
        this.cardinality += 1 - 2 * ((ba & mask) >>> v2);
        b[index] = ba ^ mask;
      }
    }
    if (this.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return this.toArrayContainer();
    }
    return this;
  }

  @Override
  public MappeableContainer ixor(MappeableBitmapContainer b2) {
    if (!BufferUtil.isBackedBySimpleArray(bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] b = bitmap.array();
    if (BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
      long[] b2Arr = b2.bitmap.array();

      int newCardinality = 0;
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(b[k] ^ b2Arr[k]);
      }
      if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
        for (int k = 0; k < len; ++k) {
          b[k] ^= b2Arr[k];
        }
        this.cardinality = newCardinality;
        return this;
      }
      final MappeableArrayContainer ac = new MappeableArrayContainer(newCardinality);

      org.roaringbitmap.Util.fillArrayXOR(ac.content.array(), b, b2Arr);
      ac.cardinality = newCardinality;
      return ac;

    }
    int newCardinality = 0;
    int len = this.bitmap.limit();
    for (int k = 0; k < len; ++k) {
      newCardinality += Long.bitCount(b[k] ^ b2.bitmap.get(k));
    }
    if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      for (int k = 0; k < len; ++k) {
        b[k] ^= b2.bitmap.get(k);
      }
      this.cardinality = newCardinality;
      return this;
    }
    final MappeableArrayContainer ac = new MappeableArrayContainer(newCardinality);

    BufferUtil.fillArrayXOR(ac.content.array(), this.bitmap, b2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public MappeableContainer ixor(final MappeableRunContainer x) {
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
      long[] b = this.bitmap.array();
      for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
        int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
        int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
        int prevOnes = Util.cardinalityInBitmapRange(b, start, end);
        Util.flipBitmapRange(b, start, end);
        updateCardinality(prevOnes, end - start - prevOnes);
      }
    } else {
      for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
        int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
        int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
        int prevOnes = cardinalityInRange(start, end);
        BufferUtil.flipBitmapRange(this.bitmap, start, end);
        updateCardinality(prevOnes, end - start - prevOnes);
      }
    }
    if (this.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return this;
    } else {
      return toArrayContainer();
    }
  }

  protected MappeableContainer lazyor(MappeableArrayContainer value2) {
    MappeableBitmapContainer answer = clone();
    answer.cardinality = -1;// invalid
    if (!BufferUtil.isBackedBySimpleArray(answer.bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] b = answer.bitmap.array();
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v2 = value2.content.get(k);
      final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
      b[i] |= 1L << v2;
    }
    return answer;
  }

  protected MappeableContainer lazyor(MappeableBitmapContainer x) {
    MappeableBitmapContainer answer = new MappeableBitmapContainer();
    answer.cardinality = -1;// invalid
    if (!BufferUtil.isBackedBySimpleArray(answer.bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] b = answer.bitmap.array();
    for (int k = 0; k < b.length; k++) {
      b[k] = this.bitmap.get(k) | x.bitmap.get(k);
    }
    return answer;
  }

  protected MappeableContainer lazyor(MappeableRunContainer x) {
    MappeableBitmapContainer bc = clone();
    bc.cardinality = -1;
    if (BufferUtil.isBackedBySimpleArray(bc.bitmap)) {
      long[] b = bc.bitmap.array();
      for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
        int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
        int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
        Util.setBitmapRange(b, start, end);
      }
      return bc;

    }
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
      int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
      BufferUtil.setBitmapRange(bc.bitmap, start, end);
    }
    return bc;
  }

  @Override
  public MappeableContainer limit(int maxcardinality) {
    if (maxcardinality >= this.cardinality) {
      return clone();
    }
    if (maxcardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      MappeableArrayContainer ac = new MappeableArrayContainer(maxcardinality);
      int pos = 0;
      if (!BufferUtil.isBackedBySimpleArray(ac.content)) {
        throw new RuntimeException("Should not happen. Internal bug.");
      }
      short[] cont = ac.content.array();
      int len = this.bitmap.limit();
      for (int k = 0; (ac.cardinality < maxcardinality) && (k < len); ++k) {
        long bitset = bitmap.get(k);
        while ((ac.cardinality < maxcardinality) && (bitset != 0)) {
          cont[pos++] = (short) (k * 64 + numberOfTrailingZeros(bitset));
          ac.cardinality++;
          bitset &= (bitset - 1);
        }
      }
      return ac;
    }
    MappeableBitmapContainer bc = new MappeableBitmapContainer(maxcardinality, this.bitmap);
    int s = BufferUtil.toIntUnsigned(select(maxcardinality));
    int usedwords = (s + 63) / 64;
    int len = this.bitmap.limit();
    int todelete = len - usedwords;
    for (int k = 0; k < todelete; ++k) {
      bc.bitmap.put(len - 1 - k, 0);
    }
    int lastword = s % 64;
    if (lastword != 0) {
      bc.bitmap.put(s / 64, (bc.bitmap.get(s / 64) & (0xFFFFFFFFFFFFFFFFL >>> (64 - lastword))));
    }
    return bc;
  }

  protected void loadData(final MappeableArrayContainer arrayContainer) {
    this.cardinality = arrayContainer.cardinality;
    if (!BufferUtil.isBackedBySimpleArray(bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] bitArray = bitmap.array();
    if (BufferUtil.isBackedBySimpleArray(bitmap)
        && BufferUtil.isBackedBySimpleArray(arrayContainer.content)) {
      long[] b = bitmap.array();
      short[] ac = arrayContainer.content.array();
      for (int k = 0; k < arrayContainer.cardinality; ++k) {
        final short x = ac[k];
        bitArray[BufferUtil.toIntUnsigned(x) / 64] =
            b[BufferUtil.toIntUnsigned(x) / 64] | (1L << x);
      }

    } else {
      for (int k = 0; k < arrayContainer.cardinality; ++k) {
        final short x = arrayContainer.content.get(k);
        bitArray[BufferUtil.toIntUnsigned(x) / 64] =
            bitmap.get(BufferUtil.toIntUnsigned(x) / 64) | (1L << x);
      }
    }
  }

  /**
   * Find the index of the next set bit greater or equal to i, returns -1 if none found.
   * 
   * @param i starting index
   * @return index of the next set bit
   */
  public int nextSetBit(final int i) {
    int x = i >> 6; // signed i / 64
    long w = bitmap.get(x);
    w >>>= i;
    if (w != 0) {
      return i + numberOfTrailingZeros(w);
    }
    // for speed, replaced bitmap.limit() with hardcoded MAX_CAPACITY / 64
    for (++x; x < MAX_CAPACITY / 64; ++x) {
      long X = bitmap.get(x);
      if (X != 0) {
        return x * 64 + numberOfTrailingZeros(X);
      }
    }
    return -1;
  }

  /**
   * Find the index of the next unset bit greater or equal to i, returns -1 if none found.
   *
   * @param i starting index
   * @return index of the next unset bit
   */
  public short nextUnsetBit(final int i) {
    int x = i / 64;
    long w = ~bitmap.get(x);
    w >>>= i;
    if (w != 0) {
      return (short) (i + numberOfTrailingZeros(w));
    }
    ++x;
    // for speed, replaced bitmap.limit() with hardcoded MAX_CAPACITY / 64
    for (; x < MAX_CAPACITY / 64; ++x) {
      long X = bitmap.get(x);
      if (X != ~0L) {
        return (short) (x * 64 + numberOfTrailingZeros(~X));
      }
    }
    return -1;
  }

  @Override
  public MappeableContainer not(final int firstOfRange, final int lastOfRange) {
    MappeableBitmapContainer answer = clone();
    return answer.inot(firstOfRange, lastOfRange);
  }


  @Override
  int numberOfRuns() {
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
      long[] src = this.bitmap.array();
      int numRuns = 0;
      long nextWord = src[0];

      for (int i = 0; i < src.length - 1; i++) {
        long word = nextWord;
        nextWord = src[i + 1];
        numRuns += Long.bitCount((~word) & (word << 1)) + ((word >>> 63) & ~nextWord);
      }

      long word = nextWord;
      numRuns += Long.bitCount((~word) & (word << 1));
      if ((word & 0x8000000000000000L) != 0) {
        numRuns++;
      }

      return numRuns;
    } else {
      int numRuns = 0;
      long nextWord = bitmap.get(0);
      int len = bitmap.limit();

      for (int i = 0; i < len - 1; i++) {
        long word = nextWord;
        nextWord = bitmap.get(i + 1);
        numRuns += Long.bitCount((~word) & (word << 1)) + ((word >>> 63) & ~nextWord);
      }

      long word = nextWord;
      numRuns += Long.bitCount((~word) & (word << 1));
      if ((word & 0x8000000000000000L) != 0) {
        numRuns++;
      }

      return numRuns;

    }
  }

  /**
   * Computes the number of runs
   * 
   * @return the number of runs
   */
  public int numberOfRunsAdjustment() {
    int ans = 0;
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] b = bitmap.array();
      long nextWord = b[0];
      for (int i = 0; i < b.length - 1; i++) {
        final long word = nextWord;

        nextWord = b[i + 1];
        ans += ((word >>> 63) & ~nextWord);
      }

      final long word = nextWord;

      if ((word & 0x8000000000000000L) != 0) {
        ans++;
      }

    } else {
      long nextWord = bitmap.get(0);
      int len = bitmap.limit();
      for (int i = 0; i < len - 1; i++) {
        final long word = nextWord;

        nextWord = bitmap.get(i + 1);
        ans += ((word >>> 63) & ~nextWord);
      }

      final long word = nextWord;

      if ((word & 0x8000000000000000L) != 0) {
        ans++;
      }
    }
    return ans;
  }

  /**
   * Counts how many runs there is in the bitmap, up to a maximum
   * 
   * @param mustNotExceed maximum of runs beyond which counting is pointless
   * @return estimated number of courses
   */
  public int numberOfRunsLowerBound(int mustNotExceed) {
    int numRuns = 0;
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] b = bitmap.array();

      for (int blockOffset = 0; blockOffset < b.length; blockOffset += BLOCKSIZE) {

        for (int i = blockOffset; i < blockOffset + BLOCKSIZE; i++) {
          long word = b[i];
          numRuns += Long.bitCount((~word) & (word << 1));
        }
        if (numRuns > mustNotExceed) {
          return numRuns;
        }
      }
    } else {
      int len = bitmap.limit();
      for (int blockOffset = 0; blockOffset < len; blockOffset += BLOCKSIZE) {

        for (int i = blockOffset; i < blockOffset + BLOCKSIZE; i++) {
          long word = bitmap.get(i);
          numRuns += Long.bitCount((~word) & (word << 1));
        }
        if (numRuns > mustNotExceed) {
          return numRuns;
        }
      }
    }
    return numRuns;
  }



  @Override
  public MappeableContainer or(final MappeableArrayContainer value2) {
    final MappeableBitmapContainer answer = clone();
    if (!BufferUtil.isBackedBySimpleArray(answer.bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] bitArray = answer.bitmap.array();
    if (BufferUtil.isBackedBySimpleArray(answer.bitmap)
        && BufferUtil.isBackedBySimpleArray(value2.content)) {
      long[] ab = answer.bitmap.array();
      short[] v2 = value2.content.array();
      int c = value2.cardinality;
      for (int k = 0; k < c; ++k) {
        short v = v2[k];
        final int i = BufferUtil.toIntUnsigned(v) >>> 6;
        long w = ab[i];
        long aft = w | (1L << v);
        bitArray[i] = aft;
        if (USE_BRANCHLESS) {
          answer.cardinality += (w - aft) >>> 63;
        } else {
          if (w != aft) {
            answer.cardinality++;
          }
        }
      }
    } else {
      int c = value2.cardinality;
      for (int k = 0; k < c; ++k) {
        short v2 = value2.content.get(k);
        final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
        long w = answer.bitmap.get(i);
        long aft = w | (1L << v2);
        bitArray[i] = aft;
        if (USE_BRANCHLESS) {
          answer.cardinality += (w - aft) >>> 63;
        } else {
          if (w != aft) {
            answer.cardinality++;
          }
        }
      }
    }
    if (answer.isFull()) {
      return MappeableRunContainer.full();
    }
    return answer;
  }



  @Override
  public MappeableContainer or(final MappeableBitmapContainer value2) {
    final MappeableBitmapContainer value1 = this.clone();
    return value1.ior(value2);
  }

  @Override
  public MappeableContainer or(final MappeableRunContainer value2) {
    return value2.or(this);
  }



  /**
   * Find the index of the previous set bit less than or equal to i, returns -1 if none found.
   *
   * @param i starting index
   * @return index of the previous set bit
   */
  public int prevSetBit(final int i) {
    int x = i >> 6; // signed i / 64
    long w = bitmap.get(x);
    w <<= 64 - i - 1;
    if (w != 0) {
      return i - Long.numberOfLeadingZeros(w);
    }
    for (--x; x >= 0; --x) {
      long X = bitmap.get(x);
      if (X != 0) {
        return x * 64 + 63 - Long.numberOfLeadingZeros(X);
      }
    }
    return -1;
  }

  @Override
  public int rank(short lowbits) {
    int x = BufferUtil.toIntUnsigned(lowbits);
    int leftover = (x + 1) & 63;
    int answer = 0;
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
      long[] b = this.bitmap.array();
      for (int k = 0; k < (x + 1) / 64; ++k) {
        answer += Long.bitCount(b[k]);
      }
      if (leftover != 0) {
        answer += Long.bitCount(b[(x + 1) / 64] << (64 - leftover));
      }
    } else {
      for (int k = 0; k < (x + 1) / 64; ++k) {
        answer += Long.bitCount(bitmap.get(k));
      }
      if (leftover != 0) {
        answer += Long.bitCount(bitmap.get((x + 1) / 64) << (64 - leftover));
      }
    }
    return answer;
  }


  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // little endian
    this.cardinality = 0;
    int len = this.bitmap.limit();
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] b = this.bitmap.array();
      for (int k = 0; k < len; ++k) {
        long w = Long.reverseBytes(in.readLong());
        b[k] = w;
        this.cardinality += Long.bitCount(w);
      }
    } else {
      for (int k = 0; k < len; ++k) {
        long w = Long.reverseBytes(in.readLong());
        bitmap.put(k, w);
        this.cardinality += Long.bitCount(w);
      }
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
    MappeableBitmapContainer answer = clone();
    int prevOnesInRange = answer.cardinalityInRange(begin, end);
    BufferUtil.resetBitmapRange(answer.bitmap, begin, end);
    answer.updateCardinality(prevOnesInRange, 0);
    if (answer.getCardinality() < MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return answer.toArrayContainer();
    }
    return answer;
  }



  @Override
  public MappeableContainer remove(final short i) {
    final int x = BufferUtil.toIntUnsigned(i);
    long X = bitmap.get(x / 64);
    long mask = 1L << x;

    if (cardinality == MappeableArrayContainer.DEFAULT_MAX_SIZE + 1) {// this is
      // the
      // uncommon
      // path
      if ((X & mask) != 0) {
        --cardinality;
        bitmap.put(x / 64, X & (~mask));
        return this.toArrayContainer();
      }
    }
    long aft = X & ~(mask);
    cardinality -= (aft - X) >>> 63;
    bitmap.put(x / 64, aft);
    return this;
  }


  @Override
  public MappeableContainer repairAfterLazy() {
    if (getCardinality() < 0) {
      computeCardinality();
      if(getCardinality() <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
        return this.toArrayContainer();
      } else if (isFull()) {
        return MappeableRunContainer.full();
      }
    }
    return this;
  }

  @Override
  public MappeableContainer runOptimize() {
    int numRuns = numberOfRunsLowerBound(MAXRUNS); // decent choice

    int sizeAsRunContainerLowerBound = MappeableRunContainer.serializedSizeInBytes(numRuns);

    if (sizeAsRunContainerLowerBound >= getArraySizeInBytes()) {
      return this;
    }
    // else numRuns is a relatively tight bound that needs to be exact
    // in some cases (or if we need to make the runContainer the right
    // size)
    numRuns += numberOfRunsAdjustment();
    int sizeAsRunContainer = MappeableRunContainer.serializedSizeInBytes(numRuns);

    if (getArraySizeInBytes() > sizeAsRunContainer) {
      return new MappeableRunContainer(this, numRuns);
    } else {
      return this;
    }
  }

  @Override
  public short select(int j) {
    int leftover = j;
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
      long[] b = this.bitmap.array();

      for (int k = 0; k < b.length; ++k) {
        int w = Long.bitCount(b[k]);
        if (w > leftover) {
          return (short) (k * 64 + Util.select(b[k], leftover));
        }
        leftover -= w;
      }
    } else {
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        long X = bitmap.get(k);
        int w = Long.bitCount(X);
        if (w > leftover) {
          return (short) (k * 64 + Util.select(X, leftover));
        }
        leftover -= w;
      }
    }
    throw new IllegalArgumentException("Insufficient cardinality.");
  }

  @Override
  public int serializedSizeInBytes() {
    return serializedSizeInBytes(0);
  }

  /**
   * Copies the data to an array container
   * 
   * @return the array container
   */
  public MappeableArrayContainer toArrayContainer() {
    final MappeableArrayContainer ac = new MappeableArrayContainer(cardinality);
    ac.loadData(this);
    if (ac.getCardinality() != cardinality) {
      throw new RuntimeException("Internal error.");
    }
    return ac;
  }

  @Override
  public Container toContainer() {
    return new BitmapContainer(this);
  }

  /**
   * Create a copy of the content of this container as a long array. This creates a copy.
   *
   * @return copy of the content as a long array
   */
  public long[] toLongArray() {
    long[] answer = new long[bitmap.limit()];
    bitmap.rewind();
    bitmap.get(answer);
    return answer;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    final ShortIterator i = this.getShortIterator();
    sb.append("{");
    while (i.hasNext()) {
      sb.append(BufferUtil.toIntUnsigned(i.next()));
      if (i.hasNext()) {
        sb.append(",");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public void trim() {}

  @Override
  protected void writeArray(DataOutput out) throws IOException {
    // little endian
    int len = this.bitmap.limit();
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] b = bitmap.array();
      for (int k = 0; k < len; ++k) {
        out.writeLong(Long.reverseBytes(b[k]));
      }
    } else {
      for (int k = 0; k < len; ++k) {
        final long w = bitmap.get(k);
        out.writeLong(Long.reverseBytes(w));
      }
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    writeArray(out);
  }

  @Override
  public MappeableContainer xor(final MappeableArrayContainer value2) {
    final MappeableBitmapContainer answer = clone();
    if (!BufferUtil.isBackedBySimpleArray(answer.bitmap)) {
      throw new RuntimeException("Should not happen. Internal bug.");
    }
    long[] bitArray = answer.bitmap.array();
    if (BufferUtil.isBackedBySimpleArray(value2.content)) {
      short[] v2 = value2.content.array();
      int c = value2.cardinality;
      for (int k = 0; k < c; ++k) {
        short vc = v2[k];
        long mask = 1L << vc;
        final int index = BufferUtil.toIntUnsigned(vc) >>> 6;
        long ba = bitArray[index];
        // TODO: check whether a branchy version could be faster
        answer.cardinality += 1 - 2 * ((ba & mask) >>> vc);
        bitArray[index] = ba ^ mask;
      }
    } else {
      int c = value2.cardinality;
      for (int k = 0; k < c; ++k) {
        short v2 = value2.content.get(k);
        long mask = 1L << v2;
        final int index = BufferUtil.toIntUnsigned(v2) >>> 6;
        long ba = bitArray[index];
        // TODO: check whether a branchy version could be faster
        answer.cardinality += 1 - 2 * ((ba & mask) >>> v2);
        bitArray[index] = ba ^ mask;
      }
    }
    if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return answer.toArrayContainer();
    }
    return answer;
  }

  @Override
  public MappeableContainer xor(MappeableBitmapContainer value2) {
    int newCardinality = 0;
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)
        && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
      long[] b = this.bitmap.array();
      long[] v2 = value2.bitmap.array();
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(b[k] ^ v2[k]);
      }
    } else {
      int len = this.bitmap.limit();
      for (int k = 0; k < len; ++k) {
        newCardinality += Long.bitCount(this.bitmap.get(k) ^ value2.bitmap.get(k));
      }
    }
    if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      final MappeableBitmapContainer answer = new MappeableBitmapContainer();
      long[] bitArray = answer.bitmap.array();
      if (BufferUtil.isBackedBySimpleArray(this.bitmap)
          && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
        long[] b = this.bitmap.array();
        long[] v2 = value2.bitmap.array();
        int len = answer.bitmap.limit();
        for (int k = 0; k < len; ++k) {
          bitArray[k] = b[k] ^ v2[k];
        }
      } else {
        int len = answer.bitmap.limit();
        for (int k = 0; k < len; ++k) {
          bitArray[k] = this.bitmap.get(k) ^ value2.bitmap.get(k);
        }
      }
      answer.cardinality = newCardinality;
      return answer;
    }
    final MappeableArrayContainer ac = new MappeableArrayContainer(newCardinality);
    BufferUtil.fillArrayXOR(ac.content.array(), this.bitmap, value2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public MappeableContainer xor(final MappeableRunContainer value2) {
    return value2.xor(this);
  }

  @Override
  public void forEach(short msb, IntConsumer ic) {
    int high = ((int) msb) << 16;
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] b = bitmap.array();
      for (int x = 0; x < b.length; ++x) {
        long w = b[x];
        while (w != 0) {
          ic.accept((x * 64 + numberOfTrailingZeros(w)) | high);
          w &= (w - 1);
        }
      }
    } else {
      int l = bitmap.limit();
      for (int x = 0; x < l; ++x) {
        long w = bitmap.get(x);
        while (w != 0) {
          ic.accept((x * 64 + numberOfTrailingZeros(w)) | high);
          w &= (w - 1);
        }
      }
    }
  }


  @Override
  public int andCardinality(final MappeableArrayContainer value2) {
    int answer = 0;
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content.get(k);
      answer += this.bitValue(v);
    }
    return answer;
  }

  @Override
  public int andCardinality(final MappeableBitmapContainer value2) {
    int newCardinality = 0;
    if (BufferUtil.isBackedBySimpleArray(this.bitmap)
        && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
      long[] b1 = this.bitmap.array();
      long[] b2 = value2.bitmap.array();
      for (int k = 0; k < b1.length; ++k) {
        newCardinality += Long.bitCount(b1[k] & b2[k]);
      }
    } else {
      final int size = this.bitmap.limit();
      for (int k = 0; k < size; ++k) {
        newCardinality += Long.bitCount(this.bitmap.get(k) & value2.bitmap.get(k));
      }
    }
    return newCardinality;
  }

  @Override
  public int andCardinality(MappeableRunContainer x) {
    return x.andCardinality(this);
  }

  @Override
  public MappeableBitmapContainer toBitmapContainer() {
    return this;
  }

  @Override
  public int first() {
    assertNonEmpty(cardinality == 0);
    long firstNonZeroWord;
    int i = 0;
    if(BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] array = bitmap.array();
      while(array[i] == 0) {
        ++i;
      }
      firstNonZeroWord = array[i];
    } else {
      i = bitmap.position();
      while(bitmap.get(i) == 0) {
        ++i;
      }
      firstNonZeroWord = bitmap.get(i);
    }
    return i * 64 + numberOfTrailingZeros(firstNonZeroWord);
  }

  @Override
  public int last() {
    assertNonEmpty(cardinality == 0);
    long lastNonZeroWord;
    int i = bitmap.limit() - 1;
    if(BufferUtil.isBackedBySimpleArray(bitmap)) {
      long[] array = this.bitmap.array();
      while(i > 0 && array[i] == 0) {
        --i;
      }
      lastNonZeroWord = array[i];
    } else {
      while(i > 0 && bitmap.get(i) == 0) {
        --i;
      }
      lastNonZeroWord = bitmap.get(i);
    }
    return (i + 1) * 64 - Long.numberOfLeadingZeros(lastNonZeroWord) - 1;
  }

  @Override
  protected boolean contains(MappeableBitmapContainer bitmapContainer) {
    if((cardinality != -1) && (bitmapContainer.cardinality != -1)) {
      if(cardinality < bitmapContainer.cardinality) {
        return false;
      }
    }
    for(int i = 0; i < MAX_CAPACITY / 64; ++i ) {
      if((this.bitmap.get(i) & bitmapContainer.bitmap.get(i)) != bitmapContainer.bitmap.get(i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean intersects(int minimum, int supremum) {
    int start = minimum >>> 6;
    int end = supremum >>> 6;
    if (start == end) {
      return ((bitmap.get(start) & (~((1L << minimum) - 1) & ((1L << supremum) - 1))) != 0);
    }
    if ((bitmap.get(start) & ~((1L << minimum) - 1)) != 0) {
      return true;
    }
    if (end < bitmap.limit() && (bitmap.get(end) & ((1L << supremum) - 1)) != 0) {
      return true;
    }
    for (int i = 1 + start; i < end && i < bitmap.limit(); ++i) {
      if (bitmap.get(i) != 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean contains(int minimum, int supremum) {
    int start = minimum >>> 6;
    int end = supremum >>> 6;
    long first = ~((1L << minimum) - 1);
    long last = ((1L << supremum) - 1);
    if (start == end) {
      return ((bitmap.get(end) & first & last) == (first & last));
    }
    if ((bitmap.get(start) & first) != first) {
      return false;
    }
    if (end < bitmap.limit() && (bitmap.get(end) & last) != last) {
      return false;
    }
    for (int i = start + 1; i < bitmap.limit() && i < end; ++i) {
      if (bitmap.get(i) != -1L) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean contains(MappeableRunContainer runContainer) {
    final int runCardinality = runContainer.getCardinality();
    if (cardinality != -1) {
      if (cardinality < runCardinality) {
        return false;
      }
    } else {
      int card = cardinality;
      if (card < runCardinality) {
        return false;
      }
    }
    for (int i = 0; i < runContainer.numberOfRuns(); ++i) {
      short runStart = runContainer.getValue(i);
      int le = BufferUtil.toIntUnsigned(runContainer.getLength(i));
      for (short j = runStart; j <= runStart + le; ++j) {
        if (!contains(j)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected boolean contains(MappeableArrayContainer arrayContainer) {
    if (arrayContainer.cardinality != -1) {
      if (cardinality < arrayContainer.cardinality) {
        return false;
      }
    }
    for (int i = 0; i < arrayContainer.cardinality; ++i) {
      if(!contains(arrayContainer.content.get(i))) {
        return false;
      }
    }
    return true;
  }


}


final class MappeableBitmapContainerShortIterator implements PeekableShortIterator {
  final static int len = MappeableBitmapContainer.MAX_CAPACITY / 64;// hard coded for speed
  long w;
  int x;


  MappeableBitmapContainer parent;

  MappeableBitmapContainerShortIterator() {}

  MappeableBitmapContainerShortIterator(MappeableBitmapContainer p) {
    wrap(p);
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
    return x < len;
  }

  @Override
  public short next() {
    short answer = (short) (x * 64 + numberOfTrailingZeros(w));
    w &= (w - 1);
    while (w == 0) {
      ++x;
      if (x == len) {
        break;
      }
      w = parent.bitmap.get(x);
    }
    return answer;
  }

  @Override
  public int nextAsInt() {
    return BufferUtil.toIntUnsigned(next());
  }

  @Override
  public void remove() {
    // TODO: implement
    throw new RuntimeException("unsupported operation: remove");
  }

  void wrap(MappeableBitmapContainer p) {
    parent = p;
    for (x = 0; x < len; ++x) {
      if ((w = parent.bitmap.get(x)) != 0) {
        break;
      }
    }
  }

  @Override
  public void advanceIfNeeded(short minval) {
    if (BufferUtil.toIntUnsigned(minval) >= (x + 1) * 64) {
      x = BufferUtil.toIntUnsigned(minval) / 64;
      w = parent.bitmap.get(x);
      while (w == 0) {
        ++x;
        if (x == len) {
          return;
        }
        w = parent.bitmap.get(x);
      }
    }
    while (hasNext() && (BufferUtil.toIntUnsigned(peekNext()) < BufferUtil.toIntUnsigned(minval))) {
      next(); // could be optimized
    }

  }

  @Override
  public short peekNext() {
    return (short) (x * 64 + numberOfTrailingZeros(w));
  }
}


final class ReverseMappeableBitmapContainerShortIterator implements ShortIterator {

  final static int len = MappeableBitmapContainer.MAX_CAPACITY / 64;// hard coded for speed
  long w;
  int x;

  MappeableBitmapContainer parent;

  ReverseMappeableBitmapContainerShortIterator() {}

  ReverseMappeableBitmapContainerShortIterator(MappeableBitmapContainer p) {
    wrap(p);
  }

  @Override
  public ShortIterator clone() {
    try {
      return (ShortIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    return x >= 0;
  }

  @Override
  public short next() {
    int shift = Long.numberOfLeadingZeros(w) + 1;
    short answer = (short)((x + 1) * 64 - shift);
    w &= ~(1L << (64 - shift));
    while (w == 0) {
      --x;
      if (x < 0) {
        break;
      }
      w = parent.bitmap.get(x);
    }
    return answer;
  }

  @Override
  public int nextAsInt() {
    return BufferUtil.toIntUnsigned(next());
  }

  @Override
  public void remove() {
    // TODO: implement
    throw new RuntimeException("unsupported operation: remove");
  }

  public void wrap(MappeableBitmapContainer p) {
    parent = p;
    for (x = len - 1; x >= 0; --x) {
      if ((w = parent.bitmap.get(x)) != 0) {
        break;
      }
    }
  }
}
