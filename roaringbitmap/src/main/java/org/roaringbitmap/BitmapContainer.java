/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import org.roaringbitmap.buffer.MappeableBitmapContainer;
import org.roaringbitmap.buffer.MappeableContainer;

import java.io.*;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Iterator;

import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Long.numberOfTrailingZeros;


/**
 * Simple bitset-like container.
 */
public final class BitmapContainer extends Container implements Cloneable {
  protected static final int MAX_CAPACITY = 1 << 16;


  private static final long serialVersionUID = 2L;

  // bail out early when the number of runs is excessive, without
  // an exact count (just a decent lower bound)
  private static final int BLOCKSIZE = 128;
  // 64 words can have max 32 runs per word, max 2k runs

  /**
   * optimization flag: whether the cardinality of the bitmaps is maintained through branchless
   * operations
   */
  public static final boolean USE_BRANCHLESS = true;

  /**
   * Return a bitmap iterator over this array
   *
   * @param bitmap array to be iterated over
   * @return an iterator
   */
  public static ShortIterator getReverseShortIterator(long[] bitmap) {
    return new ReverseBitmapContainerShortIterator(bitmap);
  }

  /**
   * Return a bitmap iterator over this array
   *
   * @param bitmap array to be iterated over
   * @return an iterator
   */
  public static PeekableShortIterator getShortIterator(long[] bitmap) {
    return new BitmapContainerShortIterator(bitmap);
  }

  // the parameter is for overloading and symmetry with ArrayContainer
  protected static int serializedSizeInBytes(int unusedCardinality) {
    return MAX_CAPACITY / 8;
  }

  final long[] bitmap;

  int cardinality;

  // nruns value for which RunContainer.serializedSizeInBytes ==
  // BitmapContainer.getArraySizeInBytes()
  private final int MAXRUNS = (getArraySizeInBytes() - 2) / 4;


  /**
   * Create a bitmap container with all bits set to false
   */
  public BitmapContainer() {
    this.cardinality = 0;
    this.bitmap = new long[MAX_CAPACITY / 64];
  }



  /**
   * Create a bitmap container with a run of ones from firstOfRun to lastOfRun. Caller must ensure
   * that the range isn't so small that an ArrayContainer should have been created instead
   *
   * @param firstOfRun first index
   * @param lastOfRun last index (range is exclusive)
   */
  public BitmapContainer(final int firstOfRun, final int lastOfRun) {
    this.cardinality = lastOfRun - firstOfRun;
    this.bitmap = new long[MAX_CAPACITY / 64];
    Util.setBitmapRange(bitmap, firstOfRun, lastOfRun);
  }

  private BitmapContainer(int newCardinality, long[] newBitmap) {
    this.cardinality = newCardinality;
    this.bitmap = Arrays.copyOf(newBitmap, newBitmap.length);
  }

  /**
   * Create a new container, no copy is made.
   *
   * @param newBitmap content
   * @param newCardinality desired cardinality.
   */
  public BitmapContainer(long[] newBitmap, int newCardinality) {
    this.cardinality = newCardinality;
    this.bitmap = newBitmap;
  }



  /**
   * Creates a new non-mappeable container from a mappeable one. This copies the data.
   *
   * @param bc the original container
   */
  public BitmapContainer(MappeableBitmapContainer bc) {
    this.cardinality = bc.getCardinality();
    this.bitmap = bc.toLongArray();
  }


  @Override
  public Container add(int begin, int end) {
    // TODO: may need to convert to a RunContainer
    if (end == begin) {
      return clone();
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    BitmapContainer answer = clone();
    int prevOnesInRange = answer.cardinalityInRange(begin, end);
    Util.setBitmapRange(answer.bitmap, begin, end);
    answer.updateCardinality(prevOnesInRange, end - begin);
    return answer;
  }



  @Override
  public Container add(final short i) {
    final int x = Util.toIntUnsigned(i);
    final long previous = bitmap[x / 64];
    long newval = previous | (1L << x);
    bitmap[x / 64] = newval;
    if (USE_BRANCHLESS) {
      cardinality += (previous ^ newval) >>> x;
    } else if (previous != newval) {
      ++cardinality;
    }
    return this;
  }

  @Override
  public ArrayContainer and(final ArrayContainer value2) {
    final ArrayContainer answer = new ArrayContainer(value2.content.length);
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content[k];
      answer.content[answer.cardinality] = v;
      answer.cardinality += this.bitValue(v);
    }
    return answer;
  }

  @Override
  public Container and(final BitmapContainer value2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] & value2.bitmap[k]);
    }
    if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
      final BitmapContainer answer = new BitmapContainer();
      for (int k = 0; k < answer.bitmap.length; ++k) {
        answer.bitmap[k] = this.bitmap[k] & value2.bitmap[k];
      }
      answer.cardinality = newCardinality;
      return answer;
    }
    ArrayContainer ac = new ArrayContainer(newCardinality);
    Util.fillArrayAND(ac.content, this.bitmap, value2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public Container and(RunContainer x) {
    return x.and(this);
  }

  @Override
  public int andCardinality(final ArrayContainer value2) {
    int answer = 0;
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content[k];
      answer += this.bitValue(v);
    }
    return answer;
  }

  @Override
  public int andCardinality(final BitmapContainer value2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] & value2.bitmap[k]);
    }
    return newCardinality;
  }

  @Override
  public int andCardinality(RunContainer x) {
    return x.andCardinality(this);
  }

  @Override
  public Container andNot(final ArrayContainer value2) {
    final BitmapContainer answer = clone();
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content[k];
      final int i = Util.toIntUnsigned(v) >>> 6;
      long w = answer.bitmap[i];
      long aft = w & (~(1L << v));
      answer.bitmap[i] = aft;
      answer.cardinality -= (w ^ aft) >>> v;
    }
    if (answer.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return answer.toArrayContainer();
    }
    return answer;
  }

  @Override
  public Container andNot(final BitmapContainer value2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] & (~value2.bitmap[k]));
    }
    if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
      final BitmapContainer answer = new BitmapContainer();
      for (int k = 0; k < answer.bitmap.length; ++k) {
        answer.bitmap[k] = this.bitmap[k] & (~value2.bitmap[k]);
      }
      answer.cardinality = newCardinality;
      return answer;
    }
    ArrayContainer ac = new ArrayContainer(newCardinality);
    Util.fillArrayANDNOT(ac.content, this.bitmap, value2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public Container andNot(RunContainer x) {
    // could be rewritten as return andNot(x.toBitmapOrArrayContainer());
    BitmapContainer answer = this.clone();
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(x.getValue(rlepos));
      int end = start + Util.toIntUnsigned(x.getLength(rlepos)) + 1;
      int prevOnesInRange = answer.cardinalityInRange(start, end);
      Util.resetBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnesInRange, 0);
    }
    if (answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayContainer();
    }
  }

  @Override
  public void clear() {
    if (cardinality != 0) {
      cardinality = 0;
      Arrays.fill(bitmap, 0);
    }
  }

  @Override
  public BitmapContainer clone() {
    return new BitmapContainer(this.cardinality, this.bitmap);
  }

  @Override
  public boolean isEmpty() {
    return cardinality == 0;
  }

  /**
   * Recomputes the cardinality of the bitmap.
   */
  protected void computeCardinality() {
    this.cardinality = 0;
    for (int k = 0; k < this.bitmap.length; k++) {
      this.cardinality += Long.bitCount(this.bitmap[k]);
    }
  }

  protected int cardinalityInRange(int start, int end) {
    assert (cardinality != -1);
    if (end - start > MAX_CAPACITY / 2) {
      int before = Util.cardinalityInBitmapRange(bitmap, 0, start);
      int after = Util.cardinalityInBitmapRange(bitmap, end, MAX_CAPACITY);
      return cardinality - before - after;
    }
    return Util.cardinalityInBitmapRange(bitmap, start, end);
  }

  protected void updateCardinality(int prevOnes, int newOnes) {
    int oldCardinality = this.cardinality;
    this.cardinality = oldCardinality - prevOnes + newOnes;
  }

  @Override
  public boolean contains(final short i) {
    final int x = Util.toIntUnsigned(i);
    return (bitmap[x / 64] & (1L << x)) != 0;
  }

  @Override
  public boolean contains(int minimum, int supremum) {
    int start = minimum >>> 6;
    int end = supremum >>> 6;
    long first = ~((1L << minimum) - 1);
    long last = ((1L << supremum) - 1);
    if (start == end) {
      return ((bitmap[end] & first & last) == (first & last));
    }
    if ((bitmap[start] & first) != first) {
      return false;
    }
    if (end < bitmap.length && (bitmap[end] & last) != last) {
      return false;
    }
    for (int i = start + 1; i < bitmap.length && i < end; ++i) {
      if (bitmap[i] != -1L) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean contains(BitmapContainer bitmapContainer) {
    if((cardinality != -1) && (bitmapContainer.cardinality != -1)) {
      if(cardinality < bitmapContainer.cardinality) {
        return false;
      }
    }
    for(int i = 0; i < bitmapContainer.bitmap.length; ++i ) {
      if((this.bitmap[i] & bitmapContainer.bitmap[i]) != bitmapContainer.bitmap[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean contains(RunContainer runContainer) {
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
      int le = Util.toIntUnsigned(runContainer.getLength(i));
      for (short j = runStart; j <= runStart + le; ++j) {
        if (!contains(j)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected boolean contains(ArrayContainer arrayContainer) {
    if (arrayContainer.cardinality != -1) {
      if (cardinality < arrayContainer.cardinality) {
        return false;
      }
    }
    for (int i = 0; i < arrayContainer.cardinality; ++i) {
      if(!contains(arrayContainer.content[i])) {
        return false;
      }
    }
    return true;
  }


  protected long bitValue(final short i) {
    final int x = Util.toIntUnsigned(i);
    return (bitmap[x / 64] >>> x ) & 1;
  }


  @Override
  public void deserialize(DataInput in) throws IOException {
    // little endian
    this.cardinality = 0;
    for (int k = 0; k < bitmap.length; ++k) {
      long w = Long.reverseBytes(in.readLong());
      bitmap[k] = w;
      this.cardinality += Long.bitCount(w);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BitmapContainer) {
      BitmapContainer srb = (BitmapContainer) o;
      if (srb.cardinality != this.cardinality) {
        return false;
      }
      return Arrays.equals(this.bitmap, srb.bitmap);
    } else if (o instanceof RunContainer) {
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
    int base = 0;
    for (int k = 0; k < bitmap.length; ++k) {
      long bitset = bitmap[k];
      while (bitset != 0) {
        array[pos++] = (short) (base + numberOfTrailingZeros(bitset));
        bitset &= (bitset - 1);
      }
      base += 64;
    }
  }

  @Override
  public void fillLeastSignificant16bits(int[] x, int i, int mask) {
    int pos = i;
    int base = mask;
    for (int k = 0; k < bitmap.length; ++k) {
      long bitset = bitmap[k];
      while (bitset != 0) {
        x[pos++] = base + numberOfTrailingZeros(bitset);
        bitset &= (bitset - 1);
      }
      base += 64;
    }
  }


  @Override
  public Container flip(short i) {
    final int x = Util.toIntUnsigned(i);
    int index = x / 64;
    long bef = bitmap[index];
    long mask = 1L << x;
    if (cardinality == ArrayContainer.DEFAULT_MAX_SIZE + 1) {// this is
      // the
      // uncommon
      // path
      if ((bef & mask) != 0) {
        --cardinality;
        bitmap[index] &= ~mask;
        return this.toArrayContainer();
      }
    }
    // TODO: check whether a branchy version could be faster
    cardinality += 1 - 2 * ((bef & mask) >>> x);
    bitmap[index] ^= mask;
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
    return new ReverseBitmapContainerShortIterator(this.bitmap);
  }

  @Override
  public PeekableShortIterator getShortIterator() {
    return new BitmapContainerShortIterator(this.bitmap);
  }

  @Override
  public ContainerBatchIterator getBatchIterator() {
    return new BitmapBatchIterator(this);
  }

  @Override
  public int getSizeInBytes() {
    return this.bitmap.length * 8;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.bitmap);
  }

  @Override
  public Container iadd(int begin, int end) {
    // TODO: may need to convert to a RunContainer
    if (end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int prevOnesInRange = cardinalityInRange(begin, end);
    Util.setBitmapRange(bitmap, begin, end);
    updateCardinality(prevOnesInRange, end - begin);
    return this;
  }

  @Override
  public Container iand(final ArrayContainer b2) {
    return b2.and(this);// no inplace possible
  }

  @Override
  public Container iand(final BitmapContainer b2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] & b2.bitmap[k]);
    }
    if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
      for (int k = 0; k < this.bitmap.length; ++k) {
        this.bitmap[k] = this.bitmap[k] & b2.bitmap[k];
      }
      this.cardinality = newCardinality;
      return this;
    }
    ArrayContainer ac = new ArrayContainer(newCardinality);
    Util.fillArrayAND(ac.content, this.bitmap, b2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public Container iand(RunContainer x) {
    // could probably be replaced with return iand(x.toBitmapOrArrayContainer());
    final int card = x.getCardinality();
    if (card <= ArrayContainer.DEFAULT_MAX_SIZE) {
      // no point in doing it in-place
      ArrayContainer answer = new ArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
        int runStart = Util.toIntUnsigned(x.getValue(rlepos));
        int runEnd = runStart + Util.toIntUnsigned(x.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          answer.content[answer.cardinality] = (short) runValue;
          answer.cardinality += this.bitValue((short) runValue);
        }
      }
      return answer;
    }
    int start = 0;
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int end = Util.toIntUnsigned(x.getValue(rlepos));
      int prevOnes = cardinalityInRange(start, end);
      Util.resetBitmapRange(this.bitmap, start, end);
      updateCardinality(prevOnes, 0);
      start = end + Util.toIntUnsigned(x.getLength(rlepos)) + 1;
    }
    int ones = cardinalityInRange(start, MAX_CAPACITY);
    Util.resetBitmapRange(this.bitmap, start, MAX_CAPACITY);
    updateCardinality(ones, 0);
    if (getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE) {
      return this;
    } else {
      return toArrayContainer();
    }
  }

  @Override
  public Container iandNot(final ArrayContainer b2) {
    for (int k = 0; k < b2.cardinality; ++k) {
      this.remove(b2.content[k]);
    }
    if (cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return this.toArrayContainer();
    }
    return this;
  }

  @Override
  public Container iandNot(final BitmapContainer b2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] & (~b2.bitmap[k]));
    }
    if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
      for (int k = 0; k < this.bitmap.length; ++k) {
        this.bitmap[k] = this.bitmap[k] & (~b2.bitmap[k]);
      }
      this.cardinality = newCardinality;
      return this;
    }
    ArrayContainer ac = new ArrayContainer(newCardinality);
    Util.fillArrayANDNOT(ac.content, this.bitmap, b2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public Container iandNot(RunContainer x) {
    // could probably be replaced with return iandNot(x.toBitmapOrArrayContainer());
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(x.getValue(rlepos));
      int end = start + Util.toIntUnsigned(x.getLength(rlepos)) + 1;
      int prevOnesInRange = cardinalityInRange(start, end);
      Util.resetBitmapRange(this.bitmap, start, end);
      updateCardinality(prevOnesInRange, 0);
    }
    if (getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE) {
      return this;
    } else {
      return toArrayContainer();
    }
  }

  protected Container ilazyor(ArrayContainer value2) {
    this.cardinality = -1;// invalid
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content[k];
      final int i = Util.toIntUnsigned(v) >>> 6;
      this.bitmap[i] |= (1L << v);
    }
    return this;
  }

  protected Container ilazyor(BitmapContainer x) {
    this.cardinality = -1;// invalid
    for (int k = 0; k < this.bitmap.length; k++) {
      this.bitmap[k] |= x.bitmap[k];
    }
    return this;
  }

  protected Container ilazyor(RunContainer x) {
    // could be implemented as return ilazyor(x.toTemporaryBitmap());
    cardinality = -1; // invalid
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(x.getValue(rlepos));
      int end = start + Util.toIntUnsigned(x.getLength(rlepos)) + 1;
      Util.setBitmapRange(this.bitmap, start, end);
    }
    return this;
  }

  @Override
  public Container inot(final int firstOfRange, final int lastOfRange) {
    int prevOnes = cardinalityInRange(firstOfRange, lastOfRange);
    Util.flipBitmapRange(bitmap, firstOfRange, lastOfRange);
    updateCardinality(prevOnes, lastOfRange - firstOfRange - prevOnes);
    if (cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return toArrayContainer();
    }
    return this;
  }

  @Override
  public boolean intersects(ArrayContainer value2) {
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      if (this.contains(value2.content[k])) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(BitmapContainer value2) {
    for (int k = 0; k < this.bitmap.length; ++k) {
      if ((this.bitmap[k] & value2.bitmap[k]) != 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(RunContainer x) {
    return x.intersects(this);
  }

  @Override
  public boolean intersects(int minimum, int supremum) {
    int start = minimum >>> 6;
    int end = supremum >>> 6;
    if (start == end) {
      return ((bitmap[end] & (~((1L << minimum) - 1) & ((1L << supremum) - 1))) != 0);
    }
    if ((bitmap[start] & ~((1L << minimum) - 1)) != 0) {
      return true;
    }
    if (end < bitmap.length && (bitmap[end] & ((1L << supremum) - 1)) != 0) {
      return true;
    }
    for (int i = 1 + start; i < end && i < bitmap.length; ++i) {
      if (bitmap[i] != 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public BitmapContainer ior(final ArrayContainer value2) {
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;

      long bef = this.bitmap[i];
      long aft = bef | (1L << value2.content[k]);
      this.bitmap[i] = aft;
      if (USE_BRANCHLESS) {
        cardinality += (bef - aft) >>> 63;
      } else {
        if (bef != aft) {
          cardinality++;
        }
      }
    }
    return this;
  }

  @Override
  public Container ior(final BitmapContainer b2) {
    this.cardinality = 0;
    for (int k = 0; k < this.bitmap.length; k++) {
      long w = this.bitmap[k] | b2.bitmap[k];
      this.bitmap[k] = w;
      this.cardinality += Long.bitCount(w);
    }
    if (isFull()) {
      return RunContainer.full();
    }
    return this;
  }

  @Override
  public Container ior(RunContainer x) {
    // could probably be replaced with return ior(x.toBitmapOrArrayContainer());
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(x.getValue(rlepos));
      int end = start + Util.toIntUnsigned(x.getLength(rlepos)) + 1;
      int prevOnesInRange = cardinalityInRange(start, end);
      Util.setBitmapRange(this.bitmap, start, end);
      updateCardinality(prevOnesInRange, end - start);
    }
    if (isFull()) {
      return RunContainer.full();
    }
    return this;
  }

  @Override
  public Container iremove(int begin, int end) {
    if (end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    int prevOnesInRange = cardinalityInRange(begin, end);
    Util.resetBitmapRange(bitmap, begin, end);
    updateCardinality(prevOnesInRange, 0);
    if (getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return toArrayContainer();
    }
    return this;
  }

  @Override
  public Iterator<Short> iterator() {
    return new Iterator<Short>() {
      final ShortIterator si = BitmapContainer.this.getShortIterator();

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
  public Container ixor(final ArrayContainer value2) {
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short vc = value2.content[k];
      long mask = 1L << vc;
      final int index = Util.toIntUnsigned(vc) >>> 6;
      long ba = this.bitmap[index];
      // TODO: check whether a branchy version could be faster
      this.cardinality += 1 - 2 * ((ba & mask) >>> vc);
      this.bitmap[index] = ba ^ mask;
    }
    if (this.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return this.toArrayContainer();
    }
    return this;
  }


  @Override
  public Container ixor(BitmapContainer b2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] ^ b2.bitmap[k]);
    }
    if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
      for (int k = 0; k < this.bitmap.length; ++k) {
        this.bitmap[k] = this.bitmap[k] ^ b2.bitmap[k];
      }
      this.cardinality = newCardinality;
      return this;
    }
    ArrayContainer ac = new ArrayContainer(newCardinality);
    Util.fillArrayXOR(ac.content, this.bitmap, b2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public Container ixor(RunContainer x) {
    // could probably be replaced with return ixor(x.toBitmapOrArrayContainer());
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(x.getValue(rlepos));
      int end = start + Util.toIntUnsigned(x.getLength(rlepos)) + 1;
      int prevOnes = cardinalityInRange(start, end);
      Util.flipBitmapRange(this.bitmap, start, end);
      updateCardinality(prevOnes, end - start - prevOnes);
    }
    if (this.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE) {
      return this;
    } else {
      return toArrayContainer();
    }
  }

  protected Container lazyor(ArrayContainer value2) {
    BitmapContainer answer = this.clone();
    answer.cardinality = -1;// invalid
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content[k];
      final int i = Util.toIntUnsigned(v) >>> 6;
      answer.bitmap[i] |= (1L << v);
    }
    return answer;
  }

  protected Container lazyor(BitmapContainer x) {
    BitmapContainer answer = new BitmapContainer();
    answer.cardinality = -1;// invalid
    for (int k = 0; k < this.bitmap.length; k++) {
      answer.bitmap[k] = this.bitmap[k] | x.bitmap[k];
    }
    return answer;
  }


  protected Container lazyor(RunContainer x) {
    BitmapContainer bc = clone();
    bc.cardinality = -1; // invalid
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(x.getValue(rlepos));
      int end = start + Util.toIntUnsigned(x.getLength(rlepos)) + 1;
      Util.setBitmapRange(bc.bitmap, start, end);
    }
    return bc;
  }

  @Override
  public Container limit(int maxcardinality) {
    if (maxcardinality >= this.cardinality) {
      return clone();
    }
    if (maxcardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      ArrayContainer ac = new ArrayContainer(maxcardinality);
      int pos = 0;
      for (int k = 0; (ac.cardinality < maxcardinality) && (k < bitmap.length); ++k) {
        long bitset = bitmap[k];
        while ((ac.cardinality < maxcardinality) && (bitset != 0)) {
          ac.content[pos++] = (short) (k * 64 + numberOfTrailingZeros(bitset));
          ac.cardinality++;
          bitset &= (bitset - 1);
        }
      }
      return ac;
    }
    BitmapContainer bc = new BitmapContainer(maxcardinality, this.bitmap);
    int s = Util.toIntUnsigned(select(maxcardinality));
    int usedwords = (s + 63) / 64;
    int todelete = this.bitmap.length - usedwords;
    for (int k = 0; k < todelete; ++k) {
      bc.bitmap[bc.bitmap.length - 1 - k] = 0;
    }
    int lastword = s % 64;
    if (lastword != 0) {
      bc.bitmap[s / 64] &= (0xFFFFFFFFFFFFFFFFL >>> (64 - lastword));
    }
    return bc;
  }

  protected void loadData(final ArrayContainer arrayContainer) {
    this.cardinality = arrayContainer.cardinality;
    for (int k = 0; k < arrayContainer.cardinality; ++k) {
      final short x = arrayContainer.content[k];
      bitmap[Util.toIntUnsigned(x) / 64] |= (1L << x);
    }
  }

  /**
   * Find the index of the next set bit greater or equal to i, returns -1 if none found.
   *
   * @param i starting index
   * @return index of the next set bit
   */
  public int nextSetBit(final int i) {
    int x = i >> 6; // i / 64 with sign extension
    long w = bitmap[x];
    w >>>= i;
    if (w != 0) {
      return i + numberOfTrailingZeros(w);
    }
    for (++x; x < bitmap.length; ++x) {
      if (bitmap[x] != 0) {
        return x * 64 + numberOfTrailingZeros(bitmap[x]);
      }
    }
    return -1;
  }


  @Override
  public Container not(final int firstOfRange, final int lastOfRange) {
    BitmapContainer answer = clone();
    return answer.inot(firstOfRange, lastOfRange);
  }

  @Override
  int numberOfRuns() {
    int numRuns = 0;
    long nextWord = bitmap[0];

    for (int i = 0; i < bitmap.length - 1; i++) {
      long word = nextWord;
      nextWord = bitmap[i + 1];
      numRuns += Long.bitCount((~word) & (word << 1)) + ((word >>> 63) & ~nextWord);
    }

    long word = nextWord;
    numRuns += Long.bitCount((~word) & (word << 1));
    if ((word & 0x8000000000000000L) != 0) {
      numRuns++;
    }

    return numRuns;
  }

  /**
   * Computes the number of runs
   *
   * @return the number of runs
   */
  public int numberOfRunsAdjustment() {
    int ans = 0;
    long nextWord = bitmap[0];
    for (int i = 0; i < bitmap.length - 1; i++) {
      final long word = nextWord;

      nextWord = bitmap[i + 1];
      ans += ((word >>> 63) & ~nextWord);
    }
    final long word = nextWord;

    if ((word & 0x8000000000000000L) != 0) {
      ans++;
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

    for (int blockOffset = 0; blockOffset < bitmap.length; blockOffset += BLOCKSIZE) {

      for (int i = blockOffset; i < blockOffset + BLOCKSIZE; i++) {
        long word = bitmap[i];
        numRuns += Long.bitCount((~word) & (word << 1));
      }
      if (numRuns > mustNotExceed) {
        return numRuns;
      }
    }
    return numRuns;
  }

  @Override
  public Container or(final ArrayContainer value2) {
    final BitmapContainer answer = clone();
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content[k];
      final int i = Util.toIntUnsigned(v) >>> 6;
      long w = answer.bitmap[i];
      long aft = w | (1L << v);
      answer.bitmap[i] = aft;
      if (USE_BRANCHLESS) {
        answer.cardinality += (w - aft) >>> 63;
      } else {
        if (w != aft) {
          answer.cardinality++;
        }
      }
    }
    if (answer.isFull()) {
      return RunContainer.full();
    }
    return answer;
  }

  protected boolean isFull() {
    return cardinality == MAX_CAPACITY;
  }

  @Override
  public Container or(final BitmapContainer value2) {
    BitmapContainer value1 = this.clone();
    return value1.ior(value2);
  }

  @Override
  public Container or(RunContainer x) {
    return x.or(this);
  }

  /**
   * Find the index of the previous set bit less than or equal to i, returns -1 if none found.
   *
   * @param i starting index
   * @return index of the previous set bit
   */
  public int prevSetBit(final int i) {
    int x = i >> 6; // i / 64 with sign extension
    long w = bitmap[x];
    w <<= 64 - i - 1;
    if (w != 0) {
      return i - Long.numberOfLeadingZeros(w);
    }
    for (--x; x >= 0; --x) {
      if (bitmap[x] != 0) {
        return x * 64 + 63 - Long.numberOfLeadingZeros(bitmap[x]);
      }
    }
    return -1;
  }

  @Override
  public int rank(short lowbits) {
    int x = Util.toIntUnsigned(lowbits);
    int leftover = (x + 1) & 63;
    int answer = 0;
    for (int k = 0; k < (x + 1) / 64; ++k) {
      answer += Long.bitCount(bitmap[k]);
    }
    if (leftover != 0) {
      answer += Long.bitCount(bitmap[(x + 1) / 64] << (64 - leftover));
    }
    return answer;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    deserialize(in);
  }


  @Override
  public Container remove(int begin, int end) {
    if (end == begin) {
      return clone();
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    BitmapContainer answer = clone();
    int prevOnesInRange = answer.cardinalityInRange(begin, end);
    Util.resetBitmapRange(answer.bitmap, begin, end);
    answer.updateCardinality(prevOnesInRange, 0);
    if (answer.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return answer.toArrayContainer();
    }
    return answer;
  }

  @Override
  public Container remove(final short i) {
    final int x = Util.toIntUnsigned(i);
    int index = x / 64;
    long bef = bitmap[index];
    long mask = 1L << x;
    if (cardinality == ArrayContainer.DEFAULT_MAX_SIZE + 1) {// this is
      // the
      // uncommon
      // path
      if ((bef & mask) != 0) {
        --cardinality;
        bitmap[x / 64] = bef & (~mask);
        return this.toArrayContainer();
      }
    }
    long aft = bef & (~mask);
    cardinality -= (aft - bef) >>> 63;
    bitmap[index] = aft;
    return this;
  }

  @Override
  public Container repairAfterLazy() {
    if (getCardinality() < 0) {
      computeCardinality();
      if(getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
        return this.toArrayContainer();
      } else if (isFull()) {
        return RunContainer.full();
      }
    }
    return this;
  }

  @Override
  public Container runOptimize() {
    int numRuns = numberOfRunsLowerBound(MAXRUNS); // decent choice

    int sizeAsRunContainerLowerBound = RunContainer.serializedSizeInBytes(numRuns);

    if (sizeAsRunContainerLowerBound >= getArraySizeInBytes()) {
      return this;
    }
    // else numRuns is a relatively tight bound that needs to be exact
    // in some cases (or if we need to make the runContainer the right
    // size)
    numRuns += numberOfRunsAdjustment();
    int sizeAsRunContainer = RunContainer.serializedSizeInBytes(numRuns);

    if (getArraySizeInBytes() > sizeAsRunContainer) {
      return new RunContainer(this, numRuns);
    } else {
      return this;
    }
  }

  @Override
  public short select(int j) {
    int leftover = j;
    for (int k = 0; k < bitmap.length; ++k) {
      int w = Long.bitCount(bitmap[k]);
      if (w > leftover) {
        return (short) (k * 64 + Util.select(bitmap[k], leftover));
      }
      leftover -= w;
    }
    throw new IllegalArgumentException("Insufficient cardinality.");
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    // little endian
    for (long w : bitmap) {
      out.writeLong(Long.reverseBytes(w));
    }
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
  public ArrayContainer toArrayContainer() {
    ArrayContainer ac = new ArrayContainer(cardinality);
    ac.loadData(this);
    if (ac.getCardinality() != cardinality) {
      throw new RuntimeException("Internal error.");
    }
    return ac;
  }

  /**
   * Return the content of this container as a LongBuffer. This creates a copy and might be
   * relatively slow.
   *
   * @return the LongBuffer
   */
  public LongBuffer toLongBuffer() {
    LongBuffer lb = LongBuffer.allocate(bitmap.length);
    lb.put(bitmap);
    return lb;
  }

  @Override
  public MappeableContainer toMappeableContainer() {
    return new MappeableBitmapContainer(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    final ShortIterator i = this.getShortIterator();
    sb.append("{");
    while (i.hasNext()) {
      sb.append(Util.toIntUnsigned(i.next()));
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
    serialize(out);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    serialize(out);
  }

  @Override
  public Container xor(final ArrayContainer value2) {
    final BitmapContainer answer = clone();
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short vc = value2.content[k];
      final int index = Util.toIntUnsigned(vc) >>> 6;
      final long mask = 1L << vc;
      final long val = answer.bitmap[index];
      // TODO: check whether a branchy version could be faster
      answer.cardinality += 1 - 2 * ((val & mask) >>> vc);
      answer.bitmap[index] = val ^ mask;
    }
    if (answer.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return answer.toArrayContainer();
    }
    return answer;
  }

  @Override
  public Container xor(BitmapContainer value2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] ^ value2.bitmap[k]);
    }
    if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
      final BitmapContainer answer = new BitmapContainer();
      for (int k = 0; k < answer.bitmap.length; ++k) {
        answer.bitmap[k] = this.bitmap[k] ^ value2.bitmap[k];
      }
      answer.cardinality = newCardinality;
      return answer;
    }
    ArrayContainer ac = new ArrayContainer(newCardinality);
    Util.fillArrayXOR(ac.content, this.bitmap, value2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public Container xor(RunContainer x) {
    return x.xor(this);
  }

  @Override
  public void forEach(short msb, IntConsumer ic) {
    int high = ((int) msb) << 16;
    for (int x = 0; x < bitmap.length; ++x) {
      long w = bitmap[x];
      while (w != 0) {
        ic.accept((x * 64 + numberOfTrailingZeros(w)) | high);
        w &= (w - 1);
      }
    }
  }

  @Override
  public BitmapContainer toBitmapContainer() {
    return this;
  }

  @Override
  public int nextValue(short fromValue) {
    return nextSetBit(Util.toIntUnsigned(fromValue));
  }

  @Override
  public int first() {
    assertNonEmpty(cardinality == 0);
    int i = 0;
    while(i < bitmap.length - 1 && bitmap[i] == 0) {
      ++i; // seek forward
    }
    // sizeof(long) * #empty words at start + number of bits preceding the first bit set
    return i * 64 + numberOfTrailingZeros(bitmap[i]);
  }

  @Override
  public int last() {
    assertNonEmpty(cardinality == 0);
    int i = bitmap.length - 1;
    while(i > 0 && bitmap[i] == 0) {
      --i; // seek backward
    }
    // sizeof(long) * #words from start - number of bits after the last bit set
    return (i + 1) * 64 - Long.numberOfLeadingZeros(bitmap[i]) - 1;
  }

}


final class BitmapContainerShortIterator implements PeekableShortIterator {

  long w;
  int x;

  long[] bitmap;

  BitmapContainerShortIterator() {}

  BitmapContainerShortIterator(long[] p) {
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
    return x < bitmap.length;
  }

  @Override
  public short next() {
    short answer = (short) (x * 64 + numberOfTrailingZeros(w));
    w &= (w - 1);
    while (w == 0) {
      ++x;
      if (x == bitmap.length) {
        break;
      }
      w = bitmap[x];
    }
    return answer;
  }



  @Override
  public int nextAsInt() {
    return Util.toIntUnsigned(next());
  }

  @Override
  public void remove() {
    // TODO: implement
    throw new RuntimeException("unsupported operation: remove");
  }


  public void wrap(long[] b) {
    bitmap = b;
    for (x = 0; x < bitmap.length; ++x) {
      if ((w = bitmap[x]) != 0) {
        break;
      }
    }
  }

  @Override
  public void advanceIfNeeded(short minval) {
    if (Util.toIntUnsigned(minval) >= (x + 1) * 64) {
      x = Util.toIntUnsigned(minval) / 64;
      w = bitmap[x];
      while (w == 0) {
        ++x;
        if (x == bitmap.length) {
          return;
        }
        w = bitmap[x];
      }
    }
    while (hasNext() && (Util.toIntUnsigned(peekNext()) < Util.toIntUnsigned(minval))) {
      next(); // could be optimized
    }
  }

  @Override
  public short peekNext() {
    return (short) (x * 64 + numberOfTrailingZeros(w));
  }
}


final class ReverseBitmapContainerShortIterator implements ShortIterator {

  long word;
  int position;

  long[] bitmap;

  ReverseBitmapContainerShortIterator() {}

  ReverseBitmapContainerShortIterator(long[] bitmap) {
    wrap(bitmap);
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
    return position >= 0;
  }

  @Override
  public short next() {
    int shift = Long.numberOfLeadingZeros(word) + 1;
    short answer = (short)((position + 1) * 64 - shift);
    word &= ~(1L << (64 - shift));
    while (word == 0) {
      --position;
      if (position < 0) {
        break;
      }
      word = bitmap[position];
    }
    return answer;
  }

  @Override
  public int nextAsInt() {
    return Util.toIntUnsigned(next());
  }

  @Override
  public void remove() {
    // TODO: implement
    throw new RuntimeException("unsupported operation: remove");
  }

  void wrap(long[] b) {
    bitmap = b;
    for (position = bitmap.length - 1; position >= 0; --position) {
      if ((word = bitmap[position]) != 0) {
        break;
      }
    }
  }
}
