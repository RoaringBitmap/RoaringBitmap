/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import static java.lang.Long.bitCount;
import static java.lang.Long.numberOfTrailingZeros;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.roaringbitmap.buffer.MappeableBitmapContainer;
import org.roaringbitmap.buffer.MappeableContainer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Simple bitset-like container.
 */
public final class BitmapContainer extends Container implements Cloneable {
  public static final int MAX_CAPACITY = 1 << 16;

  private static final int MAX_CAPACITY_BYTE = MAX_CAPACITY / Byte.SIZE;

  private static final int MAX_CAPACITY_LONG = MAX_CAPACITY / Long.SIZE;

  private static final long serialVersionUID = 3L;

  // bail out early when the number of runs is excessive, without
  // an exact count (just a decent lower bound)
  private static final int BLOCKSIZE = 128;

  // 64 words can have max 32 runs per word, max 2k runs

  /**
   * optimization flag: whether the cardinality of the bitmaps is maintained through branchless
   * operations
   */
  private static final boolean USE_BRANCHLESS = true;

  private static final VectorSpecies<Long> LONG_VECTOR_SPECIES = LongVector.SPECIES_PREFERRED;
  private static final int VECTOR_LANE_COUNT = LONG_VECTOR_SPECIES.length();
  private static final int VECTOR_THRESHOLD_FACTOR = 64;
  // Baseline lane count (4 lanes = 256-bit) for scaling density thresholds.
  private static final int VECTOR_LANE_BASELINE = 4;
  // Heuristics to avoid vectorizing when most words are empty.
  private static final int VECTOR_SPARSE_CARDINALITY =
      (int) Math.min(
          MAX_CAPACITY,
          (long) (MAX_CAPACITY / 4) * VECTOR_LANE_COUNT / VECTOR_LANE_BASELINE);
  private static final int VECTOR_DENSE_CARDINALITY =
      (int) Math.min(
          MAX_CAPACITY,
          (long) (MAX_CAPACITY / 2) * VECTOR_LANE_COUNT / VECTOR_LANE_BASELINE);
  private static final int VECTOR_WORD_SAMPLE_STRIDE = 16;
  private static final int VECTOR_WORD_SAMPLE_NONZERO_RATIO = 4;
  private static final int VECTOR_MIN_CARDINALITY = VECTOR_LANE_COUNT * VECTOR_THRESHOLD_FACTOR;

  /**
   * Return a bitmap iterator over this array
   *
   * @param bitmap array to be iterated over
   * @return an iterator
   */
  public static CharIterator getReverseShortIterator(long[] bitmap) {
    return new ReverseBitmapContainerCharIterator(bitmap);
  }

  /**
   * Return a bitmap iterator over this array
   *
   * @param bitmap array to be iterated over
   * @return an iterator
   */
  public static PeekableCharIterator getShortIterator(long[] bitmap) {
    return new BitmapContainerCharIterator(bitmap);
  }

  // the parameter is for overloading and symmetry with ArrayContainer
  protected static int serializedSizeInBytes(int unusedCardinality) {
    return MAX_CAPACITY / 8;
  }

  final long[] bitmap;

  int cardinality;

  // nruns value for which RunContainer.serializedSizeInBytes ==
  // BitmapContainer.getArraySizeInBytes()
  private static final int MAXRUNS = (MAX_CAPACITY_BYTE - 2) / 4;

  /**
   * Create a bitmap container with all bits set to false
   */
  public BitmapContainer() {
    this.cardinality = 0;
    this.bitmap = new long[MAX_CAPACITY_LONG];
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
  public Container add(final char i) {
    final long previous = bitmap[i >>> 6];
    long newval = previous | (1L << i);
    bitmap[i >>> 6] = newval;
    if (USE_BRANCHLESS) {
      cardinality += (int) ((previous ^ newval) >>> i);
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
      char v = value2.content[k];
      answer.content[answer.cardinality] = v;
      answer.cardinality += (int) this.bitValue(v);
    }
    return answer;
  }

  @Override
  public Container and(final BitmapContainer value2) {
    int newCardinality = andCardinality(value2);
    if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
      final BitmapContainer answer = new BitmapContainer();
      vectorAnd(this.bitmap, value2.bitmap, answer.bitmap);
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
      char v = value2.content[k];
      answer += (int) this.bitValue(v);
    }
    return answer;
  }

  @Override
  public int andCardinality(final BitmapContainer value2) {
    return andCardinality(this.bitmap, this.cardinality, value2.bitmap, value2.cardinality);
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
      char v = value2.content[k];
      final int i = (v) >>> 6;
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
      vectorAndNot(this.bitmap, value2.bitmap, answer.bitmap);
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
      int start = (x.getValue(rlepos));
      int end = start + (x.getLength(rlepos)) + 1;
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
  public Boolean validate() {
    if (cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return false;
    }
    int computed_cardinality = vectorCardinality(this.bitmap);
    return cardinality == computed_cardinality;
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
  void computeCardinality() {
    this.cardinality = vectorCardinality(this.bitmap);
  }

  private static int vectorCardinality(long[] bitmap) {
    int length = bitmap.length;
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int upperBound = LONG_VECTOR_SPECIES.loopBound(length);
    int unroll = laneCount * 4;
    int unrolledBound = upperBound - (upperBound % unroll);
    LongVector acc0 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc1 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc2 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc3 = LongVector.zero(LONG_VECTOR_SPECIES);
    // Unroll to keep multiple accumulators and reduce once at the end.
    for (; i < unrolledBound; i += unroll) {
      LongVector v0 = LongVector.fromArray(LONG_VECTOR_SPECIES, bitmap, i);
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, bitmap, i + laneCount);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, bitmap, i + 2 * laneCount);
      LongVector v3 = LongVector.fromArray(LONG_VECTOR_SPECIES, bitmap, i + 3 * laneCount);
      acc0 = acc0.add(v0.lanewise(VectorOperators.BIT_COUNT));
      acc1 = acc1.add(v1.lanewise(VectorOperators.BIT_COUNT));
      acc2 = acc2.add(v2.lanewise(VectorOperators.BIT_COUNT));
      acc3 = acc3.add(v3.lanewise(VectorOperators.BIT_COUNT));
    }
    LongVector acc = acc0.add(acc1).add(acc2).add(acc3);
    for (; i < upperBound; i += laneCount) {
      LongVector v = LongVector.fromArray(LONG_VECTOR_SPECIES, bitmap, i);
      acc = acc.add(v.lanewise(VectorOperators.BIT_COUNT));
    }
    long total = acc.reduceLanes(VectorOperators.ADD);
    for (; i < length; ++i) {
      total += Long.bitCount(bitmap[i]);
    }
    return (int) total;
  }

  int cardinalityInRange(int start, int end) {
    if (cardinality != -1 && end - start > MAX_CAPACITY / 2) {
      int before = Util.cardinalityInBitmapRange(bitmap, 0, start);
      int after = Util.cardinalityInBitmapRange(bitmap, end, MAX_CAPACITY);
      return cardinality - before - after;
    }
    return Util.cardinalityInBitmapRange(bitmap, start, end);
  }

  void updateCardinality(int prevOnes, int newOnes) {
    int oldCardinality = this.cardinality;
    this.cardinality = oldCardinality - prevOnes + newOnes;
  }

  private static boolean useVectorAnd(
      int leftCardinality, int rightCardinality, long[] left, long[] right) {
    if (leftCardinality >= 0 && rightCardinality >= 0) {
      int minCardinality = Math.min(leftCardinality, rightCardinality);
      if (minCardinality < VECTOR_MIN_CARDINALITY) {
        return false;
      }
      if (minCardinality <= VECTOR_SPARSE_CARDINALITY) {
        return false;
      }
    }
    return sampledNonZeroAnd(left, right);
  }

  private static boolean useVectorOr(
      int leftCardinality, int rightCardinality, long[] left, long[] right) {
    if (leftCardinality >= 0 && rightCardinality >= 0) {
      int maxCardinality = Math.max(leftCardinality, rightCardinality);
      int minCardinality = Math.min(leftCardinality, rightCardinality);
      if (minCardinality < VECTOR_MIN_CARDINALITY) {
        return false;
      }
      if (maxCardinality >= VECTOR_DENSE_CARDINALITY) {
        return true;
      }
      if (maxCardinality <= VECTOR_SPARSE_CARDINALITY) {
        return false;
      }
    }
    return sampledNonZeroOr(left, right);
  }

  private static boolean useVectorXor(
      int leftCardinality, int rightCardinality, long[] left, long[] right) {
    if (leftCardinality >= 0 && rightCardinality >= 0) {
      int maxCardinality = Math.max(leftCardinality, rightCardinality);
      int minCardinality = Math.min(leftCardinality, rightCardinality);
      if (minCardinality < VECTOR_MIN_CARDINALITY) {
        return false;
      }
      if (maxCardinality <= VECTOR_SPARSE_CARDINALITY) {
        return false;
      }
    }
    return sampledNonZeroXor(left, right);
  }

  private static boolean sampledNonZeroAnd(long[] left, long[] right) {
    int length = Math.min(left.length, right.length);
    if (length == 0) {
      return true;
    }
    int stride = Math.min(VECTOR_WORD_SAMPLE_STRIDE, length);
    int nonZero = 0;
    int samples = 0;
    for (int i = 0; i < length; i += stride) {
      if ((left[i] & right[i]) != 0L) {
        ++nonZero;
      }
      ++samples;
    }
    return nonZero * VECTOR_WORD_SAMPLE_NONZERO_RATIO >= samples;
  }

  private static boolean sampledNonZeroOr(long[] left, long[] right) {
    int length = Math.min(left.length, right.length);
    if (length == 0) {
      return true;
    }
    int stride = Math.min(VECTOR_WORD_SAMPLE_STRIDE, length);
    int nonZero = 0;
    int samples = 0;
    for (int i = 0; i < length; i += stride) {
      if ((left[i] | right[i]) != 0L) {
        ++nonZero;
      }
      ++samples;
    }
    return nonZero * VECTOR_WORD_SAMPLE_NONZERO_RATIO >= samples;
  }

  private static boolean sampledNonZeroXor(long[] left, long[] right) {
    int length = Math.min(left.length, right.length);
    if (length == 0) {
      return true;
    }
    int stride = Math.min(VECTOR_WORD_SAMPLE_STRIDE, length);
    int nonZero = 0;
    int samples = 0;
    for (int i = 0; i < length; i += stride) {
      if ((left[i] ^ right[i]) != 0L) {
        ++nonZero;
      }
      ++samples;
    }
    return nonZero * VECTOR_WORD_SAMPLE_NONZERO_RATIO >= samples;
  }

  private boolean vectorContains(long[] superset, long[] subset) {
    int length = subset.length;
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int upperBound = LONG_VECTOR_SPECIES.loopBound(length);
    for (; i < upperBound; i += laneCount) {
      LongVector vSuper = LongVector.fromArray(LONG_VECTOR_SPECIES, superset, i);
      LongVector vSub = LongVector.fromArray(LONG_VECTOR_SPECIES, subset, i);
      if (!vSub.and(vSuper.not()).test(VectorOperators.IS_DEFAULT).allTrue()) {
        return false;
      }
    }
    for (; i < length; ++i) {
      if ((superset[i] & subset[i]) != subset[i]) {
        return false;
      }
    }
    return true;
  }

  private static void vectorAnd(long[] left, long[] right, long[] out) {
    int length = Math.min(Math.min(left.length, right.length), out.length);
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int vectorBound = LONG_VECTOR_SPECIES.loopBound(length);
    int unroll = laneCount * 4;
    int unrolledBound = vectorBound - (vectorBound % unroll);
    boolean skipZeroStores = out != left && out != right;
    for (; i < unrolledBound; i += unroll) {
      LongVector left0 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector left1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + laneCount);
      LongVector left2 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 2 * laneCount);
      LongVector left3 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 3 * laneCount);
      LongVector right0 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector right1 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + laneCount);
      LongVector right2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 2 * laneCount);
      LongVector right3 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 3 * laneCount);
      LongVector v0 = left0.and(right0);
      LongVector v1 = left1.and(right1);
      LongVector v2 = left2.and(right2);
      LongVector v3 = left3.and(right3);
      if (!skipZeroStores || !v0.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v0.intoArray(out, i);
      }
      if (!skipZeroStores || !v1.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v1.intoArray(out, i + laneCount);
      }
      if (!skipZeroStores || !v2.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v2.intoArray(out, i + 2 * laneCount);
      }
      if (!skipZeroStores || !v3.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v3.intoArray(out, i + 3 * laneCount);
      }
    }
    for (; i < vectorBound; i += laneCount) {
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector v = v1.and(v2);
      if (!skipZeroStores || !v.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v.intoArray(out, i);
      }
    }
    if (i < length) {
      VectorMask<Long> mask = LONG_VECTOR_SPECIES.indexInRange(i, length);
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i, mask);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i, mask);
      LongVector v = v1.and(v2);
      if (!skipZeroStores || !v.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v.intoArray(out, i, mask);
      }
    }
  }

  private static void vectorAndNot(long[] left, long[] right, long[] out) {
    int length = Math.min(Math.min(left.length, right.length), out.length);
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int vectorBound = LONG_VECTOR_SPECIES.loopBound(length);
    int unroll = laneCount * 4;
    int unrolledBound = vectorBound - (vectorBound % unroll);
    for (; i < unrolledBound; i += unroll) {
      LongVector left0 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector left1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + laneCount);
      LongVector left2 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 2 * laneCount);
      LongVector left3 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 3 * laneCount);
      LongVector right0 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector right1 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + laneCount);
      LongVector right2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 2 * laneCount);
      LongVector right3 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 3 * laneCount);
      left0.and(right0.not()).intoArray(out, i);
      left1.and(right1.not()).intoArray(out, i + laneCount);
      left2.and(right2.not()).intoArray(out, i + 2 * laneCount);
      left3.and(right3.not()).intoArray(out, i + 3 * laneCount);
    }
    for (; i < vectorBound; i += laneCount) {
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      v1.and(v2.not()).intoArray(out, i);
    }
    if (i < length) {
      VectorMask<Long> mask = LONG_VECTOR_SPECIES.indexInRange(i, length);
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i, mask);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i, mask);
      v1.and(v2.not()).intoArray(out, i, mask);
    }
  }

  private static int scalarAndCardinality(long[] left, long[] right) {
    int length = Math.min(left.length, right.length);
    int cardinality = 0;
    for (int i = 0; i < length; ++i) {
      long word = left[i] & right[i];
      if (word != 0L) {
        cardinality += Long.bitCount(word);
      }
    }
    return (int) cardinality;
  }

  private static int scalarOrIntoCardinality(long[] left, long[] right, long[] out) {
    int length = Math.min(Math.min(left.length, right.length), out.length);
    int cardinality = 0;
    boolean inPlace = out == left;
    for (int i = 0; i < length; ++i) {
      long word = left[i] | right[i];
      if (!inPlace || word != left[i]) {
        out[i] = word;
      }
      if (word != 0L) {
        cardinality += Long.bitCount(word);
      }
    }
    return cardinality;
  }

  private static int scalarXorCardinality(long[] left, long[] right) {
    int length = Math.min(left.length, right.length);
    int cardinality = 0;
    for (int i = 0; i < length; ++i) {
      long word = left[i] ^ right[i];
      if (word != 0L) {
        cardinality += Long.bitCount(word);
      }
    }
    return cardinality;
  }

  private static int scalarXorIntoCardinality(long[] left, long[] right, long[] out) {
    int length = Math.min(Math.min(left.length, right.length), out.length);
    int cardinality = 0;
    boolean skipZeroStores = out != left && out != right;
    for (int i = 0; i < length; ++i) {
      long word = left[i] ^ right[i];
      if (!skipZeroStores || word != 0L) {
        out[i] = word;
      }
      if (word != 0L) {
        cardinality += Long.bitCount(word);
      }
    }
    return cardinality;
  }

  private static void vectorOr(long[] left, long[] right, long[] out) {
    int length = Math.min(Math.min(left.length, right.length), out.length);
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int vectorBound = LONG_VECTOR_SPECIES.loopBound(length);
    int unroll = laneCount * 4;
    int unrolledBound = vectorBound - (vectorBound % unroll);
    boolean inPlace = out == left;
    for (; i < unrolledBound; i += unroll) {
      LongVector left0 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector left1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + laneCount);
      LongVector left2 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 2 * laneCount);
      LongVector left3 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 3 * laneCount);
      LongVector right0 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector right1 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + laneCount);
      LongVector right2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 2 * laneCount);
      LongVector right3 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 3 * laneCount);
      if (right0.test(VectorOperators.IS_DEFAULT).allTrue()) {
        if (!inPlace) {
          left0.intoArray(out, i);
        }
      } else {
        left0.or(right0).intoArray(out, i);
      }
      if (right1.test(VectorOperators.IS_DEFAULT).allTrue()) {
        if (!inPlace) {
          left1.intoArray(out, i + laneCount);
        }
      } else {
        left1.or(right1).intoArray(out, i + laneCount);
      }
      if (right2.test(VectorOperators.IS_DEFAULT).allTrue()) {
        if (!inPlace) {
          left2.intoArray(out, i + 2 * laneCount);
        }
      } else {
        left2.or(right2).intoArray(out, i + 2 * laneCount);
      }
      if (right3.test(VectorOperators.IS_DEFAULT).allTrue()) {
        if (!inPlace) {
          left3.intoArray(out, i + 3 * laneCount);
        }
      } else {
        left3.or(right3).intoArray(out, i + 3 * laneCount);
      }
    }
    for (; i < vectorBound; i += laneCount) {
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      if (v2.test(VectorOperators.IS_DEFAULT).allTrue()) {
        if (!inPlace) {
          v1.intoArray(out, i);
        }
      } else {
        v1.or(v2).intoArray(out, i);
      }
    }
    if (i < length) {
      VectorMask<Long> mask = LONG_VECTOR_SPECIES.indexInRange(i, length);
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i, mask);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i, mask);
      if (v2.test(VectorOperators.IS_DEFAULT).allTrue()) {
        if (!inPlace) {
          v1.intoArray(out, i, mask);
        }
      } else {
        v1.or(v2).intoArray(out, i, mask);
      }
    }
  }

  private static void vectorXor(long[] left, long[] right, long[] out) {
    int length = Math.min(Math.min(left.length, right.length), out.length);
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int vectorBound = LONG_VECTOR_SPECIES.loopBound(length);
    int unroll = laneCount * 4;
    int unrolledBound = vectorBound - (vectorBound % unroll);
    for (; i < unrolledBound; i += unroll) {
      LongVector left0 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector left1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + laneCount);
      LongVector left2 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 2 * laneCount);
      LongVector left3 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 3 * laneCount);
      LongVector right0 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector right1 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + laneCount);
      LongVector right2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 2 * laneCount);
      LongVector right3 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 3 * laneCount);
      left0.lanewise(VectorOperators.XOR, right0).intoArray(out, i);
      left1.lanewise(VectorOperators.XOR, right1).intoArray(out, i + laneCount);
      left2.lanewise(VectorOperators.XOR, right2).intoArray(out, i + 2 * laneCount);
      left3.lanewise(VectorOperators.XOR, right3).intoArray(out, i + 3 * laneCount);
    }
    for (; i < vectorBound; i += laneCount) {
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      v1.lanewise(VectorOperators.XOR, v2).intoArray(out, i);
    }
    if (i < length) {
      VectorMask<Long> mask = LONG_VECTOR_SPECIES.indexInRange(i, length);
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i, mask);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i, mask);
      v1.lanewise(VectorOperators.XOR, v2).intoArray(out, i, mask);
    }
  }

  private static int vectorOrIntoCardinality(long[] left, long[] right, long[] out) {
    int length = Math.min(Math.min(left.length, right.length), out.length);
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int upperBound = LONG_VECTOR_SPECIES.loopBound(length);
    int unroll = laneCount * 4;
    int unrolledBound = upperBound - (upperBound % unroll);
    boolean inPlace = out == left;
    LongVector acc0 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc1 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc2 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc3 = LongVector.zero(LONG_VECTOR_SPECIES);
    for (; i < unrolledBound; i += unroll) {
      LongVector left0 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector left1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + laneCount);
      LongVector left2 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 2 * laneCount);
      LongVector left3 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 3 * laneCount);
      LongVector right0 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector right1 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + laneCount);
      LongVector right2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 2 * laneCount);
      LongVector right3 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 3 * laneCount);
      LongVector v0;
      LongVector v1;
      LongVector v2;
      LongVector v3;
      if (right0.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v0 = left0;
        if (!inPlace) {
          v0.intoArray(out, i);
        }
      } else {
        v0 = left0.or(right0);
        if (!inPlace || v0.compare(VectorOperators.NE, left0).anyTrue()) {
          v0.intoArray(out, i);
        }
      }
      if (right1.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v1 = left1;
        if (!inPlace) {
          v1.intoArray(out, i + laneCount);
        }
      } else {
        v1 = left1.or(right1);
        if (!inPlace || v1.compare(VectorOperators.NE, left1).anyTrue()) {
          v1.intoArray(out, i + laneCount);
        }
      }
      if (right2.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v2 = left2;
        if (!inPlace) {
          v2.intoArray(out, i + 2 * laneCount);
        }
      } else {
        v2 = left2.or(right2);
        if (!inPlace || v2.compare(VectorOperators.NE, left2).anyTrue()) {
          v2.intoArray(out, i + 2 * laneCount);
        }
      }
      if (right3.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v3 = left3;
        if (!inPlace) {
          v3.intoArray(out, i + 3 * laneCount);
        }
      } else {
        v3 = left3.or(right3);
        if (!inPlace || v3.compare(VectorOperators.NE, left3).anyTrue()) {
          v3.intoArray(out, i + 3 * laneCount);
        }
      }
      acc0 = acc0.add(v0.lanewise(VectorOperators.BIT_COUNT));
      acc1 = acc1.add(v1.lanewise(VectorOperators.BIT_COUNT));
      acc2 = acc2.add(v2.lanewise(VectorOperators.BIT_COUNT));
      acc3 = acc3.add(v3.lanewise(VectorOperators.BIT_COUNT));
    }
    LongVector acc = acc0.add(acc1).add(acc2).add(acc3);
    for (; i < upperBound; i += laneCount) {
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector v;
      if (v2.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v = v1;
        if (!inPlace) {
          v.intoArray(out, i);
        }
      } else {
        v = v1.or(v2);
        if (!inPlace || v.compare(VectorOperators.NE, v1).anyTrue()) {
          v.intoArray(out, i);
        }
      }
      acc = acc.add(v.lanewise(VectorOperators.BIT_COUNT));
    }
    if (i < length) {
      VectorMask<Long> mask = LONG_VECTOR_SPECIES.indexInRange(i, length);
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i, mask);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i, mask);
      LongVector v;
      if (v2.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v = v1;
        if (!inPlace) {
          v.intoArray(out, i, mask);
        }
      } else {
        v = v1.or(v2);
        if (!inPlace || v.compare(VectorOperators.NE, v1).anyTrue()) {
          v.intoArray(out, i, mask);
        }
      }
      acc = acc.add(v.lanewise(VectorOperators.BIT_COUNT));
    }
    long cardinality = acc.reduceLanes(VectorOperators.ADD);
    return (int) cardinality;
  }

  private static int vectorXorIntoCardinality(long[] left, long[] right, long[] out) {
    int length = Math.min(Math.min(left.length, right.length), out.length);
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int upperBound = LONG_VECTOR_SPECIES.loopBound(length);
    int unroll = laneCount * 4;
    int unrolledBound = upperBound - (upperBound % unroll);
    boolean skipZeroStores = out != left && out != right;
    LongVector acc0 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc1 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc2 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc3 = LongVector.zero(LONG_VECTOR_SPECIES);
    for (; i < unrolledBound; i += unroll) {
      LongVector left0 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector left1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + laneCount);
      LongVector left2 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 2 * laneCount);
      LongVector left3 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 3 * laneCount);
      LongVector right0 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector right1 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + laneCount);
      LongVector right2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 2 * laneCount);
      LongVector right3 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 3 * laneCount);
      LongVector v0 = left0.lanewise(VectorOperators.XOR, right0);
      LongVector v1 = left1.lanewise(VectorOperators.XOR, right1);
      LongVector v2 = left2.lanewise(VectorOperators.XOR, right2);
      LongVector v3 = left3.lanewise(VectorOperators.XOR, right3);
      if (!skipZeroStores || !v0.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v0.intoArray(out, i);
      }
      if (!skipZeroStores || !v1.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v1.intoArray(out, i + laneCount);
      }
      if (!skipZeroStores || !v2.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v2.intoArray(out, i + 2 * laneCount);
      }
      if (!skipZeroStores || !v3.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v3.intoArray(out, i + 3 * laneCount);
      }
      acc0 = acc0.add(v0.lanewise(VectorOperators.BIT_COUNT));
      acc1 = acc1.add(v1.lanewise(VectorOperators.BIT_COUNT));
      acc2 = acc2.add(v2.lanewise(VectorOperators.BIT_COUNT));
      acc3 = acc3.add(v3.lanewise(VectorOperators.BIT_COUNT));
    }
    LongVector acc = acc0.add(acc1).add(acc2).add(acc3);
    for (; i < upperBound; i += laneCount) {
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector v = v1.lanewise(VectorOperators.XOR, v2);
      if (!skipZeroStores || !v.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v.intoArray(out, i);
      }
      acc = acc.add(v.lanewise(VectorOperators.BIT_COUNT));
    }
    if (i < length) {
      VectorMask<Long> mask = LONG_VECTOR_SPECIES.indexInRange(i, length);
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i, mask);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i, mask);
      LongVector v = v1.lanewise(VectorOperators.XOR, v2);
      if (!skipZeroStores || !v.test(VectorOperators.IS_DEFAULT).allTrue()) {
        v.intoArray(out, i, mask);
      }
      acc = acc.add(v.lanewise(VectorOperators.BIT_COUNT));
    }
    long cardinality = acc.reduceLanes(VectorOperators.ADD);
    return (int) cardinality;
  }

  private static int andCardinality(
      long[] left, int leftCardinality, long[] right, int rightCardinality) {
    if (leftCardinality >= 0
        && rightCardinality >= 0
        && (leftCardinality < VECTOR_MIN_CARDINALITY
            || rightCardinality < VECTOR_MIN_CARDINALITY)) {
      return scalarAndCardinality(left, right);
    }
    if (useVectorAnd(leftCardinality, rightCardinality, left, right)) {
      return vectorAndCardinality(left, right);
    }
    return scalarAndCardinality(left, right);
  }

  private static int orInto(
      long[] left, int leftCardinality, long[] right, int rightCardinality, long[] out) {
    if (leftCardinality >= 0
        && rightCardinality >= 0
        && (leftCardinality < VECTOR_MIN_CARDINALITY
            || rightCardinality < VECTOR_MIN_CARDINALITY)) {
      return scalarOrIntoCardinality(left, right, out);
    }
    if (useVectorOr(leftCardinality, rightCardinality, left, right)) {
      return vectorOrIntoCardinality(left, right, out);
    }
    return scalarOrIntoCardinality(left, right, out);
  }

  private static int xorCardinality(
      long[] left, int leftCardinality, long[] right, int rightCardinality) {
    if (leftCardinality >= 0
        && rightCardinality >= 0
        && (leftCardinality < VECTOR_MIN_CARDINALITY
            || rightCardinality < VECTOR_MIN_CARDINALITY)) {
      return scalarXorCardinality(left, right);
    }
    if (useVectorXor(leftCardinality, rightCardinality, left, right)) {
      return vectorXorCardinality(left, right);
    }
    return scalarXorCardinality(left, right);
  }

  private static int xorInto(
      long[] left, int leftCardinality, long[] right, int rightCardinality, long[] out) {
    if (leftCardinality >= 0
        && rightCardinality >= 0
        && (leftCardinality < VECTOR_MIN_CARDINALITY
            || rightCardinality < VECTOR_MIN_CARDINALITY)) {
      return scalarXorIntoCardinality(left, right, out);
    }
    if (useVectorXor(leftCardinality, rightCardinality, left, right)) {
      return vectorXorIntoCardinality(left, right, out);
    }
    return scalarXorIntoCardinality(left, right, out);
  }

  private static int vectorAndCardinality(long[] left, long[] right) {
    int length = Math.min(left.length, right.length);
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int upperBound = LONG_VECTOR_SPECIES.loopBound(length);
    int unroll = laneCount * 4;
    int unrolledBound = upperBound - (upperBound % unroll);
    LongVector acc0 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc1 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc2 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc3 = LongVector.zero(LONG_VECTOR_SPECIES);
    for (; i < unrolledBound; i += unroll) {
      LongVector left0 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector left1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + laneCount);
      LongVector left2 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 2 * laneCount);
      LongVector left3 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 3 * laneCount);
      LongVector right0 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector right1 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + laneCount);
      LongVector right2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 2 * laneCount);
      LongVector right3 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 3 * laneCount);
      acc0 = acc0.add(left0.and(right0).lanewise(VectorOperators.BIT_COUNT));
      acc1 = acc1.add(left1.and(right1).lanewise(VectorOperators.BIT_COUNT));
      acc2 = acc2.add(left2.and(right2).lanewise(VectorOperators.BIT_COUNT));
      acc3 = acc3.add(left3.and(right3).lanewise(VectorOperators.BIT_COUNT));
    }
    LongVector acc = acc0.add(acc1).add(acc2).add(acc3);
    for (; i < upperBound; i += laneCount) {
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      acc = acc.add(v1.and(v2).lanewise(VectorOperators.BIT_COUNT));
    }
    long cardinality = acc.reduceLanes(VectorOperators.ADD);
    for (; i < length; ++i) {
      cardinality += Long.bitCount(left[i] & right[i]);
    }
    return (int) cardinality;
  }

  private static int vectorXorCardinality(long[] left, long[] right) {
    int length = Math.min(left.length, right.length);
    int i = 0;
    int laneCount = LONG_VECTOR_SPECIES.length();
    int upperBound = LONG_VECTOR_SPECIES.loopBound(length);
    int unroll = laneCount * 4;
    int unrolledBound = upperBound - (upperBound % unroll);
    LongVector acc0 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc1 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc2 = LongVector.zero(LONG_VECTOR_SPECIES);
    LongVector acc3 = LongVector.zero(LONG_VECTOR_SPECIES);
    for (; i < unrolledBound; i += unroll) {
      LongVector left0 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector left1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + laneCount);
      LongVector left2 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 2 * laneCount);
      LongVector left3 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i + 3 * laneCount);
      LongVector right0 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      LongVector right1 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + laneCount);
      LongVector right2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 2 * laneCount);
      LongVector right3 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i + 3 * laneCount);
      acc0 =
          acc0.add(
              left0.lanewise(VectorOperators.XOR, right0)
                  .lanewise(VectorOperators.BIT_COUNT));
      acc1 =
          acc1.add(
              left1.lanewise(VectorOperators.XOR, right1)
                  .lanewise(VectorOperators.BIT_COUNT));
      acc2 =
          acc2.add(
              left2.lanewise(VectorOperators.XOR, right2)
                  .lanewise(VectorOperators.BIT_COUNT));
      acc3 =
          acc3.add(
              left3.lanewise(VectorOperators.XOR, right3)
                  .lanewise(VectorOperators.BIT_COUNT));
    }
    LongVector acc = acc0.add(acc1).add(acc2).add(acc3);
    for (; i < upperBound; i += laneCount) {
      LongVector v1 = LongVector.fromArray(LONG_VECTOR_SPECIES, left, i);
      LongVector v2 = LongVector.fromArray(LONG_VECTOR_SPECIES, right, i);
      acc =
          acc.add(
              v1.lanewise(VectorOperators.XOR, v2).lanewise(VectorOperators.BIT_COUNT));
    }
    long cardinality = acc.reduceLanes(VectorOperators.ADD);
    for (; i < length; ++i) {
      cardinality += Long.bitCount(left[i] ^ right[i]);
    }
    return (int) cardinality;
  }

  @Override
  public boolean contains(final char i) {
    return (bitmap[i >>> 6] & (1L << i)) != 0;
  }

  @Override
  public boolean contains(int minimum, int supremum) {
    int start = minimum >>> 6;
    int end = supremum >>> 6;
    long first = -(1L << minimum);
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
    if ((cardinality != -1) && (bitmapContainer.cardinality != -1)) {
      if (cardinality < bitmapContainer.cardinality) {
        return false;
      }
    }
    return vectorContains(this.bitmap, bitmapContainer.bitmap);
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
    if (arrayContainer.cardinality != -1) {
      if (cardinality < arrayContainer.cardinality) {
        return false;
      }
    }
    for (int i = 0; i < arrayContainer.cardinality; ++i) {
      if (!contains(arrayContainer.content[i])) {
        return false;
      }
    }
    return true;
  }

  int bitValue(final char i) {
    return (int) (bitmap[i >>> 6] >>> i) & 1;
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
  void fillArray(final char[] array) {
    BitSetUtil.arrayContainerBufferOf(0, bitmap.length, array, bitmap);
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
  public Container flip(char i) {
    int index = i >>> 6;
    long bef = bitmap[index];
    long mask = 1L << i;
    if (cardinality == ArrayContainer.DEFAULT_MAX_SIZE + 1) { // this is
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
    cardinality += 1 - 2 * (int) ((bef & mask) >>> i);
    bitmap[index] ^= mask;
    return this;
  }

  @Override
  public int getArraySizeInBytes() {
    return MAX_CAPACITY_BYTE;
  }

  @Override
  public int getCardinality() {
    return cardinality;
  }

  @Override
  public PeekableCharIterator getReverseCharIterator() {
    return new ReverseBitmapContainerCharIterator(this.bitmap);
  }

  @Override
  public PeekableCharIterator getCharIterator() {
    return new BitmapContainerCharIterator(this.bitmap);
  }

  @Override
  public PeekableCharRankIterator getCharRankIterator() {
    return new BitmapContainerCharRankIterator(this.bitmap);
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
    if (-1 == cardinality) {
      // actually we can avoid allocating in lazy mode
      Util.intersectArrayIntoBitmap(bitmap, b2.content, b2.cardinality);
      return this;
    } else {
      return b2.and(this);
    }
  }

  @Override
  public Container iand(final BitmapContainer b2) {
    if (-1 == cardinality) {
      // in lazy mode, just intersect the bitmaps, can repair afterwards
      vectorAnd(bitmap, b2.bitmap, bitmap);
      return this;
    } else {
      int newCardinality = andCardinality(b2);
      if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
        vectorAnd(this.bitmap, b2.bitmap, this.bitmap);
        this.cardinality = newCardinality;
        return this;
      }
      ArrayContainer ac = new ArrayContainer(newCardinality);
      Util.fillArrayAND(ac.content, this.bitmap, b2.bitmap);
      ac.cardinality = newCardinality;
      return ac;
    }
  }

  @Override
  public Container iand(RunContainer x) {
    // could probably be replaced with return iand(x.toBitmapOrArrayContainer());
    final int card = x.getCardinality();
    if (-1 != cardinality && card <= ArrayContainer.DEFAULT_MAX_SIZE) {
      // no point in doing it in-place, unless it's a lazy operation
      ArrayContainer answer = new ArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
        int runStart = (x.getValue(rlepos));
        int runEnd = runStart + (x.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          answer.content[answer.cardinality] = (char) runValue;
          answer.cardinality += (int) this.bitValue((char) runValue);
        }
      }
      return answer;
    }
    int start = 0;
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int end = x.getValue(rlepos);
      if (-1 == cardinality) {
        Util.resetBitmapRange(this.bitmap, start, end);
      } else {
        int prevOnes = cardinalityInRange(start, end);
        Util.resetBitmapRange(this.bitmap, start, end);
        updateCardinality(prevOnes, 0);
      }
      start = end + x.getLength(rlepos) + 1;
    }
    if (-1 == cardinality) {
      // in lazy mode don't try to trim
      Util.resetBitmapRange(this.bitmap, start, MAX_CAPACITY);
    } else {
      int ones = cardinalityInRange(start, MAX_CAPACITY);
      Util.resetBitmapRange(this.bitmap, start, MAX_CAPACITY);
      updateCardinality(ones, 0);
      if (getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
        return toArrayContainer();
      }
    }
    return this;
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
      vectorAndNot(this.bitmap, b2.bitmap, this.bitmap);
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
      int start = (x.getValue(rlepos));
      int end = start + (x.getLength(rlepos)) + 1;
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

  Container ilazyor(ArrayContainer value2) {
    this.cardinality = -1; // invalid
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      char v = value2.content[k];
      final int i = (v) >>> 6;
      this.bitmap[i] |= (1L << v);
    }
    return this;
  }

  Container ilazyor(BitmapContainer x) {
    this.cardinality = -1; // invalid
    vectorOr(this.bitmap, x.bitmap, this.bitmap);
    return this;
  }

  Container ilazyor(RunContainer x) {
    // could be implemented as return ilazyor(x.toTemporaryBitmap());
    cardinality = -1; // invalid
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = (x.getValue(rlepos));
      int end = start + (x.getLength(rlepos)) + 1;
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
    if ((minimum < 0) || (supremum < minimum) || (supremum > (1 << 16))) {
      throw new RuntimeException("This should never happen (bug).");
    }
    int start = minimum >>> 6;
    int end = supremum >>> 6;
    if (start == end) {
      return ((bitmap[end] & (-(1L << minimum) & ((1L << supremum) - 1))) != 0);
    }
    if ((bitmap[start] & -(1L << minimum)) != 0) {
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
      final int i = (value2.content[k]) >>> 6;

      long bef = this.bitmap[i];
      long aft = bef | (1L << value2.content[k]);
      this.bitmap[i] = aft;
      if (USE_BRANCHLESS) {
        cardinality += (int) ((bef - aft) >>> 63);
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
    this.cardinality =
        orInto(this.bitmap, this.cardinality, b2.bitmap, b2.cardinality, this.bitmap);
    if (isFull()) {
      return RunContainer.full();
    }
    return this;
  }

  @Override
  public Container ior(RunContainer x) {
    // could probably be replaced with return ior(x.toBitmapOrArrayContainer());
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = (x.getValue(rlepos));
      int end = start + (x.getLength(rlepos)) + 1;
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
  public Iterator<Character> iterator() {
    return new Iterator<Character>() {
      final CharIterator si = BitmapContainer.this.getCharIterator();

      @Override
      public boolean hasNext() {
        return si.hasNext();
      }

      @Override
      public Character next() {
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
      char vc = value2.content[k];
      long mask = 1L << vc;
      final int index = (vc) >>> 6;
      long ba = this.bitmap[index];
      // TODO: check whether a branchy version could be faster
      this.cardinality += 1 - 2 * (int) ((ba & mask) >>> vc);
      this.bitmap[index] = ba ^ mask;
    }
    if (this.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return this.toArrayContainer();
    }
    return this;
  }

  @Override
  public Container ixor(BitmapContainer b2) {
    this.cardinality =
        xorInto(this.bitmap, this.cardinality, b2.bitmap, b2.cardinality, this.bitmap);
    if (cardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
      return this;
    }
    return toArrayContainer();
  }

  @Override
  public Container ixor(RunContainer x) {
    // could probably be replaced with return ixor(x.toBitmapOrArrayContainer());
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = x.getValue(rlepos);
      int end = start + x.getLength(rlepos) + 1;
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
    answer.cardinality = -1; // invalid
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      char v = value2.content[k];
      final int i = (v) >>> 6;
      answer.bitmap[i] |= (1L << v);
    }
    return answer;
  }

  protected Container lazyor(BitmapContainer x) {
    BitmapContainer answer = new BitmapContainer();
    answer.cardinality = -1; // invalid
    vectorOr(this.bitmap, x.bitmap, answer.bitmap);
    return answer;
  }

  protected Container lazyor(RunContainer x) {
    BitmapContainer bc = clone();
    bc.cardinality = -1; // invalid
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = (x.getValue(rlepos));
      int end = start + (x.getLength(rlepos)) + 1;
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
          ac.content[pos++] = (char) (k * 64 + numberOfTrailingZeros(bitset));
          ac.cardinality++;
          bitset &= (bitset - 1);
        }
      }
      return ac;
    }
    long[] newBitmap = new long[MAX_CAPACITY / 64];
    BitmapContainer bc = new BitmapContainer(newBitmap, maxcardinality);
    int s = (select(maxcardinality));
    int usedwords = (s + 63) / 64;
    System.arraycopy(this.bitmap, 0, newBitmap, 0, usedwords);
    int lastword = s % 64;
    if (lastword != 0) {
      bc.bitmap[s / 64] &= (0xFFFFFFFFFFFFFFFFL >>> (64 - lastword));
    }
    return bc;
  }

  void loadData(final ArrayContainer arrayContainer) {
    this.cardinality = arrayContainer.cardinality;
    Util.fillBitmapFromSortedArray(arrayContainer.content, arrayContainer.cardinality, bitmap, 0);
  }

  /**
   * Find the index of the next set bit greater or equal to i, returns -1 if none found.
   *
   * @param i starting index
   * @return index of the next set bit
   */
  public int nextSetBit(final int i) {
    int x = i >> 6;
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

  /**
   * Find the index of the next clear bit greater or equal to i.
   *
   * @param i starting index
   * @return index of the next clear bit
   */
  private int nextClearBit(final int i) {
    int x = i >> 6;
    long w = ~bitmap[x];
    w >>>= i;
    if (w != 0) {
      return i + numberOfTrailingZeros(w);
    }
    for (++x; x < bitmap.length; ++x) {
      long map = ~bitmap[x];
      if (map != 0) {
        return x * 64 + numberOfTrailingZeros(map);
      }
    }
    return MAX_CAPACITY;
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
      numRuns += Long.bitCount((~word) & (word << 1)) + (int) ((word >>> 63) & ~nextWord);
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
      ans += (int) ((word >>> 63) & ~nextWord);
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

    for (int blockOffset = 0; blockOffset + BLOCKSIZE <= bitmap.length; blockOffset += BLOCKSIZE) {

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
      char v = value2.content[k];
      final int i = (v) >>> 6;
      long w = answer.bitmap[i];
      long aft = w | (1L << v);
      answer.bitmap[i] = aft;
      if (USE_BRANCHLESS) {
        answer.cardinality += (int) ((w - aft) >>> 63);
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

  @Override
  public boolean isFull() {
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
  int prevSetBit(final int i) {
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

  /**
   * Find the index of the previous clear bit less than or equal to i.
   *
   * @param i starting index
   * @return index of the previous clear bit
   */
  private int prevClearBit(final int i) {
    int x = i >> 6; // i / 64 with sign extension
    long w = ~bitmap[x];
    w <<= 64 - (i + 1);
    if (w != 0) {
      return i - Long.numberOfLeadingZeros(w);
    }
    for (--x; x >= 0; --x) {
      long map = ~bitmap[x];
      if (map != 0) {
        return x * 64 + 63 - Long.numberOfLeadingZeros(map);
      }
    }
    return -1;
  }

  @Override
  public int rank(char lowbits) {
    int leftover = (lowbits + 1) & 63;
    int answer = 0;
    for (int k = 0; k < (lowbits + 1) >>> 6; ++k) {
      answer += Long.bitCount(bitmap[k]);
    }
    if (leftover != 0) {
      answer += Long.bitCount(bitmap[(lowbits + 1) >>> 6] << (64 - leftover));
    }
    return answer;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
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
  public Container remove(final char i) {
    int index = i >>> 6;
    long bef = bitmap[index];
    long mask = 1L << i;
    if (cardinality == ArrayContainer.DEFAULT_MAX_SIZE + 1) { // this is
      // the
      // uncommon
      // path
      if ((bef & mask) != 0) {
        --cardinality;
        bitmap[i >>> 6] = bef & ~mask;
        return this.toArrayContainer();
      }
    }
    long aft = bef & ~mask;
    cardinality -= (aft - bef) >>> 63;
    bitmap[index] = aft;
    return this;
  }

  @Override
  public Container repairAfterLazy() {
    if (getCardinality() < 0) {
      computeCardinality();
      if (getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
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
  public char select(int j) {
    if ( // cardinality != -1 && // omitted as (-1>>>1) > j as j < (1<<16)
    cardinality >>> 1 < j && j < cardinality) {
      int leftover = cardinality - j;
      for (int k = bitmap.length - 1; k >= 0; --k) {
        long w = bitmap[k];
        if (w != 0) {
          int bits = Long.bitCount(w);
          if (bits >= leftover) {
            return (char) (k * 64 + Util.select(w, bits - leftover));
          }
          leftover -= bits;
        }
      }
    } else {
      int leftover = j;
      for (int k = 0; k < bitmap.length; ++k) {
        long w = bitmap[k];
        if (w != 0) {
          int bits = Long.bitCount(bitmap[k]);
          if (bits > leftover) {
            return (char) (k * 64 + Util.select(bitmap[k], leftover));
          }
          leftover -= bits;
        }
      }
    }
    throw new IllegalArgumentException("Insufficient cardinality.");
  }

  /** TODO For comparison only, should be removed before merge.
   *
   * @param j ...
   * @return ...
   */
  public char selectOneSide(int j) {
    int leftover = j;
    for (int k = 0; k < bitmap.length; ++k) {
      int w = Long.bitCount(bitmap[k]);
      if (w > leftover) {
        return (char) (k * 64 + Util.select(bitmap[k], leftover));
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
  ArrayContainer toArrayContainer() {
    ArrayContainer ac = new ArrayContainer(cardinality);
    if (cardinality != 0) {
      ac.loadData(this);
    }
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
    StringBuilder sb = new StringBuilder("{}".length() + "-123456789,".length() * 256);
    final CharIterator i = this.getCharIterator();
    sb.append('{');
    while (i.hasNext()) {
      sb.append((int) (i.next()));
      if (i.hasNext()) {
        sb.append(',');
      }
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public void trim() {}

  @Override
  public void writeArray(DataOutput out) throws IOException {
    serialize(out);
  }

  @Override
  public void writeArray(ByteBuffer buffer) {
    assert buffer.order() == ByteOrder.LITTLE_ENDIAN;
    LongBuffer buf = buffer.asLongBuffer();
    buf.put(bitmap);
    int bytesWritten = bitmap.length * 8;
    buffer.position(buffer.position() + bytesWritten);
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
      char vc = value2.content[k];
      final int index = vc >>> 6;
      final long mask = 1L << vc;
      final long val = answer.bitmap[index];
      // TODO: check whether a branchy version could be faster
      answer.cardinality += (int) (1 - 2 * ((val & mask) >>> vc));
      answer.bitmap[index] = val ^ mask;
    }
    if (answer.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      return answer.toArrayContainer();
    }
    return answer;
  }

  @Override
  public Container xor(BitmapContainer value2) {
    int newCardinality =
        xorCardinality(this.bitmap, this.cardinality, value2.bitmap, value2.cardinality);
    if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
      final BitmapContainer answer = new BitmapContainer();
      vectorXor(this.bitmap, value2.bitmap, answer.bitmap);
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
  public void forEach(char msb, IntConsumer ic) {
    int high = msb << 16;
    for (int x = 0; x < bitmap.length; ++x) {
      long w = bitmap[x];
      while (w != 0) {
        ic.accept(((x << 6) + numberOfTrailingZeros(w)) | high);
        w &= (w - 1);
      }
    }
  }

  @Override
  public void forAll(int offset, final RelativeRangeConsumer rrc) {
    for (int wordIndex = 0; wordIndex < bitmap.length; wordIndex++) {
      long word = bitmap[wordIndex];
      int bufferWordStart = offset + (wordIndex << 6);
      int bufferWordEnd = bufferWordStart + 64;
      addWholeWordToRangeConsumer(word, bufferWordStart, bufferWordEnd, rrc);
    }
  }

  @Override
  public void forAllFrom(char startValue, final RelativeRangeConsumer rrc) {
    int startIndex = startValue >>> 6;
    for (int wordIndex = startIndex; wordIndex < bitmap.length; wordIndex++) {
      long word = bitmap[wordIndex];
      int wordStart = wordIndex << 6;
      int wordEnd = wordStart + 64;
      if (wordStart < startValue) {
        // startValue is in the middle of the word
        // some special cases for efficiency
        if (word == 0) {
          rrc.acceptAllAbsent(0, wordEnd - startValue);
        } else if (word == -1) { // all 1s
          rrc.acceptAllPresent(0, wordEnd - startValue);
        } else {
          int nextPos = startValue;
          while (word != 0) {
            int pos = wordStart + numberOfTrailingZeros(word);
            if (nextPos < pos) {
              rrc.acceptAllAbsent(nextPos - startValue, pos - startValue);
              rrc.acceptPresent(pos - startValue);
              nextPos = pos + 1;
            } else if (nextPos == pos) {
              rrc.acceptPresent(pos - startValue);
              nextPos++;
            } // else just we out before startValue, so ignore
            word &= (word - 1);
          }
          if (nextPos < wordEnd) {
            rrc.acceptAllAbsent(nextPos - startValue, wordEnd - startValue);
          }
        }
      } else {
        // startValue is aligned with word
        addWholeWordToRangeConsumer(word, wordStart - startValue, wordEnd - startValue, rrc);
      }
    }
  }

  @Override
  public void forAllUntil(int offset, char endValue, final RelativeRangeConsumer rrc) {
    int bufferEndPos = offset + endValue;
    for (int wordIndex = 0; wordIndex < bitmap.length; wordIndex++) {
      long word = bitmap[wordIndex];
      int bufferWordStart = offset + (wordIndex << 6);
      int bufferWordEnd = bufferWordStart + 64;
      if (bufferWordStart >= bufferEndPos) {
        return;
      }
      if (bufferEndPos < bufferWordEnd) {
        // we end on this word

        // some special cases for efficiency
        if (word == 0) {
          rrc.acceptAllAbsent(bufferWordStart, bufferEndPos);
        } else if (word == -1) { // all 1s
          rrc.acceptAllPresent(bufferWordStart, bufferEndPos);
        } else {
          int nextPos = bufferWordStart;
          while (word != 0) {
            int pos = bufferWordStart + numberOfTrailingZeros(word);
            if (bufferEndPos <= pos) {
              // we've moved past the end
              if (nextPos < bufferEndPos) {
                rrc.acceptAllAbsent(nextPos, bufferEndPos);
              }
              return;
            }
            if (nextPos < pos) {
              rrc.acceptAllAbsent(nextPos, pos);
              nextPos = pos;
            }
            rrc.acceptPresent(pos);
            nextPos++;
            word &= (word - 1);
          }
          if (nextPos < bufferEndPos) {
            rrc.acceptAllAbsent(nextPos, bufferEndPos);
          }
          return;
        }
      } else {
        addWholeWordToRangeConsumer(word, bufferWordStart, bufferWordEnd, rrc);
      }
    }
  }

  @Override
  public void forAllInRange(char startValue, char endValue, final RelativeRangeConsumer rrc) {
    if (endValue <= startValue) {
      throw new IllegalArgumentException(
          "startValue (" + startValue + ") must be less than endValue (" + endValue + ")");
    }
    int startIndex = startValue >>> 6;
    for (int wordIndex = startIndex; wordIndex < bitmap.length; wordIndex++) {
      long word = bitmap[wordIndex];
      int wordStart = wordIndex << 6;
      int wordEndExclusive = wordStart + 64;

      if (wordStart >= endValue) {
        return;
      }

      boolean startInWord = wordStart < startValue;
      boolean endInWord = endValue < wordEndExclusive;
      boolean wordAllZeroes = word == 0;
      boolean wordAllOnes = word == -1;

      if (startInWord && endInWord) {
        if (wordAllZeroes) {
          rrc.acceptAllAbsent(0, endValue - startValue);
        } else if (wordAllOnes) {
          rrc.acceptAllPresent(0, endValue - startValue);
        } else {
          int nextPos = startValue;
          while (word != 0) {
            int pos = wordStart + numberOfTrailingZeros(word);
            if (endValue <= pos) {
              // we've moved past the end
              if (nextPos < endValue) {
                rrc.acceptAllAbsent(nextPos - startValue, endValue - startValue);
              }
              return;
            }
            if (nextPos < pos) {
              rrc.acceptAllAbsent(nextPos - startValue, pos - startValue);
              rrc.acceptPresent(pos - startValue);
              nextPos = pos + 1;
            } else if (nextPos == pos) {
              rrc.acceptPresent(pos - startValue);
              nextPos++;
            }
            word &= (word - 1);
          }
          if (nextPos < endValue) {
            rrc.acceptAllAbsent(nextPos - startValue, endValue - startValue);
          }
        }
        return;
      } else if (startInWord) {
        if (wordAllZeroes) {
          rrc.acceptAllAbsent(0, 64 - (startValue - wordStart));
        } else if (wordAllOnes) {
          rrc.acceptAllPresent(0, 64 - (startValue - wordStart));
        } else {
          int nextPos = startValue;
          while (word != 0) {
            int pos = wordStart + numberOfTrailingZeros(word);
            if (nextPos < pos) {
              rrc.acceptAllAbsent(nextPos - startValue, pos - startValue);
              rrc.acceptPresent(pos - startValue);
              nextPos = pos + 1;
            } else if (nextPos == pos) {
              rrc.acceptPresent(pos - startValue);
              nextPos++;
            }
            word &= (word - 1);
          }
          if (nextPos < wordEndExclusive) {
            rrc.acceptAllAbsent(nextPos - startValue, wordEndExclusive - startValue);
          }
        }
      } else if (endInWord) {
        if (wordAllZeroes) {
          rrc.acceptAllAbsent(wordStart - startValue, endValue - startValue);
        } else if (wordAllOnes) {
          rrc.acceptAllPresent(wordStart - startValue, endValue - startValue);
        } else {
          int nextPos = wordStart;
          while (word != 0) {
            int pos = wordStart + numberOfTrailingZeros(word);
            if (endValue <= pos) {
              // we've moved past the end
              if (nextPos < endValue) {
                rrc.acceptAllAbsent(nextPos - startValue, endValue - startValue);
              }
              return;
            }
            if (nextPos < pos) {
              rrc.acceptAllAbsent(nextPos - startValue, pos - startValue);
              nextPos = pos;
            }
            rrc.acceptPresent(pos - startValue);
            nextPos++;
            word &= (word - 1);
          }
          if (nextPos < endValue) {
            rrc.acceptAllAbsent(nextPos - startValue, endValue - startValue);
          }
        }
        return;
      } else {
        addWholeWordToRangeConsumer(
            word, wordStart - startValue, wordEndExclusive - startValue, rrc);
      }
    }
  }

  private void addWholeWordToRangeConsumer(
      long word, int bufferWordStart, int bufferWordEnd, final RelativeRangeConsumer rrc) {
    // some special cases for efficiency
    if (word == 0) {
      rrc.acceptAllAbsent(bufferWordStart, bufferWordEnd);
    } else if (word == -1) { // all 1s
      rrc.acceptAllPresent(bufferWordStart, bufferWordEnd);
    } else {
      int nextPos = bufferWordStart;
      while (word != 0) {
        int pos = bufferWordStart + numberOfTrailingZeros(word);
        if (nextPos < pos) {
          rrc.acceptAllAbsent(nextPos, pos);
          nextPos = pos;
        }
        rrc.acceptPresent(pos);
        nextPos++;
        word &= (word - 1);
      }
      if (nextPos < bufferWordEnd) {
        rrc.acceptAllAbsent(nextPos, bufferWordEnd);
      }
    }
  }

  @Override
  public BitmapContainer toBitmapContainer() {
    return this;
  }

  @Override
  public void copyBitmapTo(long[] words, int position) {
    System.arraycopy(bitmap, 0, words, position, bitmap.length);
  }

  public void copyBitmapTo(long[] words, int position, int length) {
    System.arraycopy(bitmap, 0, words, position, length);
  }

  @Override
  public int nextValue(char fromValue) {
    return nextSetBit((fromValue));
  }

  @Override
  public int previousValue(char fromValue) {
    return prevSetBit((fromValue));
  }

  @Override
  public int nextAbsentValue(char fromValue) {
    return nextClearBit((fromValue));
  }

  @Override
  public int previousAbsentValue(char fromValue) {
    return prevClearBit((fromValue));
  }

  @Override
  public int first() {
    assertNonEmpty(cardinality == 0);
    int i = 0;
    while (i < bitmap.length - 1 && bitmap[i] == 0) {
      ++i; // seek forward
    }
    // sizeof(long) * #empty words at start + number of bits preceding the first bit set
    return i * 64 + numberOfTrailingZeros(bitmap[i]);
  }

  @Override
  public int last() {
    assertNonEmpty(cardinality == 0);
    int i = bitmap.length - 1;
    while (i > 0 && bitmap[i] == 0) {
      --i; // seek backward
    }
    // sizeof(long) * #words from start - number of bits after the last bit set
    return (i + 1) * 64 - Long.numberOfLeadingZeros(bitmap[i]) - 1;
  }
}

class BitmapContainerCharIterator implements PeekableCharIterator {

  long w;
  int x;

  long[] bitmap;

  BitmapContainerCharIterator() {}

  BitmapContainerCharIterator(long[] p) {
    wrap(p);
  }

  @Override
  public PeekableCharIterator clone() {
    try {
      return (PeekableCharIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null; // will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return x < bitmap.length;
  }

  @Override
  public char next() {
    char answer = (char) (x * 64 + numberOfTrailingZeros(w));
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
    return (next());
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
  public void advanceIfNeeded(char minval) {
    if (!hasNext()) {
      return;
    }
    if (minval >= x * 64) {
      if (minval >= (x + 1) * 64) {
        x = minval / 64;
        w = bitmap[x];
      }
      w &= ~0L << (minval & 63);
      while (w == 0) {
        x++;
        if (!hasNext()) {
          return;
        }
        w = bitmap[x];
      }
    }
  }

  @Override
  public char peekNext() {
    return (char) (x * 64 + numberOfTrailingZeros(w));
  }
}

final class BitmapContainerCharRankIterator extends BitmapContainerCharIterator
    implements PeekableCharRankIterator {
  private int nextRank = 1;

  BitmapContainerCharRankIterator(long[] p) {
    super(p);
  }

  @Override
  public int peekNextRank() {
    return nextRank;
  }

  @Override
  public char next() {
    ++nextRank;
    return super.next();
  }

  @Override
  public void advanceIfNeeded(char minval) {
    if (!hasNext()) {
      return;
    }
    if (minval >= x * 64) {
      if (minval >= (x + 1) * 64) {
        int nextX = minval / 64;
        nextRank += bitCount(w);
        for (x = x + 1; x < nextX; x++) {
          w = bitmap[x];
          nextRank += bitCount(w);
        }
        w = bitmap[nextX];
      }
      nextRank += bitCount(w);
      w &= ~0L << (minval & 63);
      nextRank -= bitCount(w);
      while (w == 0) {
        ++x;
        if (!hasNext()) {
          return;
        }
        w = bitmap[x];
      }
    }
  }

  @Override
  public PeekableCharRankIterator clone() {
    return (PeekableCharRankIterator) super.clone();
  }
}

final class ReverseBitmapContainerCharIterator implements PeekableCharIterator {

  long word;
  int position;

  long[] bitmap;

  ReverseBitmapContainerCharIterator() {}

  ReverseBitmapContainerCharIterator(long[] bitmap) {
    wrap(bitmap);
  }

  @Override
  public PeekableCharIterator clone() {
    try {
      return (PeekableCharIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    return position >= 0;
  }

  @Override
  public char next() {
    int shift = Long.numberOfLeadingZeros(word) + 1;
    char answer = (char) ((position + 1) * 64 - shift);
    word &= (0xFFFFFFFFFFFFFFFEL >>> shift);
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
    return next();
  }

  @Override
  public void advanceIfNeeded(char maxval) {
    if (maxval < (position + 1) * 64) {
      if (maxval < position * 64) {
        position = maxval / 64;
      }
      long currentWord = bitmap[position];
      currentWord &= ~0L >>> (63 - (maxval & 63));
      if (position > 0) {
        while (currentWord == 0) {
          position--;
          if (position == 0) {
            break;
          }
          currentWord = bitmap[position];
        }
      }
      word = currentWord;
    }
  }

  @Override
  public char peekNext() {
    int shift = Long.numberOfLeadingZeros(word) + 1;
    return (char) ((position + 1) * 64 - shift);
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
