package org.roaringbitmap;

import java.util.Arrays;

/**
 * This extends {@link RoaringBitmap} to provide better performance for .rank and .select
 * operations, at the cost of maintain a cache of cardinalities.
 * 
 * On {@link RoaringBitmap#select(int)} and {@link RoaringBitmap#rank(int)} operations,
 * {@link RoaringBitmap} needs to iterate along all underlying buckets to cumulate their
 * cardinalities. This may lead to sub-optimal performance for application doing a large amount of
 * .rank/.select over read-only {@link RoaringBitmap}, especially if the {@link RoaringBitmap} holds
 * a large number of underlying buckets.
 * 
 * This implementation will discard the cache of cardinality on any write operations, and it will
 * memoize the computed cardinalities on any .rank or .select operation
 * 
 * @author Benoit Lacelle
 *
 */
public class FastRankRoaringBitmap extends RoaringBitmap {
  // The cache of cardinalities: it maps the index of the underlying bucket to the cumulated
  // cardinalities (i.e. the sum of current bucket cardinalities plus all previous bucklet
  // cardinalities)
  private boolean cumulatedCardinalitiesCacheIsValid = false;
  private int[] highToCumulatedCardinality = null;

  public FastRankRoaringBitmap() {
    super();
  }

  public FastRankRoaringBitmap(RoaringArray array) {
    super(array);
  }

  private void resetCache() {
    // Reset the cache on any write operation
    if (highToCumulatedCardinality != null && highToCumulatedCardinality.length >= 1) {
      // We tag the first bucket to indicate the cache is dismissed
      cumulatedCardinalitiesCacheIsValid = false;
    }
  }

  // VisibleForTesting
  boolean isCacheDismissed() {
    return !cumulatedCardinalitiesCacheIsValid;
  }

  @Override
  public void add(long rangeStart, long rangeEnd) {
    resetCache();
    super.add(rangeStart, rangeEnd);
  }

  @Override
  public void add(int x) {
    resetCache();
    super.add(x);
  }

  @Override
  public void add(int... dat) {
    resetCache();
    super.add(dat);
  }

  @Deprecated
  @Override
  public void add(int rangeStart, int rangeEnd) {
    resetCache();
    super.add(rangeStart, rangeEnd);
  }

  @Override
  public void clear() {
    resetCache();
    super.clear();
  }

  @Override
  public void flip(int x) {
    resetCache();
    super.flip(x);
  }

  @Deprecated
  @Override
  public void flip(int rangeStart, int rangeEnd) {
    resetCache();
    super.flip(rangeStart, rangeEnd);
  }

  @Override
  public void flip(long rangeStart, long rangeEnd) {
    resetCache();
    super.flip(rangeStart, rangeEnd);
  }

  @Override
  public void and(RoaringBitmap x2) {
    resetCache();
    super.and(x2);
  }

  @Override
  public void andNot(RoaringBitmap x2) {
    resetCache();
    super.andNot(x2);
  }

  @Deprecated
  @Override
  public void remove(int rangeStart, int rangeEnd) {
    resetCache();
    super.remove(rangeStart, rangeEnd);
  }

  @Override
  public void remove(int x) {
    resetCache();
    super.remove(x);
  }

  @Override
  public void remove(long rangeStart, long rangeEnd) {
    resetCache();
    super.remove(rangeStart, rangeEnd);
  }

  @Override
  public boolean checkedAdd(int x) {
    resetCache();
    return super.checkedAdd(x);
  }

  @Override
  public boolean checkedRemove(int x) {
    resetCache();
    return super.checkedRemove(x);
  }

  @Override
  public void or(RoaringBitmap x2) {
    resetCache();
    super.or(x2);
  }

  @Override
  public void xor(RoaringBitmap x2) {
    resetCache();
    super.xor(x2);
  }

  /**
   * On any .rank or .select operation, we pre-compute all cumulated cardinalities. It will enable
   * using a binary-search to spot the relevant underlying bucket. We may prefer to cache
   * cardinality only up-to the selected rank, but it would lead to a more complex implementation
   */
  private void preComputeCardinalities() {
    if (!cumulatedCardinalitiesCacheIsValid) {
      int nbBuckets = highLowContainer.size();

      // Ensure the cache size is the right one
      if (highToCumulatedCardinality == null || highToCumulatedCardinality.length != nbBuckets) {
        highToCumulatedCardinality = new int[nbBuckets];
      }

      // Ensure the cache content is valid
      if (highToCumulatedCardinality.length >= 1) {
        // As we compute the cumulated cardinalities, the first index is an edge case
        highToCumulatedCardinality[0] = highLowContainer.getContainerAtIndex(0).getCardinality();

        for (int i = 1; i < highToCumulatedCardinality.length; i++) {
          highToCumulatedCardinality[i] = highToCumulatedCardinality[i - 1]
              + highLowContainer.getContainerAtIndex(i).getCardinality();
        }
      }

      cumulatedCardinalitiesCacheIsValid = true;
    }
  }

  @Override
  public long rankLong(int x) {
    preComputeCardinalities();

    if (highLowContainer.size() == 0) {
      return 0L;
    }

    char xhigh = Util.highbits(x);

    int index = Util.hybridUnsignedBinarySearch(this.highLowContainer.keys, 0,
        this.highLowContainer.size(), xhigh);

    boolean hasBitmapOnIdex;
    if (index < 0) {
      hasBitmapOnIdex = false;
      index = -1 - index;
    } else {
      hasBitmapOnIdex = true;
    }

    long size = 0;
    if (index > 0) {
      size += highToCumulatedCardinality[index - 1];
    }

    long rank = size;
    if (hasBitmapOnIdex) {
      rank = size + this.highLowContainer.getContainerAtIndex(index).rank(Util.lowbits(x));
    }

    return rank;
  }

  @Override
  public int select(int j) {
    preComputeCardinalities();

    if (highLowContainer.size() == 0) {
      // empty: .select is out-of-bounds

      throw new IllegalArgumentException(
          "select " + j + " when the cardinality is " + this.getCardinality());
    }

    int index = Arrays.binarySearch(highToCumulatedCardinality, j);

    int fixedIndex;

    long leftover = Util.toUnsignedLong(j);

    if (index == highToCumulatedCardinality.length - 1) {
      // We select the total cardinality: we are selecting the last element
      return this.last();
    } else if (index >= 0) {
      // We selected a cumulated cardinality: we are selecting the last element of given bucket
      int keycontrib = this.highLowContainer.getKeyAtIndex(index + 1) << 16;

      // If first bucket has cardinality 1 and we select 1: we actual select the first item of
      // second bucket
      int output = keycontrib + this.highLowContainer.getContainerAtIndex(index + 1).first();

      return output;
    } else {
      // We selected a cardinality not matching exactly the cumulated cardinalities: we are not
      // selected the last element of a bucket
      fixedIndex = -1 - index;
      if (fixedIndex > 0) {
        leftover -= highToCumulatedCardinality[fixedIndex - 1];
      }
    }

    int keycontrib = this.highLowContainer.getKeyAtIndex(fixedIndex) << 16;
    int lowcontrib = (
        this.highLowContainer.getContainerAtIndex(fixedIndex).select((int) leftover));
    int value = lowcontrib + keycontrib;


    return value;
  }
  
  @Override
  public long getLongSizeInBytes() {
    long size = 8;
    size += super.getLongSizeInBytes();
    if (highToCumulatedCardinality != null) {
      size += 4L * highToCumulatedCardinality.length;
    }
    return size;
  }

  /**
   * Get a special iterator that allows .peekNextRank efficiently
   *
   * @return iterator with fast rank access
   */
  public PeekableIntRankIterator getIntRankIterator() {
    preComputeCardinalities();
    return new FastRoaringIntRankIterator();
  }

  private class FastRoaringIntRankIterator implements PeekableIntRankIterator {
    private int hs = 0;

    private PeekableCharRankIterator iter;

    private int pos = 0;

    private FastRoaringIntRankIterator() {
      nextContainer();
    }

    @Override
    public int peekNextRank() {
      int iterRank = iter.peekNextRank();
      if (pos > 0) {
        return FastRankRoaringBitmap.this.highToCumulatedCardinality[pos - 1] + iterRank;
      } else {
        return iterRank;
      }
    }

    @Override
    public PeekableIntRankIterator clone() {
      try {
        FastRoaringIntRankIterator x =
            (FastRoaringIntRankIterator) super.clone();
        if (this.iter != null) {
          x.iter = this.iter.clone();
        }
        return x;
      } catch (CloneNotSupportedException e) {
        return null;// will not happen
      }
    }

    @Override
    public boolean hasNext() {
      return pos < FastRankRoaringBitmap.this.highLowContainer.size();
    }

    @Override
    public int next() {
      final int x = iter.nextAsInt() | hs;
      if (!iter.hasNext()) {
        ++pos;
        nextContainer();
      }
      return x;
    }

    private void nextContainer() {
      if (pos < FastRankRoaringBitmap.this.highLowContainer.size()) {
        iter = FastRankRoaringBitmap.this.highLowContainer.getContainerAtIndex(pos)
                                                          .getCharRankIterator();
        hs = FastRankRoaringBitmap.this.highLowContainer.getKeyAtIndex(pos) << 16;
      }
    }

    @Override
    public void advanceIfNeeded(int minval) {
      while (hasNext() && ((hs >>> 16) < (minval >>> 16))) {
        ++pos;
        nextContainer();
      }
      if (hasNext() && ((hs >>> 16) == (minval >>> 16))) {
        iter.advanceIfNeeded(Util.lowbits(minval));
        if (!iter.hasNext()) {
          ++pos;
          nextContainer();
        }
      }
    }

    @Override
    public int peekNext() {
      return (iter.peekNext()) | hs;
    }
  }
}
