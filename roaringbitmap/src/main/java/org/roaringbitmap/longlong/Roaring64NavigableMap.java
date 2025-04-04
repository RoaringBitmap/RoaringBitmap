/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.longlong;

import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.BitmapDataProviderSupplier;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapPrivate;
import org.roaringbitmap.RoaringBitmapSupplier;
import org.roaringbitmap.Util;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmapPrivate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Roaring64NavigableMap extends RoaringBitmap to the whole range of longs (or unsigned longs). It
 * enables a greater cardinality, up to 2*Long.MAX_VALUE-1
 *
 * Longs are added by default in unsigned sorted order (i.e. -1L is the greatest long to be added
 * while 0 has no previous value). It can be configured to signed sorted order (in which case, 0 is
 * preceded by -1). That is, they are treated as unsigned integers (see Java 8's
 * Integer.toUnsignedLong function). Up to 4294967296 integers can be stored.
 *
 *
 *
 */
// this class is not thread-safe
// @Beta: this class is still in early stage. Its API may change and has not proofed itself as
// bug-proof
public class Roaring64NavigableMap implements Externalizable, LongBitmapDataProvider {
  // The initial format of Roaring64NavigableMap
  // 1: a boolean indicating if we process signed or unsigned longs
  // 2: an int indicating the umber of highToLow buckets
  // 3: loop over buckets: an int representating the high part,
  // then the lowParts as standard RoaringBitmaps format
  public static final int SERIALIZATION_MODE_LEGACY = 0;

  // The format of Roaring64NavigableMap as defined in
  // https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations
  // It is compatible with CRoaring and GoRoaring
  // https://github.com/RoaringBitmap/CRoaring/blob/master/cpp/roaring64map.hh#L959
  // https://github.com/RoaringBitmap/CRoaring/blob/master/cpp/roaring64map.hh#L991
  // https://github.com/RoaringBitmap/CRoaring/commit/efcb83dcdf332f02cde058a48574f5b7b14f73fb
  // 1: a boolean indicating if we process signed or unsigned longs
  // 2: an int indicating the umber of highToLow buckets
  // 3: loop over buckets: an int representating the high part,
  // then the lowParts as standard RoaringBitmaps format
  public static final int SERIALIZATION_MODE_PORTABLE = 1;

  // As of RoaringBitmap 0.X, we stick to the legacy format for retrocompatibility
  // RoaringBitmap 1.X may switch to the portable format by default
  public static int SERIALIZATION_MODE = SERIALIZATION_MODE_LEGACY;

  // Not final to enable initialization in Externalizable.readObject
  private NavigableMap<Integer, BitmapDataProvider> highToBitmap;

  // If true, we handle longs a plain java longs: -1 if right before 0
  // If false, we handle longs as unsigned longs: 0 has no predecessor and Long.MAX_VALUE + 1L is
  // expressed as a negative long
  private boolean signedLongs = false;

  private transient BitmapDataProviderSupplier supplier;

  // By default, we cache cardinalities
  private transient boolean doCacheCardinalities = true;

  // Prevent recomputing all cardinalities when requesting consecutive ranks
  private transient int firstHighNotValid = highestHigh() + 1;

  // This boolean needs firstHighNotValid == Integer.MAX_VALUE to be allowed to be true
  // If false, it means nearly all cumulated cardinalities are valid, except high=Integer.MAX_VALUE
  // If true, it means all cumulated cardinalities are valid, even high=Integer.MAX_VALUE
  private transient boolean allValid = false;

  // TODO: I would prefer not managing arrays myself
  private transient long[] sortedCumulatedCardinality = new long[0];
  private transient int[] sortedHighs = new int[0];

  // We guess consecutive .addLong will be on proximate longs: we remember the bitmap attached to
  // this bucket in order to skip the indirection
  private transient Map.Entry<Integer, BitmapDataProvider> latestAddedHigh = null;

  private static final boolean DEFAULT_ORDER_IS_SIGNED = false;
  private static final boolean DEFAULT_CARDINALITIES_ARE_CACHED = true;

  /**
   * By default, we consider longs are unsigned longs: normal longs: 0 is the lowest possible long.
   * Long.MAX_VALUE is followed by Long.MIN_VALUE. -1L is the highest possible value
   */
  public Roaring64NavigableMap() {
    this(DEFAULT_ORDER_IS_SIGNED);
  }

  /**
   *
   * By default, use RoaringBitmap as underlyings {@link BitmapDataProvider}
   *
   * @param signedLongs true if longs has to be ordered as plain java longs. False to handle them as
   *        unsigned 64bits long (as RoaringBitmap with unsigned integers)
   */
  public Roaring64NavigableMap(boolean signedLongs) {
    this(signedLongs, DEFAULT_CARDINALITIES_ARE_CACHED);
  }

  /**
   * By default, use RoaringBitmap as underlyings {@link BitmapDataProvider}
   *
   * @param signedLongs true if longs has to be ordered as plain java longs. False to handle them as
   *        unsigned 64bits long (as RoaringBitmap with unsigned integers)
   * @param cacheCardinalities true if cardinalities have to be cached. It will prevent many
   *        iteration along the NavigableMap
   */
  public Roaring64NavigableMap(boolean signedLongs, boolean cacheCardinalities) {
    this(signedLongs, cacheCardinalities, new RoaringBitmapSupplier());
  }

  /**
   * By default, longs are managed as unsigned longs and cardinalities are cached.
   *
   * @param supplier provide the logic to instantiate new {@link BitmapDataProvider}, typically
   *        instantiated once per high.
   */
  public Roaring64NavigableMap(BitmapDataProviderSupplier supplier) {
    this(DEFAULT_ORDER_IS_SIGNED, DEFAULT_CARDINALITIES_ARE_CACHED, supplier);
  }

  /**
   * By default, we activating cardinalities caching.
   *
   * @param signedLongs true if longs has to be ordered as plain java longs. False to handle them as
   *        unsigned 64bits long (as RoaringBitmap with unsigned integers)
   * @param supplier provide the logic to instantiate new {@link BitmapDataProvider}, typically
   *        instantiated once per high.
   */
  public Roaring64NavigableMap(boolean signedLongs, BitmapDataProviderSupplier supplier) {
    this(signedLongs, DEFAULT_CARDINALITIES_ARE_CACHED, supplier);
  }

  /**
   *
   * @param signedLongs true if longs has to be ordered as plain java longs. False to handle them as
   *        unsigned 64bits long (as RoaringBitmap with unsigned integers)
   * @param cacheCardinalities true if cardinalities have to be cached. It will prevent many
   *        iteration along the NavigableMap
   * @param supplier provide the logic to instantiate new {@link BitmapDataProvider}, typically
   *        instantiated once per high.
   */
  public Roaring64NavigableMap(
      boolean signedLongs, boolean cacheCardinalities, BitmapDataProviderSupplier supplier) {
    this.signedLongs = signedLongs;
    this.supplier = supplier;

    if (signedLongs) {
      highToBitmap = new TreeMap<>();
    } else {
      highToBitmap = new TreeMap<>(RoaringIntPacking.unsignedComparator());
    }

    this.doCacheCardinalities = cacheCardinalities;
    resetPerfHelpers();
  }

  private void resetPerfHelpers() {
    firstHighNotValid = RoaringIntPacking.highestHigh(signedLongs) + 1;
    allValid = false;

    sortedCumulatedCardinality = new long[0];
    sortedHighs = new int[0];

    latestAddedHigh = null;
  }

  // Package-friendly: for the sake of unit-testing
  // @VisibleForTesting
  NavigableMap<Integer, BitmapDataProvider> getHighToBitmap() {
    return highToBitmap;
  }

  // Package-friendly: for the sake of unit-testing
  // @VisibleForTesting
  int getLowestInvalidHigh() {
    return firstHighNotValid;
  }

  // Package-friendly: for the sake of unit-testing
  // @VisibleForTesting
  long[] getSortedCumulatedCardinality() {
    return sortedCumulatedCardinality;
  }

  private static String getClassName(BitmapDataProvider bitmap) {
    if (bitmap == null) {
      return "null";
    } else {
      return bitmap.getClass().getName();
    }
  }

  /**
   * Add the value to the container (set the value to "true"), whether it already appears or not.
   *
   * Java lacks native unsigned longs but the x argument is considered to be unsigned. Within
   * bitmaps, numbers are ordered according to{@link Long#compareUnsigned}. We order the numbers
   * like 0, 1, ..., 9223372036854775807, -9223372036854775808, -9223372036854775807,..., -1.
   *
   * @param x long value
   */
  @Override
  public void addLong(long x) {
    int high = high(x);
    int low = low(x);

    // Copy the reference to prevent race-condition
    Map.Entry<Integer, BitmapDataProvider> local = latestAddedHigh;

    BitmapDataProvider bitmap;
    if (local != null && local.getKey().intValue() == high) {
      bitmap = local.getValue();
    } else {
      bitmap = highToBitmap.get(high);
      if (bitmap == null) {
        bitmap = newRoaringBitmap();
        pushBitmapForHigh(high, bitmap);
      }
      latestAddedHigh = new AbstractMap.SimpleImmutableEntry<>(high, bitmap);
    }
    bitmap.add(low);

    invalidateAboveHigh(high);
  }

  /**
   * Add the integer value to the container (set the value to "true"), whether it already appears or
   * not.
   *
   * Javac lacks native unsigned integers but the x argument is considered to be unsigned. Within
   * bitmaps, numbers are ordered according to{@link Integer#compareUnsigned}. We order the numbers
   * like 0, 1, ..., 2147483647, -2147483648, -2147483647,..., -1.
   *
   * @param x integer value
   */
  public void addInt(int x) {
    addLong(Util.toUnsignedLong(x));
  }

  private BitmapDataProvider newRoaringBitmap() {
    return supplier.newEmpty();
  }

  private void invalidateAboveHigh(int high) {
    // The cardinalities after this bucket may not be valid anymore
    if (compare(firstHighNotValid, high) > 0) {
      // High was valid up to now
      firstHighNotValid = high;

      int indexNotValid = binarySearch(sortedHighs, firstHighNotValid);

      final int indexAfterWhichToReset;
      if (indexNotValid >= 0) {
        indexAfterWhichToReset = indexNotValid;
      } else {
        // We have invalidate a high not already present: added a value for a brand new high
        indexAfterWhichToReset = -indexNotValid - 1;
      }

      // This way, sortedHighs remains sorted, without making a new/shorter array
      Arrays.fill(sortedHighs, indexAfterWhichToReset, sortedHighs.length, highestHigh());
    }
    allValid = false;
  }

  private int compare(int x, int y) {
    if (signedLongs) {
      return Integer.compare(x, y);
    } else {
      return RoaringIntPacking.compareUnsigned(x, y);
    }
  }

  private void pushBitmapForHigh(int high, BitmapDataProvider bitmap) {
    // TODO .size is too slow
    // int nbHighBefore = highToBitmap.headMap(high).size();

    BitmapDataProvider previous = highToBitmap.put(high, bitmap);
    assert previous == null : "Should push only not-existing high";
  }

  private int low(long id) {
    return RoaringIntPacking.low(id);
  }

  private int high(long id) {
    return RoaringIntPacking.high(id);
  }

  /**
   * Returns the number of distinct integers added to the bitmap (e.g., number of bits set).
   * In general, it is a a mutator method due to caching: this function modifies
   * the internal state of the bitmap.
   * @return the cardinality
   */
  @Override
  public long getLongCardinality() {
    if (doCacheCardinalities) {
      if (highToBitmap.isEmpty()) {
        return 0L;
      }
      int indexOk = ensureCumulatives(highestHigh());

      // ensureCumulatives may have removed empty bitmaps
      if (highToBitmap.isEmpty()) {
        return 0L;
      }

      return sortedCumulatedCardinality[indexOk - 1];
    } else {
      long cardinality = 0L;
      for (BitmapDataProvider bitmap : highToBitmap.values()) {
        cardinality += bitmap.getLongCardinality();
      }
      return cardinality;
    }
  }

  /**
   * Returns the number of distinct integers added to the bitmap (e.g., number of bits set).
   * In general, it is a a mutator method due to caching: this function modifies
   * the internal state of the bitmap.
   * @return the cardinality as an int
   *
   * @throws UnsupportedOperationException if the cardinality does not fit in an int
   */
  public int getIntCardinality() throws UnsupportedOperationException {
    long cardinality = getLongCardinality();

    if (cardinality > Integer.MAX_VALUE) {
      // TODO: we should handle cardinality fitting in an unsigned int
      throw new UnsupportedOperationException(
          "Cannot call .getIntCardinality as the cardinality is bigger than Integer.MAX_VALUE");
    }

    return (int) cardinality;
  }

  /**
   * Return the jth value stored in this bitmap.
   *
   * @param j index of the value
   *
   * @return the value
   * @throws IllegalArgumentException if j is out of the bounds of the bitmap cardinality
   */
  @Override
  public long select(final long j) throws IllegalArgumentException {
    if (!doCacheCardinalities) {
      return selectNoCache(j);
    }

    // Ensure all cumulatives as we we have straightforward way to know in advance the high of the
    // j-th value
    int indexOk = ensureCumulatives(highestHigh());

    if (highToBitmap.isEmpty()) {
      return throwSelectInvalidIndex(j);
    }

    // Use normal binarySearch as cardinality does not depends on considering longs signed or
    // unsigned
    // We need sortedCumulatedCardinality not to contain duplicated, else binarySearch may return
    // any of the duplicates: we need to ensure it holds no high associated to an empty bitmap
    int position = Arrays.binarySearch(sortedCumulatedCardinality, 0, indexOk, j);

    if (position >= 0) {
      if (position == indexOk - 1) {
        // .select has been called on this.getCardinality
        return throwSelectInvalidIndex(j);
      }

      // There is a bucket leading to this cardinality: the j-th element is the first element of
      // next bucket
      int high = sortedHighs[position + 1];
      BitmapDataProvider nextBitmap = highToBitmap.get(high);
      return RoaringIntPacking.pack(high, nextBitmap.select(0));
    } else {
      // There is no bucket with this cardinality
      int insertionPoint = -position - 1;

      final long previousBucketCardinality;
      if (insertionPoint == 0) {
        previousBucketCardinality = 0L;
      } else if (insertionPoint >= indexOk) {
        return throwSelectInvalidIndex(j);
      } else {
        previousBucketCardinality = sortedCumulatedCardinality[insertionPoint - 1];
      }

      // We get a 'select' query for a single bitmap: should fit in an int
      final int givenBitmapSelect = (int) (j - previousBucketCardinality);

      int high = sortedHighs[insertionPoint];
      BitmapDataProvider lowBitmap = highToBitmap.get(high);
      int low = lowBitmap.select(givenBitmapSelect);

      return RoaringIntPacking.pack(high, low);
    }
  }

  // For benchmarks: compute without using cardinalities cache
  // https://github.com/RoaringBitmap/CRoaring/blob/master/cpp/roaring64map.hh
  private long selectNoCache(long j) {
    long left = j;

    for (Entry<Integer, BitmapDataProvider> entry : highToBitmap.entrySet()) {
      long lowCardinality = entry.getValue().getCardinality();

      if (left >= lowCardinality) {
        left -= lowCardinality;
      } else {
        // It is legit for left to be negative
        int leftAsUnsignedInt = (int) left;
        return RoaringIntPacking.pack(entry.getKey(), entry.getValue().select(leftAsUnsignedInt));
      }
    }

    return throwSelectInvalidIndex(j);
  }

  private long throwSelectInvalidIndex(long j) {
    // see org.roaringbitmap.buffer.ImmutableRoaringBitmap.select(int)
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
        // TODO?
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public void forEach(final LongConsumer lc) {
    for (final Entry<Integer, BitmapDataProvider> highEntry : highToBitmap.entrySet()) {
      highEntry
          .getValue()
          .forEach(
              new IntConsumer() {

                @Override
                public void accept(int low) {
                  lc.accept(RoaringIntPacking.pack(highEntry.getKey(), low));
                }
              });
    }
  }

  @Override
  public long rankLong(long id) {
    int high = RoaringIntPacking.high(id);
    int low = RoaringIntPacking.low(id);

    if (!doCacheCardinalities) {
      return rankLongNoCache(high, low);
    }

    int indexOk = ensureCumulatives(high);

    int highPosition = binarySearch(sortedHighs, 0, indexOk, high);

    if (highPosition >= 0) {
      // There is a bucket holding this item

      final long previousBucketCardinality;
      if (highPosition == 0) {
        previousBucketCardinality = 0;
      } else {
        previousBucketCardinality = sortedCumulatedCardinality[highPosition - 1];
      }

      BitmapDataProvider lowBitmap = highToBitmap.get(sortedHighs[highPosition]);

      // Rank is previous cardinality plus rank in current bitmap
      return previousBucketCardinality + lowBitmap.rankLong(low);
    } else {
      // There is no bucket holding this item: insertionPoint is previous bitmap
      int insertionPoint = -highPosition - 1;

      if (insertionPoint == 0) {
        // this key is before all inserted keys
        return 0;
      } else {
        // The rank is the cardinality of this previous bitmap
        return sortedCumulatedCardinality[insertionPoint - 1];
      }
    }
  }

  // https://github.com/RoaringBitmap/CRoaring/blob/master/cpp/roaring64map.hh
  private long rankLongNoCache(int high, int low) {
    long result = 0L;

    BitmapDataProvider lastBitmap = highToBitmap.get(high);
    if (lastBitmap == null) {
      // There is no value with same high: the rank is a sum of cardinalities
      for (Entry<Integer, BitmapDataProvider> bitmap : highToBitmap.entrySet()) {
        if (bitmap.getKey().intValue() > high) {
          break;
        } else {
          result += bitmap.getValue().getLongCardinality();
        }
      }
    } else {
      for (BitmapDataProvider bitmap : highToBitmap.values()) {
        if (bitmap == lastBitmap) {
          result += bitmap.rankLong(low);
          break;
        } else {
          result += bitmap.getLongCardinality();
        }
      }
    }

    return result;
  }

  /**
   *
   * @param high for which high bucket should we compute the cardinality
   * @return the highest validatedIndex
   */
  protected int ensureCumulatives(int high) {
    if (allValid) {
      // the whole array is valid (up-to its actual length, not its capacity)
      return highToBitmap.size();
    } else if (compare(high, firstHighNotValid) < 0) {
      // The high is strictly below the first not valid: it is valid

      // sortedHighs may have only a subset of valid values on the right. However, these invalid
      // values have been set to maxValue, and we are here as high < firstHighNotValid ==> high <
      // maxHigh()
      int position = binarySearch(sortedHighs, high);

      if (position >= 0) {
        // This high has a bitmap: +1 as this index will be used as right (excluded) bound in a
        // binary-search
        return position + 1;
      } else {
        // This high has no bitmap: it could be between 2 highs with bitmaps
        int insertionPosition = -position - 1;
        return insertionPosition;
      }
    } else {

      // For each deprecated buckets
      SortedMap<Integer, BitmapDataProvider> tailMap =
          highToBitmap.tailMap(firstHighNotValid, true);

      // TODO .size on tailMap make an iterator: arg
      int indexOk = highToBitmap.size() - tailMap.size();

      // TODO: It should be possible to compute indexOk based on sortedHighs array
      // assert indexOk == binarySearch(sortedHighs, firstHighNotValid);

      Iterator<Entry<Integer, BitmapDataProvider>> it = tailMap.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Integer, BitmapDataProvider> e = it.next();
        int currentHigh = e.getKey();

        if (compare(currentHigh, high) > 0) {
          // No need to compute more than needed
          break;
        } else if (e.getValue().isEmpty()) {
          // highToBitmap cannot be modified as we iterate over it
          if (latestAddedHigh != null && latestAddedHigh.getKey().intValue() == currentHigh) {
            // Dismiss the cached bitmap as it is removed from the NavigableMap
            latestAddedHigh = null;
          }
          it.remove();
        } else {
          ensureOne(e, currentHigh, indexOk);

          // We have added one valid cardinality
          indexOk++;
        }
      }

      if (highToBitmap.isEmpty() || indexOk == highToBitmap.size()) {
        // We have compute all cardinalities
        allValid = true;
      }

      return indexOk;
    }
  }

  private int binarySearch(int[] array, int key) {
    if (signedLongs) {
      return Arrays.binarySearch(array, key);
    } else {
      return unsignedBinarySearch(
          array, 0, array.length, key, RoaringIntPacking.unsignedComparator());
    }
  }

  private int binarySearch(int[] array, int from, int to, int key) {
    if (signedLongs) {
      return Arrays.binarySearch(array, from, to, key);
    } else {
      return unsignedBinarySearch(array, from, to, key, RoaringIntPacking.unsignedComparator());
    }
  }

  // From Arrays.binarySearch (Comparator). Check with org.roaringbitmap.Util.unsignedBinarySearch
  private static int unsignedBinarySearch(
      int[] a, int fromIndex, int toIndex, int key, Comparator<? super Integer> c) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = a[mid];
      int cmp = c.compare(midVal, key);
      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1); // key not found.
  }

  private void ensureOne(Map.Entry<Integer, BitmapDataProvider> e, int currentHigh, int indexOk) {
    // sortedHighs are valid only up to some index
    assert indexOk <= sortedHighs.length : indexOk + " is bigger than " + sortedHighs.length;

    final int index;
    if (indexOk == 0) {
      if (sortedHighs.length == 0) {
        index = -1;
        // } else if (sortedHighs[0] == currentHigh) {
        // index = 0;
      } else {
        index = -1;
      }
    } else if (indexOk < sortedHighs.length) {
      index = -indexOk - 1;
    } else {
      index = -sortedHighs.length - 1;
    }
    assert index == binarySearch(sortedHighs, 0, indexOk, currentHigh)
        : "Computed "
            + index
            + " differs from dummy binary-search index: "
            + binarySearch(sortedHighs, 0, indexOk, currentHigh);

    if (index >= 0) {
      // This would mean calling .ensureOne is useless: should never got here at the first time
      throw new IllegalStateException(
          "Unexpectedly found "
              + currentHigh
              + " in "
              + Arrays.toString(sortedHighs)
              + " strictly before index"
              + indexOk);
    } else {
      int insertionPosition = -index - 1;

      // This is a new key
      if (insertionPosition >= sortedHighs.length) {
        int previousSize = sortedHighs.length;

        // TODO softer growing factor
        int newSize = Math.min(Integer.MAX_VALUE, sortedHighs.length * 2 + 1);

        // Insertion at the end
        sortedHighs = Arrays.copyOf(sortedHighs, newSize);
        sortedCumulatedCardinality = Arrays.copyOf(sortedCumulatedCardinality, newSize);

        // Not actually needed. But simplify the reading of array content
        Arrays.fill(sortedHighs, previousSize, sortedHighs.length, highestHigh());
        Arrays.fill(sortedCumulatedCardinality, previousSize, sortedHighs.length, Long.MAX_VALUE);
      }
      sortedHighs[insertionPosition] = currentHigh;

      final long previousCardinality;
      if (insertionPosition >= 1) {
        previousCardinality = sortedCumulatedCardinality[insertionPosition - 1];
      } else {
        previousCardinality = 0;
      }

      sortedCumulatedCardinality[insertionPosition] =
          previousCardinality + e.getValue().getLongCardinality();

      if (currentHigh == highestHigh()) {
        // We are already on the highest high. Do not set allValid as it is set anyway out of the
        // loop
        firstHighNotValid = currentHigh;
      } else {
        // The first not valid is the next high
        // TODO: The entry comes from a NavigableMap: it may be quite cheap to know the next high
        firstHighNotValid = currentHigh + 1;
      }
    }
  }

  private int highestHigh() {
    return RoaringIntPacking.highestHigh(signedLongs);
  }

  /**
   * In-place bitwise OR (union) operation without maintaining cardinality.
   * Don't forget to call repairAfterLazy() afterward. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void naivelazyor(final Roaring64NavigableMap x2) {
    if (this == x2) {
      return;
    }
    for (Entry<Integer, BitmapDataProvider> e2 : x2.highToBitmap.entrySet()) {
      // Keep object to prevent auto-boxing
      Integer high = e2.getKey();

      BitmapDataProvider lowBitmap1 = this.highToBitmap.get(high);

      BitmapDataProvider lowBitmap2 = e2.getValue();

      if (lowBitmap1 == null) {
        // Clone to prevent future modification of this modifying the input Bitmap
        BitmapDataProvider lowBitmap2Clone;
        if (lowBitmap2 instanceof RoaringBitmap) {
          lowBitmap2Clone = ((RoaringBitmap) lowBitmap2).clone();
        } else if (lowBitmap2 instanceof MutableRoaringBitmap) {
          lowBitmap2Clone = ((MutableRoaringBitmap) lowBitmap2).clone();
        } else {
          throw new UnsupportedOperationException(
              ".naivelazyor(...) over " + getClassName(lowBitmap2));
        }

        pushBitmapForHigh(high, lowBitmap2Clone);
      } else if (lowBitmap1 instanceof RoaringBitmap && lowBitmap2 instanceof RoaringBitmap) {
        RoaringBitmapPrivate.naivelazyor((RoaringBitmap) lowBitmap1, (RoaringBitmap) lowBitmap2);
      } else if (lowBitmap1 instanceof MutableRoaringBitmap
          && lowBitmap2 instanceof MutableRoaringBitmap) {
        MutableRoaringBitmapPrivate.naivelazyor(
            (MutableRoaringBitmap) lowBitmap1, (MutableRoaringBitmap) lowBitmap2);
      } else {
        throw new UnsupportedOperationException(
            ".naivelazyor(...) over "
                + getClassName(lowBitmap1)
                + " and "
                + getClassName(lowBitmap2));
      }
    }
  }

  /**
   * In-place bitwise OR (union) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void or(final Roaring64NavigableMap x2) {
    if (this == x2) {
      return;
    }
    boolean firstBucket = true;

    for (Entry<Integer, BitmapDataProvider> e2 : x2.highToBitmap.entrySet()) {
      // Keep object to prevent auto-boxing
      Integer high = e2.getKey();

      BitmapDataProvider lowBitmap1 = this.highToBitmap.get(high);

      BitmapDataProvider lowBitmap2 = e2.getValue();

      // TODO Reviewers: is it a good idea to rely on BitmapDataProvider except in methods
      // expecting an actual MutableRoaringBitmap?
      // TODO This code may lead to closing a buffer Bitmap in current Navigable even if current is
      // not on buffer
      if ((lowBitmap1 == null || lowBitmap1 instanceof RoaringBitmap)
          && lowBitmap2 instanceof RoaringBitmap) {
        if (lowBitmap1 == null) {
          // Clone to prevent future modification of this modifying the input Bitmap
          RoaringBitmap lowBitmap2Clone = ((RoaringBitmap) lowBitmap2).clone();

          pushBitmapForHigh(high, lowBitmap2Clone);
        } else {
          ((RoaringBitmap) lowBitmap1).or((RoaringBitmap) lowBitmap2);
        }
      } else if ((lowBitmap1 == null || lowBitmap1 instanceof MutableRoaringBitmap)
          && lowBitmap2 instanceof MutableRoaringBitmap) {
        if (lowBitmap1 == null) {
          // Clone to prevent future modification of this modifying the input Bitmap
          BitmapDataProvider lowBitmap2Clone = ((MutableRoaringBitmap) lowBitmap2).clone();

          pushBitmapForHigh(high, lowBitmap2Clone);
        } else {
          ((MutableRoaringBitmap) lowBitmap1).or((MutableRoaringBitmap) lowBitmap2);
        }
      } else {
        throw new UnsupportedOperationException(
            ".or(...) over " + getClassName(lowBitmap1) + " and " + getClassName(lowBitmap2));
      }

      if (firstBucket) {
        firstBucket = false;

        // Invalidate the lowest high as lowest not valid
        firstHighNotValid = Math.min(firstHighNotValid, high);
        allValid = false;
      }
    }
  }

  /**
   * Bitwise OR (union) operation. The provided bitmaps are *not* modified. This operation is
   * thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   */
  public static Roaring64NavigableMap or(
      final Roaring64NavigableMap x1, final Roaring64NavigableMap x2) {
    Roaring64NavigableMap result = new Roaring64NavigableMap();
    if (x1 == x2) {
      return x1;
    }
    if (x1 == null) {
      return x2;
    }
    if (x2 == null) {
      return x1;
    }

    Iterator<Entry<Integer, BitmapDataProvider>> x1Iterator =
        x1.getHighToBitmap().entrySet().iterator();
    Iterator<Entry<Integer, BitmapDataProvider>> x2Iterator =
        x2.getHighToBitmap().entrySet().iterator();
    Entry<Integer, BitmapDataProvider> x1Entry = x1Iterator.hasNext() ? x1Iterator.next() : null;
    Entry<Integer, BitmapDataProvider> x2Entry = x2Iterator.hasNext() ? x2Iterator.next() : null;

    while (x1Entry != null || x2Entry != null) {
      if (x1Entry == null) {
        pushClonedEntry(x2Entry, result);
        x2Entry = x2Iterator.hasNext() ? x2Iterator.next() : null;
      } else if (x2Entry == null) {
        pushClonedEntry(x1Entry, result);
        x1Entry = x1Iterator.hasNext() ? x1Iterator.next() : null;
      } else {
        // compare & merge
        int compare = Integer.compareUnsigned(x1Entry.getKey(), x2Entry.getKey());
        if (compare == 0) {
          if (x1Entry.getValue() instanceof RoaringBitmap
              && x2Entry.getValue() instanceof RoaringBitmap) {
            result.pushBitmapForHigh(
                x1Entry.getKey(),
                RoaringBitmap.or(
                    (RoaringBitmap) x1Entry.getValue(), (RoaringBitmap) x2Entry.getValue()));
          } else if (x1Entry.getValue() instanceof MutableRoaringBitmap
              && x2Entry.getValue() instanceof MutableRoaringBitmap) {
            result.pushBitmapForHigh(
                x1Entry.getKey(),
                MutableRoaringBitmap.or(
                    (MutableRoaringBitmap) x1Entry.getValue(),
                    (MutableRoaringBitmap) x2Entry.getValue()));
          } else {
            throw new UnsupportedOperationException(
                ".or(...) over "
                    + getClassName(x1Entry.getValue())
                    + " and "
                    + getClassName(x2Entry.getValue()));
          }
          x1Entry = x1Iterator.hasNext() ? x1Iterator.next() : null;
          x2Entry = x2Iterator.hasNext() ? x2Iterator.next() : null;
        } else if (compare < 0) {
          pushClonedEntry(x1Entry, result);
          x1Entry = x1Iterator.hasNext() ? x1Iterator.next() : null;
        } else {
          pushClonedEntry(x2Entry, result);
          x2Entry = x2Iterator.hasNext() ? x2Iterator.next() : null;
        }
      }
    }

    result.resetPerfHelpers();
    return result;
  }

  /**
   * static method for push a new entry(cloned) into target Roaring64NavigableMap
   *
   * @param entry the original entry item of a Roaring64NavigableMap instance
   * @param result the target Roaring64NavigableMap
   */
  private static void pushClonedEntry(
      Entry<Integer, BitmapDataProvider> entry, Roaring64NavigableMap result) {
    if (entry.getValue() instanceof RoaringBitmap) {
      result.pushBitmapForHigh(entry.getKey(), ((RoaringBitmap) (entry.getValue())).clone());
    } else if (entry.getValue() instanceof MutableRoaringBitmap) {
      result.pushBitmapForHigh(entry.getKey(), ((MutableRoaringBitmap) (entry.getValue())).clone());
    } else {
      throw new UnsupportedOperationException(
          ". Unsupported type for pushClonedEntry: " + getClassName(entry.getValue()));
    }
  }

  /**
   * In-place bitwise XOR (symmetric difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void xor(final Roaring64NavigableMap x2) {
    if (x2 == this) {
      clear();
      return;
    }
    boolean firstBucket = true;

    for (Entry<Integer, BitmapDataProvider> e2 : x2.highToBitmap.entrySet()) {
      // Keep object to prevent auto-boxing
      Integer high = e2.getKey();

      BitmapDataProvider lowBitmap1 = this.highToBitmap.get(high);

      BitmapDataProvider lowBitmap2 = e2.getValue();

      // TODO Reviewers: is it a good idea to rely on BitmapDataProvider except in methods
      // expecting an actual MutableRoaringBitmap?
      // TODO This code may lead to closing a buffer Bitmap in current Navigable even if current is
      // not on buffer
      if ((lowBitmap1 == null || lowBitmap1 instanceof RoaringBitmap)
          && lowBitmap2 instanceof RoaringBitmap) {
        if (lowBitmap1 == null) {
          // Clone to prevent future modification of this modifying the input Bitmap
          RoaringBitmap lowBitmap2Clone = ((RoaringBitmap) lowBitmap2).clone();

          pushBitmapForHigh(high, lowBitmap2Clone);
        } else {
          ((RoaringBitmap) lowBitmap1).xor((RoaringBitmap) lowBitmap2);
        }
      } else if ((lowBitmap1 == null || lowBitmap1 instanceof MutableRoaringBitmap)
          && lowBitmap2 instanceof MutableRoaringBitmap) {
        if (lowBitmap1 == null) {
          // Clone to prevent future modification of this modifying the input Bitmap
          BitmapDataProvider lowBitmap2Clone = ((MutableRoaringBitmap) lowBitmap2).clone();

          pushBitmapForHigh(high, lowBitmap2Clone);
        } else {
          ((MutableRoaringBitmap) lowBitmap1).xor((MutableRoaringBitmap) lowBitmap2);
        }
      } else {
        throw new UnsupportedOperationException(
            ".or(...) over " + getClassName(lowBitmap1) + " and " + getClassName(lowBitmap2));
      }

      if (firstBucket) {
        firstBucket = false;

        // Invalidate the lowest high as lowest not valid
        firstHighNotValid = Math.min(firstHighNotValid, high);
        allValid = false;
      }
    }
  }

  /**
   * Checks whether the two bitmaps intersect. This can be much faster than calling "and" and
   * checking the cardinality of the result.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return true if they intersect
   */
  public static boolean intersects(final Roaring64NavigableMap x1, final Roaring64NavigableMap x2) {
    if (x2 == x1) {
      return true;
    }
    if (x1 == null || x2 == null) {
      return false;
    }

    // find the one with smaller entries cnt
    long x1HighCnt = x1.getHighToBitmap().size();
    long x2HighCnt = x2.getHighToBitmap().size();
    Roaring64NavigableMap outer = x1HighCnt < x2HighCnt ? x1 : x2;
    Roaring64NavigableMap inner = x1HighCnt < x2HighCnt ? x2 : x1;
    for (Entry<Integer, BitmapDataProvider> entry : outer.getHighToBitmap().entrySet()) {
      BitmapDataProvider lowBitmap1 = entry.getValue();
      BitmapDataProvider lowBitmap2 = inner.getHighToBitmap().get(entry.getKey());
      if (lowBitmap2 == null) {
        continue;
      }
      if (lowBitmap1 instanceof RoaringBitmap && lowBitmap2 instanceof RoaringBitmap) {
        if (RoaringBitmap.intersects((RoaringBitmap) lowBitmap1, (RoaringBitmap) lowBitmap2)) {
          return true;
        }
      } else if (lowBitmap1 instanceof MutableRoaringBitmap
          && lowBitmap2 instanceof MutableRoaringBitmap) {
        if (MutableRoaringBitmap.intersects(
            (MutableRoaringBitmap) lowBitmap1, (MutableRoaringBitmap) lowBitmap2)) {
          return true;
        }
      } else {
        throw new UnsupportedOperationException(
            ".intersects(...) over "
                + getClassName(lowBitmap1)
                + " and "
                + getClassName(lowBitmap2));
      }
    }

    return false;
  }

  /**
   * In-place bitwise AND (intersection) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void and(final Roaring64NavigableMap x2) {
    if (x2 == this) {
      return;
    }
    boolean firstBucket = true;

    Iterator<Entry<Integer, BitmapDataProvider>> thisIterator = highToBitmap.entrySet().iterator();
    while (thisIterator.hasNext()) {
      Entry<Integer, BitmapDataProvider> e1 = thisIterator.next();

      // Keep object to prevent auto-boxing
      Integer high = e1.getKey();

      BitmapDataProvider lowBitmap2 = x2.highToBitmap.get(high);

      if (lowBitmap2 == null) {
        // None of given high values are present in x2
        thisIterator.remove();
      } else {
        BitmapDataProvider lowBitmap1 = e1.getValue();

        if (lowBitmap2 instanceof RoaringBitmap && lowBitmap1 instanceof RoaringBitmap) {
          ((RoaringBitmap) lowBitmap1).and((RoaringBitmap) lowBitmap2);
        } else if (lowBitmap2 instanceof MutableRoaringBitmap
            && lowBitmap1 instanceof MutableRoaringBitmap) {
          ((MutableRoaringBitmap) lowBitmap1).and((MutableRoaringBitmap) lowBitmap2);
        } else {
          throw new UnsupportedOperationException(
              ".and(...) over " + getClassName(lowBitmap1) + " and " + getClassName(lowBitmap2));
        }
      }

      if (firstBucket) {
        firstBucket = false;

        // Invalidate the lowest high as lowest not valid
        firstHighNotValid = Math.min(firstHighNotValid, high);
        allValid = false;
      }
    }
  }

  /**
   * Bitwise AND (intersection) operation. The provided bitmaps are *not* modified. This operation
   * is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   */
  public static Roaring64NavigableMap and(
      final Roaring64NavigableMap x1, final Roaring64NavigableMap x2) {
    Roaring64NavigableMap result = new Roaring64NavigableMap();
    if (x2 == x1) {
      return x1;
    }
    if (x1 == null || x2 == null) {
      return result;
    }
    Iterator<Entry<Integer, BitmapDataProvider>> x1Iterator =
        x1.getHighToBitmap().entrySet().iterator();
    while (x1Iterator.hasNext()) {
      Entry<Integer, BitmapDataProvider> e1 = x1Iterator.next();
      // Keep object to prevent auto-boxing
      Integer high = e1.getKey();

      BitmapDataProvider lowBitmap1 = e1.getValue();
      BitmapDataProvider lowBitmap2 = x2.getHighToBitmap().get(high);
      if (lowBitmap2 != null) {
        if (lowBitmap2 instanceof RoaringBitmap && lowBitmap1 instanceof RoaringBitmap) {
          RoaringBitmap andResult =
              RoaringBitmap.and((RoaringBitmap) lowBitmap1, (RoaringBitmap) lowBitmap2);
          result.pushBitmapForHigh(high, andResult);
        } else if (lowBitmap2 instanceof MutableRoaringBitmap
            && lowBitmap1 instanceof MutableRoaringBitmap) {
          MutableRoaringBitmap andResult =
              MutableRoaringBitmap.and(
                  (MutableRoaringBitmap) lowBitmap1, (MutableRoaringBitmap) lowBitmap2);
          result.pushBitmapForHigh(high, andResult);
        } else {
          throw new UnsupportedOperationException(
              ".and(...) over " + getClassName(lowBitmap1) + " and " + getClassName(lowBitmap2));
        }
      }
    }
    result.resetPerfHelpers();
    return result;
  }

  /**
   * Cardinality of Bitwise AND (intersection) operation. The provided bitmaps are *not* modified.
   * This operation is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return as if you did and(x1,x2).getCardinality()
   */
  public static long andCardinality(
      final Roaring64NavigableMap x1, final Roaring64NavigableMap x2) {
    long cardinality = 0;
    if (x1 == null || x2 == null) {
      return cardinality;
    }
    if (x1.getHighToBitmap().isEmpty() || x2.getHighToBitmap().isEmpty()) {
      return cardinality;
    }
    if (x2 == x1) {
      return x1.getLongCardinality();
    }

    Iterator<Entry<Integer, BitmapDataProvider>> x1Iterator =
        x1.getHighToBitmap().entrySet().iterator();
    Iterator<Entry<Integer, BitmapDataProvider>> x2Iterator =
        x2.getHighToBitmap().entrySet().iterator();
    Entry<Integer, BitmapDataProvider> x1Entry = x1Iterator.next();
    Entry<Integer, BitmapDataProvider> x2Entry = x2Iterator.next();

    while (x1Entry != null && x2Entry != null) {
      int highKey1 = x1Entry.getKey();
      int highKey2 = x2Entry.getKey();
      BitmapDataProvider lowBitmap1 = x1Entry.getValue();
      BitmapDataProvider lowBitmap2 = x2Entry.getValue();

      int compare = Integer.compareUnsigned(highKey1, highKey2);
      if (compare == 0) {
        if (lowBitmap2 instanceof RoaringBitmap && lowBitmap1 instanceof RoaringBitmap) {
          cardinality +=
              RoaringBitmap.andCardinality((RoaringBitmap) lowBitmap1, (RoaringBitmap) lowBitmap2);
        } else if (lowBitmap2 instanceof MutableRoaringBitmap
            && lowBitmap1 instanceof MutableRoaringBitmap) {
          cardinality +=
              MutableRoaringBitmap.andCardinality(
                  (MutableRoaringBitmap) lowBitmap1, (MutableRoaringBitmap) lowBitmap2);
        } else {
          throw new UnsupportedOperationException(
              ".andCardinality(...) over "
                  + getClassName(lowBitmap1)
                  + " and "
                  + getClassName(lowBitmap2));
        }
        x1Entry = x1Iterator.hasNext() ? x1Iterator.next() : null;
        x2Entry = x2Iterator.hasNext() ? x2Iterator.next() : null;
      } else if (compare < 0) {
        x1Entry = x1Iterator.hasNext() ? x1Iterator.next() : null;
      } else {
        x2Entry = x2Iterator.hasNext() ? x2Iterator.next() : null;
      }
    }

    return cardinality;
  }

  /**
   * In-place bitwise ANDNOT (difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void andNot(final Roaring64NavigableMap x2) {
    if (x2 == this) {
      clear();
      return;
    }
    boolean firstBucket = true;

    Iterator<Entry<Integer, BitmapDataProvider>> thisIterator = highToBitmap.entrySet().iterator();
    while (thisIterator.hasNext()) {
      Entry<Integer, BitmapDataProvider> e1 = thisIterator.next();

      // Keep object to prevent auto-boxing
      Integer high = e1.getKey();

      BitmapDataProvider lowBitmap2 = x2.highToBitmap.get(high);

      if (lowBitmap2 != null) {
        BitmapDataProvider lowBitmap1 = e1.getValue();

        if (lowBitmap2 instanceof RoaringBitmap && lowBitmap1 instanceof RoaringBitmap) {
          ((RoaringBitmap) lowBitmap1).andNot((RoaringBitmap) lowBitmap2);
        } else if (lowBitmap2 instanceof MutableRoaringBitmap
            && lowBitmap1 instanceof MutableRoaringBitmap) {
          ((MutableRoaringBitmap) lowBitmap1).andNot((MutableRoaringBitmap) lowBitmap2);
        } else {
          throw new UnsupportedOperationException(
              ".and(...) over " + getClassName(lowBitmap1) + " and " + getClassName(lowBitmap2));
        }
      }

      if (firstBucket) {
        firstBucket = false;

        // Invalidate the lowest high as lowest not valid
        firstHighNotValid = Math.min(firstHighNotValid, high);
        allValid = false;
      }
    }
  }

  /**
   * Bitwise ANDNOT (difference) operation. The provided bitmaps are *not* modified. This operation
   * is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   */
  public static Roaring64NavigableMap andNot(
      final Roaring64NavigableMap x1, final Roaring64NavigableMap x2) {
    Roaring64NavigableMap result = new Roaring64NavigableMap();
    if (x1 == x2 || x1 == null || x1.getHighToBitmap() == null) {
      return result;
    }
    if (x2 == null || x2.getHighToBitmap() == null) {
      return x1;
    }

    Iterator<Entry<Integer, BitmapDataProvider>> x1Iterator =
        x1.getHighToBitmap().entrySet().iterator();
    while (x1Iterator.hasNext()) {
      Entry<Integer, BitmapDataProvider> e1 = x1Iterator.next();

      // Keep object to prevent auto-boxing
      Integer high = e1.getKey();
      BitmapDataProvider lowBitmap1 = e1.getValue();
      BitmapDataProvider lowBitmap2 = x2.getHighToBitmap().get(high);
      if (lowBitmap2 != null) {
        if (lowBitmap2 instanceof RoaringBitmap && lowBitmap1 instanceof RoaringBitmap) {
          RoaringBitmap andNotResult =
              RoaringBitmap.andNot((RoaringBitmap) lowBitmap1, (RoaringBitmap) lowBitmap2);
          result.pushBitmapForHigh(high, andNotResult);
        } else if (lowBitmap2 instanceof MutableRoaringBitmap
            && lowBitmap1 instanceof MutableRoaringBitmap) {
          MutableRoaringBitmap andNotResult =
              MutableRoaringBitmap.andNot(
                  (MutableRoaringBitmap) lowBitmap1, (MutableRoaringBitmap) lowBitmap2);
          result.pushBitmapForHigh(high, andNotResult);
        } else {
          throw new UnsupportedOperationException(
              ".andNot(...) over " + getClassName(lowBitmap1) + " and " + getClassName(lowBitmap2));
        }
      } else {
        result.pushBitmapForHigh(high, lowBitmap1);
      }
    }
    result.resetPerfHelpers();
    return result;
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
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    deserialize(in);
  }

  /**
   * A string describing the bitmap.
   *
   * @return the string
   */
  @Override
  public String toString() {
    final StringBuilder answer =
        new StringBuilder("{}".length() + "-1234567890123456789,".length() * 256);
    final LongIterator i = this.getLongIterator();
    answer.append('{');
    if (i.hasNext()) {
      if (signedLongs) {
        answer.append(i.next());
      } else {
        answer.append(RoaringIntPacking.toUnsignedString(i.next()));
      }
    }
    while (i.hasNext()) {
      answer.append(',');
      // to avoid using too much memory, we limit the size
      if (answer.length() > 0x80000) {
        answer.append('.').append('.').append('.');
        break;
      }
      if (signedLongs) {
        answer.append(i.next());
      } else {
        answer.append(RoaringIntPacking.toUnsignedString(i.next()));
      }
    }
    answer.append("}");
    return answer.toString();
  }

  /**
   *
   * For better performance, consider the Use the {@link #forEach forEach} method.
   *
   * @return a custom iterator over set bits, the bits are traversed in ascending sorted order
   */
  @Override
  public LongIterator getLongIterator() {
    final Iterator<Map.Entry<Integer, BitmapDataProvider>> it = highToBitmap.entrySet().iterator();

    return toIterator(it, false);
  }

  protected LongIterator toIterator(
      final Iterator<Map.Entry<Integer, BitmapDataProvider>> it, final boolean reversed) {
    return new LongIterator() {

      protected int currentKey;
      protected IntIterator currentIt;

      @Override
      public boolean hasNext() {
        if (currentIt == null) {
          // Were initially empty
          if (!moveToNextEntry(it)) {
            return false;
          }
        }

        while (true) {
          if (currentIt.hasNext()) {
            return true;
          } else {
            if (!moveToNextEntry(it)) {
              return false;
            }
          }
        }
      }

      /**
       *
       * @param it the underlying iterator which has to be moved to next long
       * @return true if we MAY have more entries. false if there is definitely nothing more
       */
      private boolean moveToNextEntry(Iterator<Map.Entry<Integer, BitmapDataProvider>> it) {
        if (it.hasNext()) {
          Map.Entry<Integer, BitmapDataProvider> next = it.next();
          currentKey = next.getKey();
          if (reversed) {
            currentIt = next.getValue().getReverseIntIterator();
          } else {
            currentIt = next.getValue().getIntIterator();
          }

          // We may have more long
          return true;
        } else {
          // We know there is nothing more
          return false;
        }
      }

      @Override
      public long next() {
        if (hasNext()) {
          return RoaringIntPacking.pack(currentKey, currentIt.next());
        } else {
          throw new IllegalStateException("empty");
        }
      }

      @Override
      public LongIterator clone() {
        throw new UnsupportedOperationException("TODO");
      }
    };
  }

  @Override
  public boolean contains(long x) {
    int high = RoaringIntPacking.high(x);
    BitmapDataProvider lowBitmap = highToBitmap.get(high);
    if (lowBitmap == null) {
      return false;
    }

    int low = RoaringIntPacking.low(x);
    return lowBitmap.contains(low);
  }

  @Override
  public int getSizeInBytes() {
    return (int) getLongSizeInBytes();
  }

  @Override
  public long getLongSizeInBytes() {
    long size = 8;

    // Size of containers
    size += highToBitmap.values().stream().mapToLong(p -> p.getLongSizeInBytes()).sum();

    // Size of Map data-structure: we consider each TreeMap entry costs 40 bytes
    // http://java-performance.info/memory-consumption-of-java-data-types-2/
    size += 8L + 40L * highToBitmap.size();

    // Size of (boxed) Integers used as keys
    size += 16L * highToBitmap.size();

    // The cache impacts the size in heap
    size += 8L * sortedCumulatedCardinality.length;
    size += 4L * sortedHighs.length;

    return size;
  }

  @Override
  /**
   * Returns true if the bitmap is empty (i.e., has no set bits).
   * In general, it is a a mutator method due to caching: this function modifies
   * the internal state of the bitmap.
   */
  public boolean isEmpty() {
    return getLongCardinality() == 0L;
  }

  @Override
  public ImmutableLongBitmapDataProvider limit(long x) {
    throw new UnsupportedOperationException("TODO");
  }

  /**
   * to be used with naivelazyor
   */
  public void repairAfterLazy() {
    for (BitmapDataProvider lowBitmap : highToBitmap.values()) {
      if (lowBitmap instanceof RoaringBitmap) {
        RoaringBitmapPrivate.repairAfterLazy((RoaringBitmap) lowBitmap);
      } else if (lowBitmap instanceof MutableRoaringBitmap) {
        MutableRoaringBitmapPrivate.repairAfterLazy((MutableRoaringBitmap) lowBitmap);
      } else {
        throw new UnsupportedOperationException(
            ".repairAfterLazy is not supported for " + lowBitmap.getClass());
      }
    }
  }

  /**
   * Use a run-length encoding where it is estimated as more space efficient
   *
   * @return whether a change was applied
   */
  public boolean runOptimize() {
    boolean hasChanged = false;
    for (BitmapDataProvider lowBitmap : highToBitmap.values()) {
      if (lowBitmap instanceof RoaringBitmap) {
        hasChanged |= ((RoaringBitmap) lowBitmap).runOptimize();
      } else if (lowBitmap instanceof MutableRoaringBitmap) {
        hasChanged |= ((MutableRoaringBitmap) lowBitmap).runOptimize();
      }
    }
    return hasChanged;
  }

  /**
   * Serialize this bitmap.
   *
   * If SERIALIZATION_MODE is set to SERIALIZATION_MODE_PORTABLE, this will rely on the
   * specification at
   * https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations.
   * As of 0.x, this is **not** the default behavior.
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
    if (SERIALIZATION_MODE == SERIALIZATION_MODE_PORTABLE) {
      serializePortable(out);
    } else {
      serializeLegacy(out);
    }
  }

  /**
   * Serialize this bitmap.
   *
   * This format was introduced before converting with CRoaring portable format. It remains useful
   * when considering signed longs. It is the default in 0.x
   *
   * Consider calling {@link #runOptimize} before serialization to improve compression.
   *
   * The current bitmap is not modified.
   *
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Deprecated
  public void serializeLegacy(DataOutput out) throws IOException {
    out.writeBoolean(signedLongs);

    out.writeInt(highToBitmap.size());

    for (Entry<Integer, BitmapDataProvider> entry : highToBitmap.entrySet()) {
      out.writeInt(entry.getKey().intValue());
      entry.getValue().serialize(out);
    }
  }

  /**
   * Serialize this bitmap.
   *
   * The format is specified at
   * https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations.
   * It is the compatible with CRoaring (and GoRoaring).
   *
   * Consider calling {@link #runOptimize} before serialization to improve compression.
   *
   * The current bitmap is not modified.
   *
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void serializePortable(DataOutput out) throws IOException {
    out.writeLong(Long.reverseBytes(highToBitmap.size()));

    for (Entry<Integer, BitmapDataProvider> entry : highToBitmap.entrySet()) {
      out.writeInt(Integer.reverseBytes(entry.getKey().intValue()));
      entry.getValue().serialize(out);
    }
  }

  /**
   * Deserialize (retrieve) this bitmap.
   *
   * If SERIALIZATION_MODE is set to SERIALIZATION_MODE_PORTABLE, this will rely on the
   * specification at
   * https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations.
   * As of 0.x, this is **not** the default behavior.
   *
   * The current bitmap is overwritten.
   *
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserialize(DataInput in) throws IOException {
    if (SERIALIZATION_MODE == SERIALIZATION_MODE_PORTABLE) {
      deserializePortable(in);
    } else {
      deserializeLegacy(in);
    }
  }

  /**
   * Deserialize (retrieve) this bitmap.
   *
   * This format was introduced before converting with CRoaring portable format. It remains useful
   * when considering signed longs. It is the default in 0.x
   *
   * The current bitmap is overwritten.
   *
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserializeLegacy(DataInput in) throws IOException {
    this.clear();

    signedLongs = in.readBoolean();

    int nbHighs = in.readInt();

    // Other NavigableMap may accept a target capacity
    if (signedLongs) {
      highToBitmap = new TreeMap<>();
    } else {
      highToBitmap = new TreeMap<>(RoaringIntPacking.unsignedComparator());
    }

    for (int i = 0; i < nbHighs; i++) {
      int high = in.readInt();
      BitmapDataProvider provider = newRoaringBitmap();
      if (provider instanceof RoaringBitmap) {
        ((RoaringBitmap) provider).deserialize(in);
      } else if (provider instanceof MutableRoaringBitmap) {
        ((MutableRoaringBitmap) provider).deserialize(in);
      } else {
        throw new UnsupportedEncodingException("Cannot deserialize a " + provider.getClass());
      }

      highToBitmap.put(high, provider);
    }

    resetPerfHelpers();
  }

  /**
   * Deserialize (retrieve) this bitmap.
   *
   * The format is specified at
   * https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations.
   * It is the compatible with CRoaring (and GoRoaring).
   *
   * The current bitmap is overwritten.
   *
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserializePortable(DataInput in) throws IOException {
    this.clear();

    // Portable format handles only unsigned longs
    signedLongs = false;

    // Read from LittleEndian to BigEndian
    long nbHighs = Long.reverseBytes(in.readLong());

    // Other NavigableMap may accept a target capacity
    highToBitmap = new TreeMap<>(RoaringIntPacking.unsignedComparator());

    for (int i = 0; i < nbHighs; i++) {
      int high = Integer.reverseBytes(in.readInt());
      BitmapDataProvider provider = newRoaringBitmap();
      if (provider instanceof RoaringBitmap) {
        ((RoaringBitmap) provider).deserialize(in);
      } else if (provider instanceof MutableRoaringBitmap) {
        ((MutableRoaringBitmap) provider).deserialize(in);
      } else {
        throw new UnsupportedEncodingException("Cannot deserialize a " + provider.getClass());
      }

      highToBitmap.put(high, provider);
    }

    resetPerfHelpers();
  }

  @Override
  public long serializedSizeInBytes() {
    long nbBytes = 0L;

    if (SERIALIZATION_MODE == SERIALIZATION_MODE_PORTABLE) {
      // .writeLong for number of different high values
      nbBytes += 8;
    } else {
      // .writeBoolean for signedLongs boolean
      nbBytes += 1;

      // .writeInt for number of different high values
      nbBytes += 4;
    }

    for (Entry<Integer, BitmapDataProvider> entry : highToBitmap.entrySet()) {
      // .writeInt for high
      nbBytes += 4;

      // The low bitmap size in bytes
      nbBytes += entry.getValue().serializedSizeInBytes();
    }

    return nbBytes;
  }

  /**
   * reset to an empty bitmap; result occupies as much space a newly created bitmap.
   */
  public void clear() {
    this.highToBitmap.clear();
    resetPerfHelpers();
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
  public static Roaring64NavigableMap bitmapOf(final long... dat) {
    final Roaring64NavigableMap ans = new Roaring64NavigableMap();
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
   * @deprecated as this may be confused with adding individual longs
   */
  @Deprecated
  public void add(final long rangeStart, final long rangeEnd) {
    addRange(rangeStart, rangeEnd);
  }

  /**
   * Add to the current bitmap all longs in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   */
  public void addRange(final long rangeStart, final long rangeEnd) {
    int startHigh = high(rangeStart);
    int startLow = low(rangeStart);

    int endHigh = high(rangeEnd);
    int endLow = low(rangeEnd);

    int compareHigh = compare(startHigh, endHigh);
    if (compareHigh > 0
        || compareHigh == 0 && Util.toUnsignedLong(startLow) >= Util.toUnsignedLong(endLow)) {
      throw new IllegalArgumentException("Invalid range [" + rangeStart + "," + rangeEnd + ")");
    }

    for (int high = startHigh; compare(high, endHigh) <= 0; high++) {
      final int currentStartLow;
      if (startHigh == high) {
        // The whole range starts in this bucket
        currentStartLow = startLow;
      } else {
        // Add the bucket from the beginning
        currentStartLow = 0;
      }

      long startLowAsLong = Util.toUnsignedLong(currentStartLow);

      final long endLowAsLong;
      if (endHigh == high) {
        // The whole range ends in this bucket
        endLowAsLong = Util.toUnsignedLong(endLow);
      } else {
        // Add the bucket until the end: we have a +1 as, in RoaringBitmap.add(long,long), the end
        // is excluded
        endLowAsLong = Util.toUnsignedLong(-1) + 1;
      }

      if (endLowAsLong > startLowAsLong) {
        // Initialize the bitmap only if there is access data to write
        BitmapDataProvider bitmap = highToBitmap.get(high);
        if (bitmap == null) {
          bitmap = newRoaringBitmap();
          pushBitmapForHigh(high, bitmap);
        }

        bitmap.add(startLowAsLong, endLowAsLong);
      }

      if (high == highestHigh()) {
        break;
      }
    }

    invalidateAboveHigh(startHigh);
  }

  @Override
  public LongIterator getReverseLongIterator() {
    return toIterator(highToBitmap.descendingMap().entrySet().iterator(), true);
  }

  @Override
  public void removeLong(long x) {
    int high = high(x);

    BitmapDataProvider bitmap = highToBitmap.get(high);

    if (bitmap != null) {
      int low = low(x);
      bitmap.remove(low);

      if (bitmap.isEmpty()) {
        // Remove the prefix from highToBitmap map
        highToBitmap.remove(high);
        latestAddedHigh = null;
      }

      // Invalidate only if actually modified
      invalidateAboveHigh(high);
    }
  }

  @Override
  public void trim() {
    for (BitmapDataProvider bitmap : highToBitmap.values()) {
      bitmap.trim();
    }
  }

  @Override
  public int hashCode() {
    return highToBitmap.hashCode();
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
    Roaring64NavigableMap other = (Roaring64NavigableMap) obj;
    return Objects.equals(highToBitmap, other.highToBitmap);
  }

  /**
   * clone current Roaring64NavigableMap instance, the new cloned instance will have
   * the same serialization mode with this one.
   * <p>
   * *NOTE* This can only handle instances where {@link #serializedSizeInBytes} &lt; Integer.MAX_VALUE,
   * otherwise an UnsupportedOperationException will be thrown.
   */
  @Override
  public Roaring64NavigableMap clone() {
    long sizeInBytesL = this.serializedSizeInBytes();
    if (sizeInBytesL >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException();
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(baos)) {
      serialize(dataOutput);
      Roaring64NavigableMap freshOne = new Roaring64NavigableMap();
      try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
          DataInputStream dataInput = new DataInputStream(bais)) {
        freshOne.deserialize(dataInput);
        return freshOne;
      } catch (Exception e) {
        throw new RuntimeException("fail to deserialize", e);
      }
    } catch (Exception e) {
      throw new RuntimeException("fail to clone through the ser/deser", e);
    }
  }

  /**
   * Add the value if it is not already present, otherwise remove it.
   *
   * @param x long value
   */
  public void flip(final long x) {
    int high = RoaringIntPacking.high(x);
    BitmapDataProvider lowBitmap = highToBitmap.get(high);
    if (lowBitmap == null) {
      // The value is not added: add it without any flip specific code
      addLong(x);
    } else {
      int low = RoaringIntPacking.low(x);

      // .flip is not in BitmapDataProvider contract
      // TODO Is it relevant to calling .flip with a cast?
      if (lowBitmap instanceof RoaringBitmap) {
        ((RoaringBitmap) lowBitmap).flip(low);
      } else if (lowBitmap instanceof MutableRoaringBitmap) {
        ((MutableRoaringBitmap) lowBitmap).flip(low);
      } else {
        // Fallback to a manual flip
        if (lowBitmap.contains(low)) {
          lowBitmap.remove(low);
        } else {
          lowBitmap.add(low);
        }
      }
    }

    invalidateAboveHigh(high);
  }

  private void assertNonEmpty() {
    if (isEmpty()) {
      throw new NoSuchElementException("Empty " + this.getClass().getSimpleName());
    }
  }

  @Override
  public long first() {
    assertNonEmpty();
    Map.Entry<Integer, BitmapDataProvider> firstEntry = highToBitmap.firstEntry();
    return RoaringIntPacking.pack(firstEntry.getKey(), firstEntry.getValue().first());
  }

  @Override
  public long last() {
    assertNonEmpty();
    Map.Entry<Integer, BitmapDataProvider> lastEntry = highToBitmap.lastEntry();
    return RoaringIntPacking.pack(lastEntry.getKey(), lastEntry.getValue().last());
  }
}
