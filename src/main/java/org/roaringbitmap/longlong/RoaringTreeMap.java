package org.roaringbitmap.longlong;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

// this class is not thread-safe
// @Beta
public class RoaringTreeMap implements Externalizable {
  // Not final to enable initialization in Externalizable.readObject
  protected NavigableMap<Integer, MutableRoaringBitmap> highToBitmap = new TreeMap<>();

  // Prevent recomputing all cardinalities when requesting consecutive ranks
  private transient int firstHighNotValid = Integer.MIN_VALUE;

  // This boolean needs firstHighNotValid == Integer.MAX_VALUE to be allowed to be true
  // If false, it means nearly all cumulated cardinalities are valid, except high = Integer.MAX_VALUE
  // If true, it means all cumulated cardinalities are valid, even high = Integer.MAX_VALUE
  private transient boolean allValid = false;

  // TODO: I would prefer not managing arrays myself
  private transient long[] sortedCumulatedCardinality = new long[0];
  private transient int[] sortedHighs = new int[0];

  // Enable random-access to any bitmap, without requiring a new Iterator instance
  private transient final List<MutableRoaringBitmap> linkedBitmaps = new ArrayList<>();

  // Prevent indirection when writing consecutive Integers
  private transient Map.Entry<Integer, MutableRoaringBitmap> latest = null;

  // https://stackoverflow.com/questions/12772939/java-storing-two-ints-in-a-long
  public void addLong(long id) {
    int high = (int) (id >> 32);
    int low = (int) id;

    Map.Entry<Integer, MutableRoaringBitmap> local = latest;
    if (local != null && local.getKey().intValue() == high) {
      local.getValue().add(low);
    } else {
      MutableRoaringBitmap bitmap = highToBitmap.get(high);
      if (bitmap == null) {
        bitmap = new MutableRoaringBitmap();
        highToBitmap.put(high, bitmap);
      }
      bitmap.add(low);
      latest = new AbstractMap.SimpleImmutableEntry<>(high, bitmap);
    }

    // The cardinalities after this bucket may not be valid anymore
    firstHighNotValid = Math.min(firstHighNotValid, high);
    allValid = false;
  }

  private long pack(int high, int low) {
    return (((long) high) << 32) | (low & 0xffffffffL);
  }

  public long getCardinality() {
    if (highToBitmap.isEmpty()) {
      return 0L;
    }

    ensureCumulatives(Integer.MAX_VALUE);

    return sortedCumulatedCardinality[sortedCumulatedCardinality.length - 1];
  }

  /**
   * Return the jth value stored in this bitmap.
   *
   * @param j
   *            index of the value
   *
   * @return the value
   */
  public long select(final long j) {
    ensureCumulatives(Integer.MAX_VALUE);

    int position =
        Arrays.binarySearch(sortedCumulatedCardinality, 0, sortedCumulatedCardinality.length, j);

    if (position >= 0) {
      if (position == sortedCumulatedCardinality.length - 1) {
        // .select has been called on this.getCardinality
        return throwSelectInvalidIndex(j);
      }

      // There is a bucket leading to this cardinality: the j-th element is the first element of next bucket
      MutableRoaringBitmap nextBitmap = linkedBitmaps.get(position + 1);
      return pack(sortedHighs[position + 1], nextBitmap.first());
    } else {
      // There is no bucket with this cardinality
      int insertionPoint = -position - 1;

      final long previousBucketCardinality;
      if (insertionPoint == 0) {
        previousBucketCardinality = 0L;
      } else {
        previousBucketCardinality = sortedCumulatedCardinality[insertionPoint - 1];
      }

      // We get a 'select' query for a single bitmap: should fit in an int
      final int givenBitmapSelect = (int) (j - previousBucketCardinality);

      MutableRoaringBitmap bitmaps = linkedBitmaps.get(insertionPoint);

      int low = bitmaps.select(givenBitmapSelect);

      int high = sortedHighs[insertionPoint];

      return pack(high, low);
    }
  }

  private long throwSelectInvalidIndex(long j) {
    // see org.roaringbitmap.buffer.ImmutableRoaringBitmap.select(int)
    throw new IllegalArgumentException(
        "select " + j + " when the cardinality is " + this.getCardinality());
  }

  public LongIterator iterator() {
    final Iterator<Map.Entry<Integer, MutableRoaringBitmap>> it =
        highToBitmap.entrySet().iterator();

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
       * @param it
       * @return true if we MAY have more entries. false if there is definitely nothing more
       */
      private boolean moveToNextEntry(Iterator<Map.Entry<Integer, MutableRoaringBitmap>> it) {
        if (it.hasNext()) {
          Map.Entry<Integer, MutableRoaringBitmap> next = it.next();
          currentKey = next.getKey();
          currentIt = next.getValue().getIntIterator();

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
          return pack(currentKey, currentIt.next());
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

  public long rankLong(long id) {
    int high = (int) (id >> 32);
    int low = (int) id;

    ensureCumulatives(high);

    int bitmapPosition = Arrays.binarySearch(sortedHighs, 0, sortedHighs.length, high);

    if (bitmapPosition >= 0) {
      // There is a bucket holding this item

      final long previousBucketCardinality;
      if (bitmapPosition == 0) {
        previousBucketCardinality = 0;
      } else {
        previousBucketCardinality = sortedCumulatedCardinality[bitmapPosition - 1];
      }

      MutableRoaringBitmap bitmap = linkedBitmaps.get(bitmapPosition);

      // Rank is previous cardinality plus rank in current bitmap
      return previousBucketCardinality + bitmap.rankLong(low);
    } else {
      // There is no bucket holding this item: insertionPoint is previous bitmap
      int insertionPoint = -bitmapPosition - 1;

      if (insertionPoint == 0) {
        // this key is before all inserted keys
        return 0;
      } else {
        // The rank is the cardinality of this previous bitmap
        return sortedCumulatedCardinality[insertionPoint - 1];
      }
    }
  }

  protected void ensureCumulatives(int high) {
    // Check if missing data to handle this rank
    if (!allValid && firstHighNotValid <= high) {
      // For each deprecated buckets
      SortedMap<Integer, MutableRoaringBitmap> tailMap = highToBitmap.tailMap(firstHighNotValid);

      for (Map.Entry<Integer, MutableRoaringBitmap> e : tailMap.entrySet()) {
        int currentHigh = e.getKey();

        if (currentHigh > high) {
          break;
        }

        int index = Arrays.binarySearch(sortedHighs, 0, sortedHighs.length, currentHigh);

        if (index >= 0) {
          // This bitmap has already been registered
          MutableRoaringBitmap bitmap = e.getValue();
          assert bitmap == highToBitmap.get(sortedHighs[index]);

          final long previousCardinality;
          if (currentHigh >= 1) {
            previousCardinality = sortedCumulatedCardinality[index - 1];
          } else {
            previousCardinality = 0;
          }
          sortedCumulatedCardinality[index] = previousCardinality + bitmap.getCardinality();

          if (currentHigh == Integer.MAX_VALUE) {
            allValid = true;
            firstHighNotValid = currentHigh;
          } else {
            firstHighNotValid = currentHigh + 1;
          }
          if (currentHigh > high) {
            // No need to compute more than needed
            break;
          }
        } else {
          int insertionPosition = -index - 1;

          // This is a new key
          if (insertionPosition >= sortedHighs.length) {
            // Insertion at the end
            sortedHighs = Arrays.copyOf(sortedHighs, sortedHighs.length + 1);
            sortedCumulatedCardinality =
                Arrays.copyOf(sortedCumulatedCardinality, sortedCumulatedCardinality.length + 1);
          } else {
            // Insertion in the middle
            int previousLength = sortedHighs.length;
            sortedHighs = Arrays.copyOf(sortedHighs, previousLength + 1);
            // Ensure the new 0 is in the middle
            System.arraycopy(sortedHighs, insertionPosition, sortedHighs, insertionPosition + 1,
                previousLength - insertionPosition);

            sortedCumulatedCardinality =
                Arrays.copyOf(sortedCumulatedCardinality, sortedCumulatedCardinality.length + 1);

            // No need to copy higher cardinalities as anyway, the cardinalities may not be valid anymore
          }
          sortedHighs[insertionPosition] = currentHigh;
          linkedBitmaps.add(insertionPosition, e.getValue());

          final long previousCardinality;
          if (insertionPosition >= 1) {
            previousCardinality = sortedCumulatedCardinality[insertionPosition - 1];
          } else {
            previousCardinality = 0;
          }

          sortedCumulatedCardinality[insertionPosition] =
              previousCardinality + e.getValue().getLongCardinality();

          if (currentHigh == Integer.MAX_VALUE) {
            allValid = true;
            firstHighNotValid = currentHigh;
          } else {
            firstHighNotValid = currentHigh + 1;
          }
        }
      }
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(highToBitmap);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    highToBitmap = (NavigableMap<Integer, MutableRoaringBitmap>) in.readObject();
  }
}
