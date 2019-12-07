package org.roaringbitmap;


import java.util.Arrays;
import java.util.BitSet;


/***
 *
 * This class provides convenience functions to manipulate BitSet and RoaringBitmap objects.
 *
 */
public class BitSetUtil {
  // todo: add a method to convert a RoaringBitmap to a BitSet using BitSet.valueOf

  // a block consists has a maximum of 1024 words, each representing 64 bits,
  // thus representing at maximum 65536 bits
  static final private int BLOCK_LENGTH = BitmapContainer.MAX_CAPACITY / Long.SIZE; //
  // 64-bit
  // word

  private static ArrayContainer arrayContainerOf(final int from, final int to,
      final int cardinality, final long[] words) {
    // precondition: cardinality is max 4096
    final char[] content = new char[cardinality];
    int index = 0;

    for (int i = from, socket = 0; i < to; ++i, socket += Long.SIZE) {
      long word = words[i];
      while (word != 0) {
        long t = word & -word;
        content[index++] = (char) (socket + Long.bitCount(t - 1));
        word ^= t;
      }
    }
    return new ArrayContainer(content);
  }


  /**
   * Generate a RoaringBitmap out of a BitSet
   *
   * @param bitSet original bitset (will not be modified)
   * @return roaring bitmap equivalent to BitSet
   */
  public static RoaringBitmap bitmapOf(final BitSet bitSet) {
    return bitmapOf(bitSet.toLongArray());
  }

  /**
   * Generate a RoaringBitmap out of a long[], each long using little-endian representation of its
   * bits
   *
   * @see BitSet#toLongArray() for an equivalent
   * @param words array of longs (will not be modified)
   * @return roaring bitmap
   */
  public static RoaringBitmap bitmapOf(final long[] words) {
    // split long[] into blocks.
    // each block becomes a single container, if any bit is set
    final RoaringBitmap ans = new RoaringBitmap();
    int containerIndex = 0;
    for (int from = 0; from < words.length; from += BLOCK_LENGTH) {
      final int to = Math.min(from + BLOCK_LENGTH, words.length);
      final int blockCardinality = cardinality(from, to, words);
      if (blockCardinality > 0) {
        ans.highLowContainer.insertNewKeyValueAt(containerIndex++, Util.highbits(from * Long.SIZE),
            BitSetUtil.containerOf(from, to, blockCardinality, words));
      }
    }
    return ans;
  }

  private static int cardinality(final int from, final int to, final long[] words) {
    int sum = 0;
    for (int i = from; i < to; i++) {
      sum += Long.bitCount(words[i]);
    }
    return sum;
  }


  private static Container containerOf(final int from, final int to, final int blockCardinality,
      final long[] words) {
    // find the best container available
    if (blockCardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      // containers with DEFAULT_MAX_SIZE or less integers should be
      // ArrayContainers
      return arrayContainerOf(from, to, blockCardinality, words);
    } else {
      // otherwise use bitmap container
      return new BitmapContainer(Arrays.copyOfRange(words, from, from + BLOCK_LENGTH),
          blockCardinality);
    }
  }


  /**
   * Compares a RoaringBitmap and a BitSet. They are equal if and only if they contain the same set
   * of integers.
   *
   * @param bitset first object to be compared
   * @param bitmap second object to be compared
   * @return whether they are equals
   */
  public static boolean equals(final BitSet bitset, final RoaringBitmap bitmap) {
    if (bitset.cardinality() != bitmap.getCardinality()) {
      return false;
    }
    final IntIterator it = bitmap.getIntIterator();
    while (it.hasNext()) {
      int val = it.next();
      if (!bitset.get(val)) {
        return false;
      }
    }
    return true;
  }
}
