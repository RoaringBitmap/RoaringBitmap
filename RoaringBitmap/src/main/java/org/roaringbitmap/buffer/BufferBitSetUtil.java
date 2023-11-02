package org.roaringbitmap.buffer;


import org.roaringbitmap.BitSetUtil;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.util.BitSet;


/***
 *
 * This class provides convenience functions to manipulate BitSet and MutableRoaringBitmap objects.
 *
 */
public class BufferBitSetUtil {
  // todo: add a method to convert an ImmutableRoaringBitmap to a BitSet using BitSet.valueOf

  // a block consists has a maximum of 1024 words, each representing 64 bits,
  // thus representing at maximum 65536 bits
  static final private int BLOCK_LENGTH = MappeableBitmapContainer.MAX_CAPACITY / Long.SIZE; //
  // 64-bit
  // word

  private static MappeableArrayContainer arrayContainerOf(final int from, final int to,
      final int cardinality, final long[] words) {
    char[] content = BitSetUtil.arrayContainerBufferOf(from, to, cardinality, words);
    return new MappeableArrayContainer(CharBuffer.wrap(content), cardinality);
  }


  /**
   * Generate a MutableRoaringBitmap out of a BitSet
   *
   * @param bitSet original bitset (will not be modified)
   * @return roaring bitmap equivalent to BitSet
   */
  public static MutableRoaringBitmap bitmapOf(final BitSet bitSet) {
    return bitmapOf(bitSet.toLongArray());
  }

  /**
   * Generate a MutableRoaringBitmap out of a long[], each long using little-endian representation
   * of its bits
   *
   * @see BitSet#toLongArray() for an equivalent
   * @param words array of longs (will not be modified)
   * @return roaring bitmap
   */
  public static MutableRoaringBitmap bitmapOf(final long[] words) {
    // split long[] into blocks.
    // each block becomes a single container, if any bit is set
    final MutableRoaringBitmap ans = new MutableRoaringBitmap();
    int containerIndex = 0;
    for (int from = 0; from < words.length; from += BLOCK_LENGTH) {
      final int to = Math.min(from + BLOCK_LENGTH, words.length);
      final int blockCardinality = cardinality(from, to, words);
      if (blockCardinality > 0) {
        ((MutableRoaringArray) ans.highLowContainer).insertNewKeyValueAt(containerIndex++,
            BufferUtil.highbits(from * Long.SIZE),
            BufferBitSetUtil.containerOf(from, to, blockCardinality, words));
      }
    }
    return ans;
  }

  /**
   * Efficiently generate a RoaringBitmap from an uncompressed byte array or ByteBuffer
   * This method tries to minimise all kinds of memory allocation
   *
   * @param bb the uncompressed bitmap
   * @return roaring bitmap
   */
  public static MutableRoaringBitmap bitmapOf(ByteBuffer bb) {
    return bitmapOf(bb, new long[BLOCK_LENGTH]);
  }

  /**
   * Efficiently generate a RoaringBitmap from an uncompressed byte array or ByteBuffer
   * This method tries to minimise all kinds of memory allocation
   * <br>
   * You can provide a cached wordsBuffer for avoiding 8 KB of extra allocation on every call
   *   No reference is kept to the wordsBuffer, so it can be cached as a ThreadLocal
   *
   * @param bb the uncompressed bitmap
   * @param wordsBuffer buffer of length {@link BitSetUtil#BLOCK_LENGTH}
   * @return roaring bitmap
   */
  public static MutableRoaringBitmap bitmapOf(ByteBuffer bb, long[] wordsBuffer) {

    if (wordsBuffer.length != BLOCK_LENGTH) {
      throw new IllegalArgumentException("wordsBuffer length should be " + BLOCK_LENGTH);
    }

    bb = bb.slice().order(ByteOrder.LITTLE_ENDIAN);
    final MutableRoaringBitmap ans = new MutableRoaringBitmap();

    // split buffer into blocks of long[]
    int containerIndex = 0;
    int blockLength = 0, blockCardinality = 0, offset = 0;
    long word;
    while (bb.remaining() >= 8) {
      word = bb.getLong();

      // Add read long to block
      wordsBuffer[blockLength++] = word;
      blockCardinality += Long.bitCount(word);

      // When block is full, add block to bitmap
      if (blockLength == BLOCK_LENGTH) {
        // Each block becomes a single container, if any bit is set
        if (blockCardinality > 0) {
          ((MutableRoaringArray) ans.highLowContainer).insertNewKeyValueAt(containerIndex++,
              BufferUtil.highbits(offset), BufferBitSetUtil.containerOf(0, blockLength,
                  blockCardinality, wordsBuffer));
        }
        /*
            Offset can overflow when bitsets size is more than Integer.MAX_VALUE - 64
            It's harmless though, as it will happen after the last block is added
         */
        offset += (BLOCK_LENGTH * Long.SIZE);
        blockLength = blockCardinality = 0;
      }
    }

    if (bb.remaining() > 0) {
      // Read remaining (less than 8) bytes
      // We can do this in while loop also, it will probably slow things down a bit though
      word = 0;
      for (int remaining = bb.remaining(), j = 0; j < remaining; j++) {
        word |= (bb.get() & 0xffL) << (8 * j);
      }

      // Add last word to block, only if any bit is set
      if (word != 0) {
        wordsBuffer[blockLength++] = word;
        blockCardinality += Long.bitCount(word);
      }
    }

    // Add block to map, if any bit is set
    if (blockCardinality > 0) {
      ((MutableRoaringArray) ans.highLowContainer).insertNewKeyValueAt(containerIndex,
          BufferUtil.highbits(offset),
          BufferBitSetUtil.containerOf(0, blockLength, blockCardinality, wordsBuffer));
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


  private static MappeableContainer containerOf(final int from, final int to,
      final int blockCardinality, final long[] words) {
    // find the best container available
    if (blockCardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      // containers with DEFAULT_MAX_SIZE or less integers should be
      // ArrayContainers
      return arrayContainerOf(from, to, blockCardinality, words);
    } else {
      // otherwise use bitmap container
      long[] container = new long[BLOCK_LENGTH];
      System.arraycopy(words, from, container, 0, to - from);
      return new MappeableBitmapContainer(LongBuffer.wrap(container), blockCardinality);
    }
  }


  /**
   * Compares a RoaringBitmap and a BitSet. They are equal if and only if they contain the same set
   * of integers.
   *
   * @param bitset first object to be compared
   * @param bitmap second object to be compared
   * @return whether they are equal
   */
  public static boolean equals(final BitSet bitset, final ImmutableRoaringBitmap bitmap) {
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
