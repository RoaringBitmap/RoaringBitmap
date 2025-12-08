package org.roaringbitmap;

import java.util.Objects;

/**
 * Succinct rank structure for RoaringBitmap.
 *
 * @author gerald.green
 * @since Dec-2025
 */
public class SuccinctRank {

  private static final int SMALL_BITMAP_THRESHOLD = 16;
  private static final int KEY_SPACE = 1 << 16;
  private static final int BITS_PER_WORD = 64;
  private static final int WORDS_PER_SUPERBLOCK = 8;
  private static final int SUPERBLOCK_COUNT = KEY_SPACE / (WORDS_PER_SUPERBLOCK * BITS_PER_WORD);
  private static final int WORDS_PER_BLOCK = 4;
  private static final int BLOCKS_PER_CONTAINER = 1024 / WORDS_PER_BLOCK;
  private static final int BITS_PER_PACKED_BLOCK = 9;
  private static final long BLOCK_MASK = (1L << BITS_PER_PACKED_BLOCK) - 1;

  // Lookup tables for bitmap container rank queries
  private static final int[] WORD_INDEX = new int[KEY_SPACE];
  private static final int[] CUMULATIVE_RANK_INDEX = new int[KEY_SPACE];
  private static final long[] RANK_BIT_MASK = new long[KEY_SPACE];

  static {
    for (int low = 0; low < KEY_SPACE; low++) {
      WORD_INDEX[low] = low >>> 6;
      CUMULATIVE_RANK_INDEX[low] = low >>> 8;
      final int bitInWord = low & 63;
      RANK_BIT_MASK[low] = (bitInWord == 63) ? -1L : (1L << (bitInWord + 1)) - 1;
    }
  }

  private final RoaringBitmap bitmap;
  private final long[] highBits; // null for small bitmaps
  private final long[] highRankCount; // null for small bitmaps
  private final long[] cumulativePerContainer;
  private final char[][] containerCumulativeRanks;

  private SuccinctRank(
      final RoaringBitmap bitmap,
      final long[] highBits,
      final long[] highRankCount,
      final long[] cumulativePerContainer,
      final char[][] containerCumulativeRanks) {
    this.bitmap = bitmap;
    this.highBits = highBits;
    this.highRankCount = highRankCount;
    this.cumulativePerContainer = cumulativePerContainer;
    this.containerCumulativeRanks = containerCumulativeRanks;
  }

  /**
   * Builds a rank structure for the given bitmap.
   * WARNING: Does not clone the bitmap - assumes it will not be modified.
   *
   * @param source the source bitmap
   * @return a rank structure
   */
  public static SuccinctRank build(final RoaringBitmap source) {
    Objects.requireNonNull(source, "source bitmap must not be null");

    final RoaringArray ra = source.highLowContainer;
    final int containerCount = ra.size();

    final long[] cumulativePerContainer = new long[containerCount + 1];
    final char[][] containerCumulativeRanks = new char[containerCount][];

    long acc = 0L;
    for (int i = 0; i < containerCount; i++) {
      final Container container = ra.getContainerAtIndex(i);
      acc += container.getCardinality();
      cumulativePerContainer[i + 1] = acc;

      if (container instanceof BitmapContainer) {
        containerCumulativeRanks[i] = buildCumulativeRanks((BitmapContainer) container);
      }
    }

    if (containerCount <= SMALL_BITMAP_THRESHOLD) {
      return new SuccinctRank(source, null, null, cumulativePerContainer, containerCumulativeRanks);
    }

    final long[] highBits = new long[KEY_SPACE / BITS_PER_WORD];
    for (int i = 0; i < containerCount; i++) {
      final int key = Util.lowbitsAsInteger(ra.getKeyAtIndex(i));
      highBits[key >>> 6] |= (1L << (key & 63));
    }

    final long[] highRankCount = buildHighKeyRankIndex(highBits);
    return new SuccinctRank(
        source, highBits, highRankCount, cumulativePerContainer, containerCumulativeRanks);
  }

  // Two-level rank index: superblock counts + packed block counts
  private static long[] buildHighKeyRankIndex(final long[] highBits) {
    final long[] count = new long[SUPERBLOCK_COUNT * 2];

    long cumulative = 0;
    int countPos = 0;

    for (int wordIdx = 0; wordIdx < highBits.length; wordIdx += WORDS_PER_SUPERBLOCK) {
      count[countPos] = cumulative;

      long packed = 0;
      long blockCumulative = Long.bitCount(highBits[wordIdx]);
      final int superblockLimit = Math.min(WORDS_PER_SUPERBLOCK, highBits.length - wordIdx);

      for (int j = 1; j < superblockLimit; j++) {
        packed |= (blockCumulative & BLOCK_MASK) << (BITS_PER_PACKED_BLOCK * (j - 1));
        blockCumulative += Long.bitCount(highBits[wordIdx + j]);
      }
      count[countPos + 1] = packed;

      cumulative += blockCumulative;
      countPos += 2;
    }

    return count;
  }

  private static char[] buildCumulativeRanks(final BitmapContainer bc) {
    final long[] bitmap = bc.bitmap;
    final char[] cumulativeRanks = new char[BLOCKS_PER_CONTAINER];

    int cumulative = 0;
    for (int block = 0; block < BLOCKS_PER_CONTAINER; block++) {
      cumulativeRanks[block] = (char) cumulative;
      final int baseWord = block * WORDS_PER_BLOCK;
      cumulative += Long.bitCount(bitmap[baseWord]);
      cumulative += Long.bitCount(bitmap[baseWord + 1]);
      cumulative += Long.bitCount(bitmap[baseWord + 2]);
      cumulative += Long.bitCount(bitmap[baseWord + 3]);
    }

    return cumulativeRanks;
  }

  /**
   * Returns the number of integers <= x.
   *
   * @param x upper limit
   * @return the rank
   */
  public long rank(final int x) {
    if (containerCount() == 0) {
      return 0L;
    }

    final int hi = Util.highbits(x);
    final char lo = Util.lowbits(x);

    final int containerIndex = this.highBits == null ? findContainerLinear(hi) : rank1High(hi) - 1;

    if (containerIndex < 0) {
      return 0L;
    }

    final RoaringArray ra = this.bitmap.highLowContainer;
    final int actualHi = Util.lowbitsAsInteger(ra.getKeyAtIndex(containerIndex));

    if (actualHi < hi) {
      return this.cumulativePerContainer[containerIndex + 1];
    }

    final int containerRank = containerRank(containerIndex, lo);
    return this.cumulativePerContainer[containerIndex] + containerRank;
  }

  private int findContainerLinear(final int hi) {
    final RoaringArray ra = this.bitmap.highLowContainer;
    int lastSmaller = -1;
    for (int i = 0; i < ra.size(); i++) {
      final int key = Util.lowbitsAsInteger(ra.getKeyAtIndex(i));
      if (key == hi) {
        return i;
      }
      if (key < hi) {
        lastSmaller = i;
      } else {
        break;
      }
    }
    return lastSmaller;
  }

  private int rank1High(int h) {
    if (h < 0) {
      return 0;
    }
    if (h >= KEY_SPACE) {
      h = KEY_SPACE - 1;
    }

    final int wordIndex = h >>> 6;
    final int bitInWord = h & 63;
    final int superblockIndex = wordIndex >>> 3;
    final int wordInSuperblock = wordIndex & 7;

    long rank = this.highRankCount[superblockIndex * 2];

    if (wordInSuperblock > 0) {
      final long packed = this.highRankCount[superblockIndex * 2 + 1];
      rank += (packed >>> (BITS_PER_PACKED_BLOCK * (wordInSuperblock - 1))) & BLOCK_MASK;
    }

    final long mask = (-1L) >>> (63 - bitInWord);
    rank += Long.bitCount(this.highBits[wordIndex] & mask);

    return (int) rank;
  }

  private int containerRank(final int containerIndex, final char lowKey) {
    final char[] cumulativeRanks = this.containerCumulativeRanks[containerIndex];

    if (cumulativeRanks != null) {
      return bitmapContainerFastRank(containerIndex, cumulativeRanks, lowKey);
    }

    return this.bitmap.highLowContainer.getContainerAtIndex(containerIndex).rank(lowKey);
  }

  private int bitmapContainerFastRank(
      final int containerIndex, final char[] cumulativeRanks, final char lo) {
    final int loInt = Util.lowbitsAsInteger(lo);
    final int wordIndex = WORD_INDEX[loInt];
    final BitmapContainer bc =
        (BitmapContainer) this.bitmap.highLowContainer.getContainerAtIndex(containerIndex);
    final long[] words = bc.bitmap;

    int rank = cumulativeRanks[CUMULATIVE_RANK_INDEX[loInt]];

    final int mod4 = wordIndex & 3;
    final int blockBase = wordIndex - mod4;

    switch (mod4) {
      case 3:
        rank += Long.bitCount(words[blockBase + 2]);
        // fall through
      case 2:
        rank += Long.bitCount(words[blockBase + 1]);
        // fall through
      case 1:
        rank += Long.bitCount(words[blockBase]);
        // fall through
      case 0:
      default:
        break;
    }

    final long lastWord = words[wordIndex] & RANK_BIT_MASK[loInt];
    if (lastWord != 0) {
      rank += Long.bitCount(lastWord);
    }

    return rank;
  }

  public long cardinality() {
    return this.cumulativePerContainer[containerCount()];
  }

  public RoaringBitmap snapshot() {
    return this.bitmap;
  }

  public boolean usesLinearScan() {
    return this.highBits == null;
  }

  public int containerCount() {
    return this.bitmap.highLowContainer.size();
  }
}
