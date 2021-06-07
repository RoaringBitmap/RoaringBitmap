package org.roaringbitmap;

/**
 * A consumer interface to process ranges of value contained in a bitmap using relative offsets.
 * <p>
 * All positions are relative offsets to a start position given as an argument to methods
 * that expect a range consumer. In other words, the bitmap global index for every position
 * in the methods provided by this interface is start + relativePos.
 * (For 64-bit bitmaps start may be a long and so would thus start + relativePos.)
 * <p>
 * A "present" value at a global position pos is one where bitmap.contains(pos) == true.
 * An "absent" value at a global position pos is one where bitmap.contains(pos) == false.
 */
public interface RelativeRangeConsumer {
  /**
   * Consume a single present value at relativePos.
   */
  void acceptPresent(int relativePos);

  /**
   * Consume a single absent value at relativePos.
   */
  void acceptAbsent(int relativePos);

  /**
   * Consume consecutive present values in the range [relativeFrom, relativeTo).
   */
  void acceptAllPresent(int relativeFrom, int relativeTo);

  /**
   * Consume consecutive absent values in the range [relativeFrom, relativeTo).
   */
  void acceptAllAbsent(int relativeFrom, int relativeTo);
}
