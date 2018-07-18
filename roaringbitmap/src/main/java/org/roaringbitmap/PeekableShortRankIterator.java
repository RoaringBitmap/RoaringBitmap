package org.roaringbitmap;

/**
 * PeekableShortIterator that calculates the next value rank during iteration
 */
public interface PeekableShortRankIterator extends PeekableShortIterator {

  /**
   * peek in-container rank of the next value
   * @return rank of the next value
   */
  short peekNextRank();

  @Override
  PeekableShortRankIterator clone();
}
