package org.roaringbitmap;

/**
 * PeekableIntIterator that calculates the next value rank during iteration
 */
public interface PeekableIntRankIterator extends PeekableIntIterator {
  /**
   * Look at rank of the next value without advancing
   *
   * @return rank of next value
   */
  int peekNextRank();

  @Override
  PeekableIntRankIterator clone();
}


