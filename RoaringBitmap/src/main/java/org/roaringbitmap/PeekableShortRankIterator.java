package org.roaringbitmap;

/**
 * PeekableShortIterator that calculates the next value rank during iteration
 */
public interface PeekableShortRankIterator extends PeekableShortIterator {

  /**
   * peek in-container rank of the next value
   *
   * Uses integer because internal representation of rank is int
   * and in-container rank lies in range 1-65536
   *
   * @return rank of the next value
   */
  int peekNextRank();

  @Override
  PeekableShortRankIterator clone();
}
