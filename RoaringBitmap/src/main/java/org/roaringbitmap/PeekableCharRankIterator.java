package org.roaringbitmap;

/**
 * PeekableCharIterator that calculates the next value rank during iteration
 */
public interface PeekableCharRankIterator extends PeekableCharIterator {

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
  PeekableCharRankIterator clone();
}
