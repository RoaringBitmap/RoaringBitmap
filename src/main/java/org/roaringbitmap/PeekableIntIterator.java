package org.roaringbitmap;


/**
 * Simple extension to the IntIterator interface. 
 * It allows you to "skip" values using the advanceIfNeeded
 * method, and to look at the value without advancing (peekNext).
 *
 */
public interface PeekableIntIterator extends IntIterator {
  /**
   * If needed, advance as long as the next value is smaller than minval
   * 
   * @param minval threshold
   */
  public void advanceIfNeeded(int minval);

  /**
   * 
   * Look at the next value without advancing
   * 
   * @return next value
   */
  public int peekNext();
  
  /**
   * Creates a copy of the iterator.
   * 
   * @return a clone of the current iterator
   */
  @Override
  PeekableIntIterator clone();
}


