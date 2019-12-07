/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;



/**
 * Simple extension to the CharIterator interface
 *
 */
public interface PeekableCharIterator extends CharIterator {
  /**
   * If needed, advance as long as the next value is smaller than minval (as an unsigned
   * short)
   * 
   * @param minval threshold
   */
  public void advanceIfNeeded(char minval);

  /**
   * 
   * Look at the next value without advancing
   * 
   * @return next value
   */
  public char peekNext();
  
  /**
   * Creates a copy of the iterator.
   * 
   * @return a clone of the current iterator
   */
  @Override
  PeekableCharIterator clone();
}

