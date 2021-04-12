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
   * If needed, 
   * when iterating forward through the chars it will
   * advance as long as the next value is smaller than val (as an unsigned
   * short)
   * when iterating in reverse through the chars it will
   * advance as long as the next value is larger than val (as an unsigned
   * short)
   * 
   * @param thresholdVal threshold
   */
  public void advanceIfNeeded(char thresholdVal);

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

