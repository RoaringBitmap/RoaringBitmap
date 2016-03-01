/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * A simple iterator over integer values
 */
public interface IntIterator extends Cloneable {
  /**
   * Creates a copy of the iterator.
   * 
   * @return a clone of the current iterator
   */
  IntIterator clone();

  /**
   * @return whether there is another value
   */
  boolean hasNext();

  /**
   * @return next integer value
   */
  int next();

}
