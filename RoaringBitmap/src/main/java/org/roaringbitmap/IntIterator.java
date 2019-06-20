/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * A simple iterator over integer values.
 * Using an IntIterator instead of Java's Iterator&lt;Integer&gt;
 * avoids the overhead of the Interger class: on some tests,
 * IntIterator is nearly twice as fast as Iterator&lt;Integer&gt;.
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
