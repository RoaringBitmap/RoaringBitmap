/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.longlong;

/**
 * A simple iterator over long values. Using an IntIterator instead of Java's Iterator&lt;Long&gt;
 * avoids the overhead of the Long class: on some tests, LongIterator is nearly twice as fast as
 * Iterator&lt;Long&gt;.
 */
public interface LongIterator extends Cloneable {
  /**
   * Creates a copy of the iterator.
   * 
   * @return a clone of the current iterator
   */
  LongIterator clone();

  /**
   * @return whether there is another value
   */
  boolean hasNext();

  /**
   * @return next long value
   */
  long next();

}
