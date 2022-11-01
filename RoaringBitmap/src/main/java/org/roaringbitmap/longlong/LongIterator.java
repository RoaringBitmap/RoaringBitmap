/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.longlong;

import java.util.PrimitiveIterator.OfLong;

/**
 * A simple iterator over long values. Using an IntIterator instead of Java's Iterator&lt;Long&gt;
 * avoids the overhead of the Long class: on some tests, LongIterator is nearly twice as fast as
 * Iterator&lt;Long&gt;.
 */
public interface LongIterator extends Cloneable, OfLong {
  /**
   * Creates a copy of the iterator.
   * 
   * @return a clone of the current iterator
   */
  LongIterator clone();
}
