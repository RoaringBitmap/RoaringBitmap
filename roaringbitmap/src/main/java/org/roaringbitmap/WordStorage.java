package org.roaringbitmap;

public interface WordStorage<T> {

  T add(short value);

  boolean contains(short value);

  boolean isEmpty();

  int getCardinality();

  T runOptimize();

  int first();

  int last();

}
