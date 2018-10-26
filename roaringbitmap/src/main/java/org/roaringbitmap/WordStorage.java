package org.roaringbitmap;

public interface WordStorage<T> {

  T add(short value);

  boolean isEmpty();

  T runOptimize();

}
