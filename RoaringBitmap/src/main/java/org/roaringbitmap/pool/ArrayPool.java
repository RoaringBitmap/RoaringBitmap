package org.roaringbitmap.pool;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Non thread safe implementation of a pool of arrays of different sizes
 */
public abstract class ArrayPool<T> {

  //Keyed object pool mapping the size to the pool of objects
  private final Map<Integer, SimpleObjectPool<T>> keyedObjectPool;
  private final Function<Integer, SimpleObjectPool<T>> createPoolFunction;

  /**
   * @param createObject the function that, given a size, creates an array of type T
   */
  public ArrayPool(Function<Integer, T> createObject) {
    this.keyedObjectPool = new HashMap<>();
    this.createPoolFunction = integer -> new SimpleObjectPool<>(new IObjectFactory<T>() {
      @Override
      public T create() {
        return createObject.apply(integer);
      }

      @Override
      public void destroy(T t) {
        //Do nothing
      }

      @Override
      public boolean validate(T t) {
        reset(t);
        return getSize(t) == integer;
      }
    });
  }

  /**
   * Requests an object of specified size.
   *
   * @return An object having the requested size
   */
  public T take(int size) {
    try {
      return keyedObjectPool.computeIfAbsent(size, createPoolFunction).borrowObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Frees the current object once done from being used. This object will be added to the pool for
   * future reuse
   *
   * @param current current object to return to the pool
   */
  public void free(T current) {
    try {
      keyedObjectPool.get(getSize(current)).returnObject(current);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the size of the provided object
   */
  protected abstract int getSize(T current);

  /**
   * Returns the size of the provided object
   */
  protected abstract void reset(T current);
}
