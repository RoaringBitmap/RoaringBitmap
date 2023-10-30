package org.roaringbitmap.pool;

/**
 * Object factory used to create poolable objects
 */
public interface IObjectFactory<T> {

  /**
   * Creates a new poolable object
   */
  T create();

  /**
   * Destroys the poolable object
   */
  void destroy(T t);

  /**
   * Validates the object if it is still valid before putting it back in the pool
   */
  boolean validate(T t);
}
