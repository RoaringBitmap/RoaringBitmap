package org.roaringbitmap.pool;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Non thread-safe dummy object pool that uses a queue
 *
 * @param <T> The type to store in the queue
 */
public class SimpleObjectPool<T> {

  private final IObjectFactory<T> objectFactory;
  private final Queue<T> objectQueue;

  public SimpleObjectPool(IObjectFactory<T> objectFactory) {
    this.objectFactory = objectFactory;
    this.objectQueue = new ArrayDeque<>();
  }

  public int getSize() {
    return objectQueue.size();
  }

  /**
   * Gets an existing object if possible, creates it otherwise
   */
  public T borrowObject() {
    T freeObject = objectQueue.poll();

    // if it is not valid destroy it and re-poll from queue
    while (freeObject != null && !objectFactory.validate(freeObject)) {
      objectFactory.destroy(freeObject);
      freeObject = objectQueue.poll();
    }

    // create a new one if not found
    if (freeObject == null) {
      freeObject = objectFactory.create();
    }
    return freeObject;
  }

  /**
   * Clears the pool from objects
   */
  public void clear() {
    while (!objectQueue.isEmpty()) {
      T obj = objectQueue.poll();
      if (obj != null) {
        objectFactory.destroy(obj);
      }
    }
  }

  /**
   * Returns an object to the pool for future reuse
   */
  public void returnObject(T obj) {
    if (!objectQueue.offer(obj)) {
      objectFactory.destroy(obj);
    }
  }
}
