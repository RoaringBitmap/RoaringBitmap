package org.roaringbitmap;

/**
 * Key-value storage of 16 bit containers
 * @param <T> the type of stored container
 */
public interface AppendableStorage<T> {

  /**
   * Appends the key and container to the storage, throws if the key is less
   * than the current mark.
   * @param key the key to append
   * @param container the data to append
   */
  void append(char key, T container);

}
