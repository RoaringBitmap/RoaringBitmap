package org.roaringbitmap;

public interface Allocator {

  /**
   * allocate a new array
   * @param size the size of the array
   * @return a newly allocated array which should be returned to the pool
   */
  char[] allocateChars(int size);

  /**
   * allocate a new array
   * @param size the size of the array
   * @return a newly allocated array which should be returned to the pool
   */
  long[] allocateLongs(int size);

  /**
   * Allocate a new array with the element type
   * @param clazz the type of the elements
   * @param size the size of the array
   * @param <T> the type of the elements
   * @return a newly allocated array which should be returned to the pool
   */
  <T> T[] allocateObjects(Class<T> clazz, int size);

  /**
   * Copies the array, does not free the input
   * @param longs the array to be copied but not freed
   * @return a newly allocated array which should be returned to the pool
   */
  long[] copy(long[] longs);

  /**
   * Copies the array, does not free the input
   * @param chars the array to be copied but not freed
   * @return a newly allocated array which should be returned to the pool
   */
  char[] copy(char[] chars);

  /**
   * Copies the array, does not free the input
   * @param chars the array to be copied but not freed
   * @param newSize the size of the new array
   * @return a newly allocated array which should be returned to the pool
   */
  char[] copy(char[] chars, int newSize);

  /**
   * Copies the array, does not free the input
   * @param objects to be copied but not freed
   * @param <T> the type of the elements
   * @return a newly allocated array which should be returned to the pool
   */
  <T> T[] copy(T[] objects);

  /**
   * Copies the array, does not free the input
   * @param objects to be copied but not freed
   * @param <T> the type of the elements
   * @param newSize the size of the new array
   * @return a newly allocated array which should be returned to the pool
   */
  <T> T[] copy(T[] objects, int newSize);

  /**
   * Copy and free the array, return a new array
   * @param chars array which will be freed
   * @param newSize the size of the new array
   * @return a newly allocated array which should be returned to the pool
   */
  char[] extend(char[] chars, int newSize);

  /**
   * Copy and free the array, return a new array
   * @param objects objects which will be freed
   * @param newSize the size of the newly allocated array
   * @param <T> the type
   * @return a newly allocated array which should be returned to the pool
   */
  <T> T[] extend(T[] objects, int newSize);

  /**
   * free the object
   * @param object the object to free, must not be used afterwards.
   */
  void free(Object object);
}
