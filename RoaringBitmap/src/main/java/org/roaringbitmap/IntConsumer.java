package org.roaringbitmap;

/**
 * An IntConsumer receives the int values contained in a data structure.
 * Each value is visited once.
 * 
 * Usage:
 * 
 * <pre>
 * {@code
 *  bitmap.forEach(new IntConsumer() {
 *
 *    public void accept(int value) {
 *      // do something here
 *      
 *    }});
 *   }
 * }
 * </pre>
 */
public interface IntConsumer {
  /**
   * Receives the integer
   * 
   * @param value the integer value
   */
  void accept(int value);
}
