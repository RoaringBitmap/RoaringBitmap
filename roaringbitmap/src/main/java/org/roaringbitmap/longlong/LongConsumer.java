/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.longlong;

/**
 * An LongConsumer receives the long values contained in a data structure. Each value is visited
 * once.
 * 
 * Usage:
 * 
 * <pre>
 * {@code
 *  bitmap.forEach(new LongConsumer() {
 *
 *    public void accept(long value) {
 *      // do something here
 *      
 *    }});
 *   }
 * }
 * </pre>
 */
public interface LongConsumer {
  /**
   * Receives the long
   * 
   * @param value the long value
   */
  void accept(long value);
}
