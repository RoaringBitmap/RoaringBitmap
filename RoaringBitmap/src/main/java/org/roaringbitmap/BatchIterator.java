package org.roaringbitmap;

public interface BatchIterator extends Cloneable {

  /**
   * Writes the next batch of integers onto the buffer,
   * and returns how many were written. Aims to fill
   * the buffer.
   * @param buffer - the target to write onto
   * @return how many values were written during the call.
   */
  int nextBatch(int[] buffer);

  /**
   * Returns true is there are more values to get.
   * @return whether the iterator is exhaused or not.
   */
  boolean hasNext();

  /**
   * Creates a copy of the iterator.
   *
   * @return a clone of the current iterator
   */
  BatchIterator clone();

  /**
   * Creates a wrapper around the iterator so it behaves like an IntIterator
   * @param buffer - array to buffer bits into (size 128-256 should be best).
   * @return the wrapper
   */
  default IntIterator asIntIterator(int[] buffer) {
    return new BatchIntIterator(this, buffer);
  }

  /**
   * If needed, advance as long as the next value is smaller than minval
   *
   *  The advanceIfNeeded method is used for performance reasons, to skip
   *  over unnecessary repeated calls to next.
   *
   *  Suppose for example that you wish to compute the intersection between
   *  an ordered list of integers (e.g., int[] x = {1,4,5}) and a
   *  BatchIterator.
   *  You might do it as follows...
   *     <pre><code>
   *     int[] buffer = new int[128];
   *     BatchIterator j = // get an iterator
   *     int val = // first value from my other data structure
   *     j.advanceIfNeeded(val);
   *     while ( j.hasNext() ) {
   *       int limit = j.nextBatch(buffer);
   *       for (int i = 0; i &lt; limit; i++) {
   *         if (buffer[i] == val) {
   *           // got it!
   *           // do something here
   *           val = // get next value?
   *         }
   *       }
   *       j.advanceIfNeeded(val);
   *     }
   *     </code></pre>
   *
   *  The benefit of calling advanceIfNeeded is that each such call
   *  can be much faster than repeated calls to "next". The underlying
   *  implementation can "skip" over some data.
   *
   *
   * @param target threshold
   */
  void advanceIfNeeded(int target);


}
