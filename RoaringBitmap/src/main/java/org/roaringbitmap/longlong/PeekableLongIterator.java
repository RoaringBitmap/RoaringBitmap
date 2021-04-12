/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.longlong;


/**
 * Simple extension to the IntIterator interface. 
 * It allows you to "skip" values using the advanceIfNeeded
 * method, and to look at the value without advancing (peekNext).
 *
 * This richer interface enables efficient algorithms over
 * iterators of longs.
 */
public interface PeekableLongIterator extends LongIterator {
  /**
   * If needed, 
   * 
   * for a forwards iterator 
   * advance as long as the next value is smaller than thresholdVal
   * 
   * For a reverse iterator 
   * advance as long as the next value is greater than thresholdVal
   *
   *  The advanceIfNeeded method is used for performance reasons, to skip
   *  over unnecessary repeated calls to next.
   *  
   *  Suppose for example that you wish to compute the intersection between
   *  an ordered list of longs (e.g., longs[] x = {1,4,5}) and a 
   *  PeekableIntIterator.
   *  You might do it as follows...
   *     <pre><code>
   *     PeekableLongIterator j = // get an iterator
   *     long val = // first value from my other data structure
   *     j.advanceIfNeeded(val);
   *     while ( j.hasNext() ) {
   *       if(j.next() == val) {
   *         // ah! ah! val is in the intersection...
   *         // do something here
   *         val = // get next value?
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
   * @param thresholdVal threshold
   */
  public void advanceIfNeeded(long thresholdVal);

  /**
   * 
   * Look at the next value without advancing
   * 
   * The peek is useful when working with several iterators at once.
   * Suppose that you have 100 iterators, and you want to compute
   * their intersections without materializing the result.
   * You might do it as follows... 
   *    <pre><code>
   *    PriorityQueue pq = new PriorityQueue(100,
   *      new Comparator&lt;PeekableIntIterator&gt;() {
   *             public int compare(PeekableIntIterator a,
   *                                PeekableIntIterator b) {
   *                 return a.peek() - b.peek();
   *             }
   *         });
   * 
   *    //...  populate pq
   *    
   *    while(! pq.isEmpty() ) {
   *      // get iterator with a smallest value
   *      PeekableLongIterator pi = pq.poll();
   *      long x = pi.next(); // advance
   *      // do something with x
   *      if(pi.hasNext()) pq.add(pi)
   *    }
   *    </code></pre>
   * 
   * Notice how the peek method allows you to compare iterators in a way
   * that the next method could not do.
   * 
   * @return next value
   */
  public long peekNext();

  /**
   * Creates a copy of the iterator.
   * 
   * @return a clone of the current iterator
   */
  @Override
  PeekableLongIterator clone();
}


