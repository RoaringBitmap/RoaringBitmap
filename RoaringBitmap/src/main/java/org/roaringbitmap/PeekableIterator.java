package org.roaringbitmap;

import java.util.Iterator;

/**
 * Simple extension to the standard Iterator interface.
 * It allows you to "skip" values using the advanceIfNeeded
 * method, and to look at the value without advancing (peekNext).
 */
public interface PeekableIterator<E extends Comparable<? super E>> extends Iterator<E> {

    /**
     * If needed, advance as long as the next value is smaller than minval
     *
     *  The advanceIfNeeded method is used for performance reasons, to skip
     *  over unnecessary repeated calls to next.
     *
     *  The benefit of calling advanceIfNeeded is that each such call
     *  can be much faster than repeated calls to "next". The underlying
     *  implementation can "skip" over some data.
     *
     *
     * @param minval threshold
     */
    void advanceIfNeeded(E minval);

    /**
     * Look at the next value without advancing
     *
     * @return next value
     */
    E peekNext();
}
