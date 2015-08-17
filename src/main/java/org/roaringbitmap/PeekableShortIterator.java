/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;



/**
 * Simple extension to the ShortIterator interface
 *
 */
public interface PeekableShortIterator extends ShortIterator {
    /**
     * 
     * Look at the next value without advancing
     * @return next value
     */
    public short peekNext();
    /**
     * If needed, advance as long as the next value is greater or equal to minval (as an unsigned short)
     * @param minval threshold
     */
    public void advanceIfNeeded(short minval);
}

