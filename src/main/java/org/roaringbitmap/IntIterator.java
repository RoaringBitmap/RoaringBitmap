/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * A simple iterator over integer values
 */
public interface IntIterator {
    /**
     * @return whether there is another value
     */
    boolean hasNext();

    /**
     * @return next integer value
     */
    int next();

}
