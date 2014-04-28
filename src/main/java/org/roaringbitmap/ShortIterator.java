/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * Iterator over short values
 */
public interface ShortIterator {
    /**
     * @return whether there is another value
     */
    boolean hasNext();

    /**
     * @return next short value
     */
    short next();

    /**
     * remove current value
     */
    void remove();
}
