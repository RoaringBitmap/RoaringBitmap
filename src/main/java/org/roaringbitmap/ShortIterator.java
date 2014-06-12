/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * Iterator over short values
 */
public interface ShortIterator  extends Cloneable {
    /**
     * @return whether there is another value
     */
    boolean hasNext();

    /**
     * @return next short value
     */
    short next();
    

    /**
     * Creates a copy of the iterator.
     * 
     * @return a clone of the current iterator
     */
    ShortIterator clone();
    
    /**
     * If possible, remove the current value
     */
    void remove();
}
