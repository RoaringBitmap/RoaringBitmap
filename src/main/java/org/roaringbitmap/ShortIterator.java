/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * Iterator over short values.  
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
     * @return next short value as int value (using the least significant 16 bits)
     */
    int nextAsInt();
    
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
