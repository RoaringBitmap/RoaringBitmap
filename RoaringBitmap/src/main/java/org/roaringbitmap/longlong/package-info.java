/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */



/**
 * The org.roaringbitmap.longlong package  provides
 * one class ({@link org.roaringbitmap.longlong.Roaring64NavigableMap}) that   users
 * can rely upon for fast set of 64-bit integers.
 * 
 * 
 * <pre>
 * {@code
 *      import org.roaringbitmap.longlong.*;
 *
 *      //...
 *
 *      Roaring64NavigableMap r1 = new Roaring64NavigableMap();
 *      for(long k = 4000l; k<4255l;++k) r1.addLong(k);
 *      
 * }
 * </pre>
 *
 */
package org.roaringbitmap.longlong;

