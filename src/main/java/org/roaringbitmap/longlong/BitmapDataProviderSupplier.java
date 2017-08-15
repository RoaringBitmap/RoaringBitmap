package org.roaringbitmap.longlong;

import java.io.Serializable;

import org.roaringbitmap.BitmapDataProvider;

/**
 * Enable customizing the {@link BitmapDataProvider} used by {@link Roaring64NavigableMap}
 * 
 * @author Benoit Lacelle
 *
 */
public interface BitmapDataProviderSupplier extends Serializable {
  BitmapDataProvider newEmpty();
}
