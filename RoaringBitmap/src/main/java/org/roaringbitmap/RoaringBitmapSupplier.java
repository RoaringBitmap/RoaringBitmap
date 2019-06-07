/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

/**
 * A {@link BitmapDataProviderSupplier} providing {@link RoaringBitmap} as
 * {@link BitmapDataProvider}
 * 
 * @author Benoit Lacelle
 *
 */
public class RoaringBitmapSupplier implements BitmapDataProviderSupplier {

  @Override
  public BitmapDataProvider newEmpty() {
    return new RoaringBitmap();
  }

}
