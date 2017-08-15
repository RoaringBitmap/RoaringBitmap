package org.roaringbitmap.longlong;

import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class MutableRoaringBitmapSupplier implements BitmapDataProviderSupplier {
  private static final long serialVersionUID = -1332258289943674375L;

  @Override
  public BitmapDataProvider newEmpty() {
    return new MutableRoaringBitmap();
  }

}
