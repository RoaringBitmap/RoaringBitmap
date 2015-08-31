package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.IOException;

public final class ImmutableRoaringBitmapWrapper implements Bitmap {

   private final ImmutableRoaringBitmap bitmap;

   ImmutableRoaringBitmapWrapper(ImmutableRoaringBitmap bitmap) {
      this.bitmap = bitmap;
   }

   public boolean contains(int i) {
      return bitmap.contains(i);
   }

   @Override
   public int last() {
      return bitmap.getReverseIntIterator().next();
   }

   @Override
   public void serialize(DataOutputStream dos) throws IOException {
      bitmap.serialize(dos);
   }

}
