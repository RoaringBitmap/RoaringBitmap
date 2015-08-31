package org.roaringbitmap.realdata.wrapper;

import com.googlecode.javaewah32.EWAHCompressedBitmap32;

import java.io.DataOutputStream;
import java.io.IOException;

public final class Ewah32BitmapWrapper implements Bitmap {

   private final EWAHCompressedBitmap32 bitmap;

   Ewah32BitmapWrapper(EWAHCompressedBitmap32 bitmap) {
      this.bitmap = bitmap;
   }

   @Override
   public boolean contains(int i) {
      return bitmap.get(i);
   }

   @Override
   public int last() {
      return bitmap.reverseIntIterator().next();
   }

   @Override
   public void serialize(DataOutputStream dos) throws IOException {
      bitmap.serialize(dos);
   }

}
