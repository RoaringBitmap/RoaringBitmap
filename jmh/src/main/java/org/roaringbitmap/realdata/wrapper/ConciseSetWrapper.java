package org.roaringbitmap.realdata.wrapper;


import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.DataOutputStream;
import java.io.IOException;

public final class ConciseSetWrapper implements Bitmap {

   private final ConciseSet bitmap;

   ConciseSetWrapper(ConciseSet bitmap) {
      this.bitmap = bitmap;
   }

   @Override
   public boolean contains(int i) {
      return bitmap.contains(i);
   }

   @Override
   public int last() {
      return bitmap.last();
   }

   @Override
   public void serialize(DataOutputStream dos) throws IOException {
      throw new UnsupportedOperationException();
   }

}
