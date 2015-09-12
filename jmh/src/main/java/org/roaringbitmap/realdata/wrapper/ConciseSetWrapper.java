package org.roaringbitmap.realdata.wrapper;


import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.DataOutputStream;
import java.io.IOException;

final class ConciseSetWrapper implements Bitmap {

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
   public int cardinality() {
      return bitmap.size();
   }

   @Override
   public BitmapIterator iterator() {
      return new ConciseSetIteratorWrapper(bitmap.iterator());
   }

   @Override
   public BitmapIterator reverseIterator() {
      return new ConciseSetIteratorWrapper(bitmap.descendingIterator());
   }

   @Override
   public Bitmap and(Bitmap other) {
      return new ConciseSetWrapper(bitmap.intersection(((ConciseSetWrapper)other).bitmap));
   }

   @Override
   public Bitmap or(Bitmap other) {
      return new ConciseSetWrapper(bitmap.union(((ConciseSetWrapper)other).bitmap));
   }

   @Override
   public void serialize(DataOutputStream dos) throws IOException {
      throw new UnsupportedOperationException("Not implemented in ConciseSet");
   }

}
