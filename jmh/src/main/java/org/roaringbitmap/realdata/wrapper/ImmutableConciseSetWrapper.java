package org.roaringbitmap.realdata.wrapper;


import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.io.DataOutputStream;
import java.io.IOException;

import static it.uniroma3.mat.extendedset.intset.ImmutableConciseSet.intersection;

final class ImmutableConciseSetWrapper implements Bitmap {

   private final ImmutableConciseSet bitmap;

   ImmutableConciseSetWrapper(ImmutableConciseSet bitmap) {
      this.bitmap = bitmap;
   }

   @Override
   public boolean contains(int i) {
      throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
   }

   @Override
   public int last() {
      return bitmap.getLast();
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
      throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
   }

   @Override
   public Bitmap and(Bitmap other) {
      return new ImmutableConciseSetWrapper(intersection(bitmap, ((ImmutableConciseSetWrapper) other).bitmap));
   }

   @Override
   public void serialize(DataOutputStream dos) throws IOException {
      throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
   }

}
