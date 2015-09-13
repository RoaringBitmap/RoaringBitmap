package org.roaringbitmap.realdata.wrapper;


import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import static it.uniroma3.mat.extendedset.intset.ImmutableConciseSet.intersection;
import static it.uniroma3.mat.extendedset.intset.ImmutableConciseSet.union;

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
   public Bitmap or(Bitmap other) {
      return new ImmutableConciseSetWrapper(union(bitmap, ((ImmutableConciseSetWrapper) other).bitmap));
   }

   @Override
   public Bitmap xor(Bitmap other) {
      throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
   }

   @Override
   public Bitmap andNot(Bitmap other) {
      throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
   }

   @Override
   public BitmapAggregator naiveOrAggregator() {
      return new BitmapAggregator() {
         @Override
         public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
             final Iterator<Bitmap> i = bitmaps.iterator();
             ImmutableConciseSet bitmap = ((ImmutableConciseSetWrapper) i.next()).bitmap;
             while(i.hasNext()) {
                 bitmap = ImmutableConciseSet.union(bitmap, ((ImmutableConciseSetWrapper) i.next()).bitmap);
             }
             return new ImmutableConciseSetWrapper(bitmap);
         }
      };
   }

   @Override
   public void serialize(DataOutputStream dos) throws IOException {
      throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
   }

}
