package org.roaringbitmap.realdata.wrapper;


import it.uniroma3.mat.extendedset.intset.IntSet.IntIterator;

final class ConciseSetIteratorWrapper implements BitmapIterator {

   private final IntIterator iterator;

   ConciseSetIteratorWrapper(IntIterator iterator) {
      this.iterator = iterator;
   }

   @Override
   public boolean hasNext() {
      return iterator.hasNext();
   }

   @Override
   public int next() {
      return iterator.next();
   }

}
