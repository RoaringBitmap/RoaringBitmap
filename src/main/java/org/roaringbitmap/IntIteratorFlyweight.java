package org.roaringbitmap;

/**
 * This Iterator will let you go green and make your garbage collector suffer less.
 *
 * You can create one reusable instance of this class and then {@link #wrap(RoaringBitmap)}
 *
 * I am aware of the code duplication and the VeryLongNamingOfTheIterators but in order
 * to fix this it will take to make a lot of changes to the other Iterators in this project as well.
 *
 *
 *
 *
 *
 *
 * Here is the attached benchmark
 * public class RoaringIteratorBenchmark {



 public int  testStandard(RoaringState state) {

 IntIterator intIterator = state.bitmap.getIntIterator();
 int result =0;
 while(intIterator.hasNext()) {
 result = intIterator.next();

 }
 return result;

 }


 @Benchmark
 public int  testFlyweight(RoaringState state) {

 IntIteratorFlyweight intIterator = state.intIterator;

 intIterator.wrap(state.bitmap);

 int result =0;
 while(intIterator.hasNext()) {
 result = intIterator.next();

 }
 return result;

 }



 @State(Scope.Benchmark)
 public static class RoaringState {

 final Random source = new Random(0xcb000a2b9b5bdfb6l);
 final int[] data = takeSortedAndDistinct(source, 10000);

 final RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);

 final IntIteratorFlyweight intIterator = new IntIteratorFlyweight();



 private static int[] takeSortedAndDistinct(Random source, int count) {
 LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);
 for (int size = 0; size < count; size++) {
 int next;
 do {
 next = Math.abs(source.nextInt());
 } while (!ints.add(next));
 }
 int[] unboxed = Ints.toArray(ints);
 Arrays.sort(unboxed);
 return unboxed;
 }
 }




 }

  *
 *  Benchmark                                                      Mode    Samples    Score   Score error  Units
 b.c.b.t.b.roaringBitmap.RoaringIteratorBenchmark.testFlyweight    thrpt      200  17347.182      102.962  ops/s
 b.c.b.t.b.roaringBitmap.RoaringIteratorBenchmark.testStandard     thrpt      200  10302.179       53.562  ops/s
 *
 */

public class IntIteratorFlyweight implements IntIterator {

   private int hs;

   private ShortIterator iter;

   private ArrayContainerShortIteratorFlyweight arrIter = new ArrayContainerShortIteratorFlyweight();
   private BitmapContainerShortIteratorFlyweight bitmapIter = new BitmapContainerShortIteratorFlyweight();

   private int pos;

   private RoaringBitmap roaringBitmap = null;

   /**
    * Prepares a BitMap for iteration
    * @param r
    */
   public void wrap(RoaringBitmap r) {
      this.hs = 0;
      this.pos = 0;
      this.roaringBitmap = r;
      this.nextContainer();
   }

   @Override
   public boolean hasNext() {
      return pos < this.roaringBitmap.highLowContainer.size();
   }



   private void nextContainer() {
      if (pos < this.roaringBitmap.highLowContainer.size()) {

         Container container = this.roaringBitmap.highLowContainer.getContainerAtIndex(pos);

         if (container instanceof BitmapContainer) {
            bitmapIter.wrap((BitmapContainer) container);
            iter = bitmapIter;
         } else {
            arrIter.wrap((ArrayContainer) container);
            iter = arrIter;
         }

         hs = Util.toIntUnsigned(this.roaringBitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
      }
   }

   @Override
   public int next() {
      int x = Util.toIntUnsigned(iter.next()) | hs;
      if (!iter.hasNext()) {
         ++pos;
         nextContainer();
      }
      return x;
   }

   @Override
   public IntIterator clone() {
      try {
         IntIteratorFlyweight x = (IntIteratorFlyweight) super.clone();
         x.iter = this.iter.clone();
         return x;
      } catch (CloneNotSupportedException e) {
         return null;// will not happen
      }
   }

   public static void main(String[] args) {
      RoaringBitmap b = new RoaringBitmap();

      for (int i = 0; i < 100000; i++) {
         b.add(i);
      }

      IntIteratorFlyweight iterator = new IntIteratorFlyweight();

      iterator.wrap(b);

      for (int i : b) {
         System.out.println(b);
      }

   }

   public static class ArrayContainerShortIteratorFlyweight implements ShortIterator {
      int pos;

      private ArrayContainer arrayContainer;

      public void wrap(ArrayContainer c) {
         arrayContainer = c;
         pos = 0;
      }

      @Override
      public boolean hasNext() {
         return pos < arrayContainer.cardinality;
      }

      @Override
      public short next() {
         return arrayContainer.content[pos++];
      }

      @Override
      public ShortIterator clone() {
         try {
            return (ShortIterator) super.clone();
         } catch (CloneNotSupportedException e) {
            return null;// will not happen
         }
      }

      @Override
      public void remove() {
         arrayContainer.remove((short) (pos - 1));
         pos--;
      }

   }

   public static class BitmapContainerShortIteratorFlyweight implements ShortIterator {

      int i;
      int j;
      int max;

      BitmapContainer bitmapContainer = null;

      public void wrap(BitmapContainer c) {
         bitmapContainer = c;
         i = bitmapContainer.nextSetBit(0);
         max = bitmapContainer.bitmap.length * 64 - 1;

      }

      @Override
      public boolean hasNext() {
         return i >= 0;
      }

      @Override
      public short next() {
         j = i;
         i = i < max ? bitmapContainer.nextSetBit(i + 1) : -1;
         return (short) j;
      }

      @Override
      public ShortIterator clone() {
         try {
            return (ShortIterator) super.clone();
         } catch (CloneNotSupportedException e) {
            return null;// will not happen
         }
      }

      @Override
      public void remove() {
         //TODO: implement
         throw new RuntimeException("unsupported operation: remove");
      }
   }
}

