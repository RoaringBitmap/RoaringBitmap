package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

final class RoaringBitmapWrapper implements Bitmap {

  private final RoaringBitmap bitmap;

  RoaringBitmapWrapper(RoaringBitmap bitmap) {
    this.bitmap = bitmap;
  }

  @Override
  public boolean contains(int i) {
    return bitmap.contains(i);
  }

  @Override
  public int last() {
    return bitmap.getReverseIntIterator().next();
  }

  @Override
  public int cardinality() {
    return bitmap.getCardinality();
  }

  @Override
  public BitmapIterator iterator() {
    return new RoaringIteratorWrapper(bitmap.getIntIterator());
  }

  @Override
  public BitmapIterator reverseIterator() {
    return new RoaringIteratorWrapper(bitmap.getReverseIntIterator());
  }

  @Override
  public Bitmap and(Bitmap other) {
    return new RoaringBitmapWrapper(
        RoaringBitmap.and(bitmap, ((RoaringBitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap or(Bitmap other) {
    return new RoaringBitmapWrapper(
        RoaringBitmap.or(bitmap, ((RoaringBitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap ior(Bitmap other) {
    bitmap.or(((RoaringBitmapWrapper) other).bitmap);
    return this;
  }

  @Override
  public Bitmap flip(int rangeStart, int rangeEnd) {
    return new RoaringBitmapWrapper(RoaringBitmap.flip(bitmap, (long) rangeStart, (long) rangeEnd));
  }

  @Override
  public Bitmap xor(Bitmap other) {
    return new RoaringBitmapWrapper(
        RoaringBitmap.xor(bitmap, ((RoaringBitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap andNot(Bitmap other) {
    return new RoaringBitmapWrapper(
        RoaringBitmap.andNot(bitmap, ((RoaringBitmapWrapper) other).bitmap));
  }

  @Override
  public BitmapAggregator naiveAndAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        Iterator<RoaringBitmap> iterator = toRoaringBitmapIterator(bitmaps);
        return new RoaringBitmapWrapper(FastAggregation.naive_and(iterator));
      }
    };
  }

  @Override
  public BitmapAggregator naiveOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(final Iterable<Bitmap> bitmaps) {
        Iterator<RoaringBitmap> iterator = toRoaringBitmapIterator(bitmaps);
        return new RoaringBitmapWrapper(FastAggregation.naive_or(iterator));
      }
    };
  }

  @Override
  public BitmapAggregator priorityQueueOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(final Iterable<Bitmap> bitmaps) {
        Iterator<RoaringBitmap> iterator = toRoaringBitmapIterator(bitmaps);
        return new RoaringBitmapWrapper(FastAggregation.priorityqueue_or(iterator));
      }
    };
  }

  private Iterator<RoaringBitmap> toRoaringBitmapIterator(final Iterable<Bitmap> bitmaps) {
    return new Iterator<RoaringBitmap>() {
      final Iterator<Bitmap> i = bitmaps.iterator();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public RoaringBitmap next() {
        return ((RoaringBitmapWrapper) i.next()).bitmap;
      }
    };
  }

  @Override
  public void forEach(IntConsumer ic) {
    bitmap.forEach(ic);
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    bitmap.serialize(dos);
  }

  @Override
  public Bitmap clone() {
    return new RoaringBitmapWrapper(bitmap.clone());
  }
}
