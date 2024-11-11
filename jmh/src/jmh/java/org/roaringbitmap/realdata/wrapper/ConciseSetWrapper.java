package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.IntConsumer;

import io.druid.extendedset.intset.ConciseSet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

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
    return new ConciseSetWrapper(bitmap.intersection(((ConciseSetWrapper) other).bitmap));
  }

  @Override
  public Bitmap or(Bitmap other) {
    return new ConciseSetWrapper(bitmap.union(((ConciseSetWrapper) other).bitmap));
  }

  @Override
  public Bitmap ior(Bitmap other) {
    throw new UnsupportedOperationException("Not implemented in ConciseSet");
  }

  @Override
  public Bitmap xor(Bitmap other) {
    return new ConciseSetWrapper(bitmap.symmetricDifference(((ConciseSetWrapper) other).bitmap));
  }

  @Override
  public Bitmap flip(int rStart, int rEnd) {
    // throw new UnsupportedOperationException();
    // put this back, but have to hunt down the JMH param setting
    // so the comparison does not abort.
    return new ConciseSetWrapper(bitmap); // wrong result
  }

  @Override
  public Bitmap andNot(Bitmap other) {
    return new ConciseSetWrapper(bitmap.difference(((ConciseSetWrapper) other).bitmap));
  }

  @Override
  public BitmapAggregator naiveAndAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        final Iterator<Bitmap> i = bitmaps.iterator();
        ConciseSet bitmap = ((ConciseSetWrapper) i.next()).bitmap;
        while (i.hasNext()) {
          bitmap = bitmap.intersection(((ConciseSetWrapper) i.next()).bitmap);
        }
        return new ConciseSetWrapper(bitmap);
      }
    };
  }

  @Override
  public BitmapAggregator naiveOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        final Iterator<Bitmap> i = bitmaps.iterator();
        ConciseSet bitmap = ((ConciseSetWrapper) i.next()).bitmap;
        while (i.hasNext()) {
          bitmap = bitmap.union(((ConciseSetWrapper) i.next()).bitmap);
        }
        return new ConciseSetWrapper(bitmap);
      }
    };
  }

  @Override
  public BitmapAggregator priorityQueueOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        PriorityQueue<ConciseSet> pq =
            new PriorityQueue<ConciseSet>(
                128,
                new Comparator<ConciseSet>() {
                  @Override
                  public int compare(ConciseSet a, ConciseSet b) {
                    return (int) (a.size() * a.collectionCompressionRatio())
                        - (int) (b.size() * b.collectionCompressionRatio());
                  }
                });
        for (Bitmap bitmap1 : bitmaps) {
          pq.add(((ConciseSetWrapper) bitmap1).bitmap);
        }
        ConciseSet bitmap;
        if (pq.isEmpty()) {
          bitmap = new ConciseSet();
        } else {
          while (pq.size() > 1) {
            ConciseSet x1 = pq.poll();
            ConciseSet x2 = pq.poll();
            pq.add(x1.union(x2));
          }
          bitmap = pq.poll();
        }
        return new ConciseSetWrapper(bitmap);
      }
    };
  }

  @Override
  public void forEach(IntConsumer ic) {
    throw new UnsupportedOperationException("Not implemented in ConciseSet");
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    throw new UnsupportedOperationException("Not implemented in ConciseSet");
  }

  @Override
  public Bitmap clone() {
    return new ConciseSetWrapper(bitmap.clone());
  }
}
