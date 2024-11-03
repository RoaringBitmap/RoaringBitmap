package org.roaringbitmap.realdata.wrapper;

import static io.druid.extendedset.intset.ImmutableConciseSet.intersection;
import static io.druid.extendedset.intset.ImmutableConciseSet.union;

import org.roaringbitmap.IntConsumer;

import io.druid.extendedset.intset.ImmutableConciseSet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

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
    return new ImmutableConciseSetWrapper(
        intersection(bitmap, ((ImmutableConciseSetWrapper) other).bitmap));
  }

  @Override
  public Bitmap flip(int s, int e) {
    ImmutableConciseSet temp = ImmutableConciseSet.complement(bitmap, e);
    if (e == 0) return new ImmutableConciseSetWrapper(temp);
    else return new ImmutableConciseSetWrapper(ImmutableConciseSet.complement(temp, e - 1));
  }

  @Override
  public Bitmap or(Bitmap other) {
    return new ImmutableConciseSetWrapper(
        union(bitmap, ((ImmutableConciseSetWrapper) other).bitmap));
  }

  @Override
  public Bitmap ior(Bitmap other) {
    throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
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
  public BitmapAggregator naiveAndAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        return new ImmutableConciseSetWrapper(intersection(toImmutableConciseSetIterator(bitmaps)));
      }
    };
  }

  @Override
  public BitmapAggregator naiveOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        return new ImmutableConciseSetWrapper(union(toImmutableConciseSetIterator(bitmaps)));
      }
    };
  }

  private Iterator<ImmutableConciseSet> toImmutableConciseSetIterator(
      final Iterable<Bitmap> bitmaps) {
    return new Iterator<ImmutableConciseSet>() {
      final Iterator<Bitmap> i = bitmaps.iterator();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public ImmutableConciseSet next() {
        return ((ImmutableConciseSetWrapper) i.next()).bitmap;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public BitmapAggregator priorityQueueOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        PriorityQueue<ImmutableConciseSet> pq =
            new PriorityQueue<ImmutableConciseSet>(
                128,
                new Comparator<ImmutableConciseSet>() {
                  @Override
                  public int compare(ImmutableConciseSet a, ImmutableConciseSet b) {
                    return a.getLastWordIndex() - b.getLastWordIndex();
                  }
                });
        for (Bitmap bitmap1 : bitmaps) {
          pq.add(((ImmutableConciseSetWrapper) bitmap1).bitmap);
        }
        ImmutableConciseSet bitmap;
        if (pq.isEmpty()) {
          bitmap = new ImmutableConciseSet();
        } else {
          while (pq.size() > 1) {
            ImmutableConciseSet x1 = pq.poll();
            ImmutableConciseSet x2 = pq.poll();
            pq.add(union(x1, x2));
          }
          bitmap = pq.poll();
        }
        return new ImmutableConciseSetWrapper(bitmap);
      }
    };
  }

  @Override
  public void forEach(IntConsumer ic) {
    throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
  }

  @Override
  public Bitmap clone() {
    throw new UnsupportedOperationException("Not implemented in ImmutableConciseSet");
  }
}
