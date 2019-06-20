package org.roaringbitmap.realdata.wrapper;

import java.io.DataOutputStream;
import java.io.IOException;

import org.roaringbitmap.IntConsumer;

public interface Bitmap {

  boolean contains(int i);

  int last();

  int cardinality();

  BitmapIterator iterator();

  BitmapIterator reverseIterator();

  Bitmap and(Bitmap other);

  Bitmap or(Bitmap other);

  Bitmap ior(Bitmap other);

  Bitmap xor(Bitmap other);

  Bitmap flip(int rangeStart, int rangeEnd);

  Bitmap andNot(Bitmap other);

  BitmapAggregator naiveAndAggregator();

  BitmapAggregator naiveOrAggregator();

  BitmapAggregator priorityQueueOrAggregator();

  void forEach(IntConsumer ic);

  void serialize(DataOutputStream dos) throws IOException;

  Bitmap clone();

}
