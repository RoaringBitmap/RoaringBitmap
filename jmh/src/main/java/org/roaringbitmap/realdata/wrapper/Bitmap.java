package org.roaringbitmap.realdata.wrapper;

import java.io.DataOutputStream;
import java.io.IOException;

public interface Bitmap {

   boolean contains(int i);

   int last();

   int cardinality();

   BitmapIterator iterator();

   BitmapIterator reverseIterator();

   Bitmap and(Bitmap other);

   Bitmap or(Bitmap other);

   void serialize(DataOutputStream dos) throws IOException;

}
