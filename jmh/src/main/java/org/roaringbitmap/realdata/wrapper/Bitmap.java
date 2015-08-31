package org.roaringbitmap.realdata.wrapper;

import java.io.DataOutputStream;
import java.io.IOException;

public interface Bitmap {

   boolean contains(int i);

   int last();

   void serialize(DataOutputStream dos) throws IOException;

}
