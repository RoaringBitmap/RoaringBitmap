package org.roaringbitmap.buffer;

import org.junit.Test;
import org.roaringbitmap.BatchIterator;

import static org.junit.Assert.assertEquals;
import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

public class CloneBatchIteratorTest {

  @Test
  public void testIndependenceOfClones() {
    MutableRoaringBitmap bitmap = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build().toMutableRoaringBitmap();
    BatchIterator it1 = bitmap.getBatchIterator();
    while (it1.hasNext()) {
      BatchIterator it2 = it1.clone();
      int[] b1 = new int[2];
      int[] b2 = new int[1];
      int c1 = it1.nextBatch(b1);
      it2.nextBatch(b2);
      assertEquals(b1[0], b2[0]);
      if (c1 == 2) {
        it2.nextBatch(b2);
        assertEquals(b1[1], b2[0]);
      }
    }
  }

}
