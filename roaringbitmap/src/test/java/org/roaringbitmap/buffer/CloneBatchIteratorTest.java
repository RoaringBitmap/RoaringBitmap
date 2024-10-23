package org.roaringbitmap.buffer;

import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.BatchIterator;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

  @Test
  public void testIndependenceOfClones2() {
    int[] c1 = new int[]{1, 10, 20};
    int[] c2 = new int[]{65560, 70000};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int x : Ints.concat(c1, c2)) {
      bitmap.add(x);
    }

    BatchIterator it1 = bitmap.getBatchIterator();
    BatchIterator it2 = it1.clone();

    int[] buffer = new int[3];

    assertEquals(3, it2.nextBatch(buffer));
    assertArrayEquals(c1, Arrays.copyOfRange(buffer, 0, 3));
    assertEquals(2, it2.nextBatch(buffer));
    assertArrayEquals(c2, Arrays.copyOfRange(buffer, 0, 2));
    assertEquals(0, it2.nextBatch(buffer));

    assertEquals(3, it1.nextBatch(buffer));
    assertArrayEquals(c1, Arrays.copyOfRange(buffer, 0, 3));
    assertEquals(2, it1.nextBatch(buffer));
    assertArrayEquals(c2, Arrays.copyOfRange(buffer, 0, 2));
    assertEquals(0, it1.nextBatch(buffer));
  }
}
