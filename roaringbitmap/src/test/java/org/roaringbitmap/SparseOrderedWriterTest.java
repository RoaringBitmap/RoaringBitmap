package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

public class SparseOrderedWriterTest {

  @Test
  public void transfer() {
    SparseOrderedWriter sparseOrderedWriter = new SparseOrderedWriter();

    int[] expected = new int[100];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = i * 7 + 3;
      sparseOrderedWriter.add(expected[i]);
    }

    DenseOrderedWriter denseOrderedWriter = sparseOrderedWriter.transfer();
    denseOrderedWriter.flush();

    int[] result = denseOrderedWriter.getUnderlying().toArray();

    Assert.assertArrayEquals(expected, result);
  }

}