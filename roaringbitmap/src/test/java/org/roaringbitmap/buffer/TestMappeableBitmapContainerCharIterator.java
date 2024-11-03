package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.roaringbitmap.buffer.MappeableArrayContainer.DEFAULT_MAX_SIZE;

import org.roaringbitmap.PeekableCharIterator;

import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Arrays;
import java.util.List;

@Execution(ExecutionMode.CONCURRENT)
public class TestMappeableBitmapContainerCharIterator {

  private static List<Integer> asList(PeekableCharIterator ints) {
    int[] values = new int[10];
    int size = 0;
    while (ints.hasNext()) {
      if (!(size < values.length)) {
        values = Arrays.copyOf(values, values.length * 2);
      }
      values[size++] = ints.next();
    }
    return Ints.asList(Arrays.copyOf(values, size));
  }

  @Test
  public void testClone() {
    MappeableBitmapContainer mappeableBitmapContainer = new MappeableBitmapContainer();
    for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
      mappeableBitmapContainer.add((char) (k * 10));
    }
    MappeableBitmapContainerCharIterator tmbc =
        new MappeableBitmapContainerCharIterator(mappeableBitmapContainer);
    PeekableCharIterator tmbcClone = tmbc.clone();
    assertNotNull(tmbcClone);
    final List<Integer> tmbcList = asList(tmbc);
    final List<Integer> tmbcCloneList = asList(tmbcClone);
    assertEquals(tmbcList, tmbcCloneList);
  }
}
