package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.roaringbitmap.CharIterator;

import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Arrays;
import java.util.List;

@Execution(ExecutionMode.CONCURRENT)
public class TestReverseMappeableRunContainer {

  private static List<Integer> asList(CharIterator ints) {
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
    MappeableRunContainer mappeableRunContainer = new MappeableRunContainer();
    for (char i = 10; i < 20; ++i) {
      mappeableRunContainer.add(i);
    }
    ReverseMappeableRunContainerCharIterator rmr =
        new ReverseMappeableRunContainerCharIterator(mappeableRunContainer);
    CharIterator rmrClone = rmr.clone();
    final List<Integer> rmrList = asList(rmr);
    assertNotNull(rmrClone);
    final List<Integer> rmrCloneList = asList(rmrClone);
    assertEquals(rmrList, rmrCloneList);
  }

  @Test
  public void testNextAsInt() {
    MappeableRunContainer mappeableRunContainer = new MappeableRunContainer();
    for (char i = 10; i < 15; ++i) {
      mappeableRunContainer.add(i);
    }
    ReverseMappeableRunContainerCharIterator rmr =
        new ReverseMappeableRunContainerCharIterator(mappeableRunContainer);
    assertEquals(14, rmr.nextAsInt());
    rmr.next();
    rmr.next();
    rmr.next();
    rmr.next();
    rmr.next();
    rmr.nextAsInt();
    rmr.nextAsInt();
    rmr.nextAsInt();
    rmr.nextAsInt();
    rmr.nextAsInt();
    assertEquals(13, rmr.nextAsInt());
  }
}
