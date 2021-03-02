package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class RoaringBatchIteratorTest {

  @Test
  void testAdvanceThroughContainers() {
    List<Container> containers = listOf(Container.rangeOfOnes(0, 3), bitmapContainerOf(0, 1, 2),
        arrayContainerOf(0, 1, 2));
    Collections.shuffle(containers);

    RoaringArray roaringArray = new RoaringArray();
    roaringArray.append((char) 0, containers.get(0));
    roaringArray.append((char) 1, containers.get(1));
    roaringArray.append((char) 2, containers.get(2));

    RoaringBatchIterator it = new RoaringBatchIterator(roaringArray);

    it.advanceIfNeeded(65537);

    assertArrayEquals(new int[]{65537}, consume(it, 1));

    it.advanceIfNeeded(131073);
    assertArrayEquals(new int[]{131073, 131074}, consume(it, 2));

    assertEquals(0, consume(it, 1).length);
  }

  private int[] consume(RoaringBatchIterator it, int amount) {
    int[] data = new int[128];
    int[] buffer = new int[1];

    int ptr = 0;

    while (it.hasNext() && ptr < amount) {
      for (int i = 0, read = it.nextBatch(buffer); i < read; i++) {
        data[ptr++] = buffer[i];
      }

      if (ptr > data.length / 2) {
        data = Arrays.copyOf(data, data.length * 2);
      }
    }

    return Arrays.copyOfRange(data, 0, ptr);
  }

  private BitmapContainer bitmapContainerOf(int... bits) {
    BitmapContainer bitmapContainer = new BitmapContainer();
    for (int b : bits) {
      bitmapContainer.add((char) b);
    }
    return bitmapContainer;
  }

  private ArrayContainer arrayContainerOf(int... bits) {
    ArrayContainer arrayContainer = new ArrayContainer();
    for (int b : bits) {
      arrayContainer.add((char) b);
    }
    return arrayContainer;
  }

  @SafeVarargs
  private final <T> List<T> listOf(T... elements) {
    return new ArrayList<>(Arrays.asList(elements));
  }
}