package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.roaringbitmap.IntConsumer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TestForEach {

  @Test
  public void testContinuous() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(100L, 10000L);

    final MutableInteger cardinality = new MutableInteger();
    bitmap.forEach(
        new IntConsumer() {
          int expected = 100;

          @Override
          public void accept(int value) {
            cardinality.value++;
            assertEquals(value, expected++);
          }
        });
    assertEquals(cardinality.value, bitmap.getCardinality());
  }

  @Test
  public void testDense() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int k = 0; k < 100000; k += 3) bitmap.add(k);

    final MutableInteger cardinality = new MutableInteger();
    bitmap.forEach(
        new IntConsumer() {
          int expected = 0;

          @Override
          public void accept(int value) {
            cardinality.value++;
            assertEquals(value, expected);
            expected += 3;
          }
        });
    assertEquals(cardinality.value, bitmap.getCardinality());
  }

  @Test
  public void testSparse() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int k = 0; k < 100000; k += 3000) bitmap.add(k);

    final MutableInteger cardinality = new MutableInteger();
    bitmap.forEach(
        new IntConsumer() {
          int expected = 0;

          @Override
          public void accept(int value) {
            cardinality.value++;
            assertEquals(value, expected);
            expected += 3000;
          }
        });
    assertEquals(cardinality.value, bitmap.getCardinality());
  }

  @Test
  public void testRunAtIntegerMinValue() {
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOfRange(1L << 31, (1L << 31) + 10);

    final List<Integer> visited = new ArrayList<>();
    bitmap.forEach(
        new IntConsumer() {
          @Override
          public void accept(int value) {
            assertTrue(visited.size() < bitmap.getCardinality(), "too many values, got " + value);
            visited.add(value);
          }
        });
    assertArrayEquals(bitmap.toArray(), visited.stream().mapToInt(Integer::intValue).toArray());
  }

  @Test
  public void testRunAtIntegerMaxValue() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(0x7FFF0000L, 0x7FFF0010L);
    bitmap.add(0x7FFFFFFF);

    final List<Integer> visited = new ArrayList<>();
    bitmap.forEach(
        new IntConsumer() {
          @Override
          public void accept(int value) {
            assertTrue(visited.size() < bitmap.getCardinality(), "too many values, got " + value);
            visited.add(value);
          }
        });
    assertArrayEquals(bitmap.toArray(), visited.stream().mapToInt(Integer::intValue).toArray());
  }
}

class MutableInteger {
  public int value = 0;
}
