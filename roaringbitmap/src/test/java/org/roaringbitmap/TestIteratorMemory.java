package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.stream.Stream;

@Disabled("makes no meaningful assertions")
public class TestIteratorMemory {
  final IntIteratorFlyweight flyweightIterator = new IntIteratorFlyweight();

  final ReverseIntIteratorFlyweight flyweightReverseIterator = new ReverseIntIteratorFlyweight();

  // See org.roaringbitmap.iteration.IteratorsBenchmark32.BenchmarkState
  final RoaringBitmap bitmap_a;
  final RoaringBitmap bitmap_b;
  final RoaringBitmap bitmap_c;

  {
    final int[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6L), 100000);
    bitmap_a = RoaringBitmap.bitmapOf(data);

    bitmap_b = new RoaringBitmap();
    for (int k = 0; k < (1 << 30); k += 32) bitmap_b.add(k);

    bitmap_c = new RoaringBitmap();
    for (int k = 0; k < (1 << 30); k += 3) bitmap_c.add(k);
  }

  private int[] takeSortedAndDistinct(Random source, int count) {

    LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);

    for (int size = 0; size < count; size++) {
      int next;
      do {
        next = Math.abs(source.nextInt());
      } while (!ints.add(next));
    }

    int[] unboxed = toArray(ints);
    Arrays.sort(unboxed);
    return unboxed;
  }

  private int[] toArray(LinkedHashSet<Integer> integers) {
    int[] ints = new int[integers.size()];
    int i = 0;
    for (Integer n : integers) {
      ints[i++] = n;
    }
    return ints;
  }

  protected static final ThreadMXBean THREAD_MBEAN = ManagementFactory.getThreadMXBean();

  public static boolean isThreadAllocatedMemorySupported(ThreadMXBean threadMbean) {
    if (threadMbean != null
        && Stream.of(threadMbean.getClass().getInterfaces())
            .anyMatch(c -> c.getName().equals("com.sun.management.ThreadMXBean"))) {
      try {
        return (Boolean)
            Class.forName("com.sun.management.ThreadMXBean")
                .getMethod("isThreadAllocatedMemorySupported")
                .invoke(threadMbean);
      } catch (IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException
          | NoSuchMethodException
          | SecurityException
          | ClassNotFoundException e) {
        return false;
      }
    } else {
      return false;
    }
  }

  public static long getThreadAllocatedBytes(ThreadMXBean threadMbean, long l) {
    if (isThreadAllocatedMemorySupported(threadMbean)) {
      try {
        return (Long)
            Class.forName("com.sun.management.ThreadMXBean")
                .getMethod("getThreadAllocatedBytes", long.class)
                .invoke(threadMbean, l);
      } catch (IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException
          | NoSuchMethodException
          | SecurityException
          | ClassNotFoundException e) {
        return -1L;
      }
    } else {
      return -1L;
    }
  }

  @BeforeAll
  public static void checkMemoryTrackingIsOK() {
    assumeTrue(isThreadAllocatedMemorySupported(THREAD_MBEAN));
  }

  @Test
  public void measureBoxedIterationAllocation() {
    if (isThreadAllocatedMemorySupported(THREAD_MBEAN)) {
      long before = getThreadAllocatedBytes(THREAD_MBEAN, Thread.currentThread().getId());

      Iterator<Integer> intIterator = bitmap_a.iterator();
      long result = 0;
      while (intIterator.hasNext()) {
        result += intIterator.next();
      }
      // A small check for iterator consistency
      assertEquals(407, result % 1024);

      long after = getThreadAllocatedBytes(THREAD_MBEAN, Thread.currentThread().getId());
      System.out.println("Boxed Iteration allocated: " + (after - before));
    }
  }

  @Test
  public void measureStandardIterationAllocation() {
    if (isThreadAllocatedMemorySupported(THREAD_MBEAN)) {
      long before = getThreadAllocatedBytes(THREAD_MBEAN, Thread.currentThread().getId());

      IntIterator intIterator = bitmap_a.getIntIterator();
      long result = 0;
      while (intIterator.hasNext()) {
        result += intIterator.next();
      }
      // A small check for iterator consistency
      assertEquals(407, result % 1024);

      long after = getThreadAllocatedBytes(THREAD_MBEAN, Thread.currentThread().getId());
      System.out.println("Standard Iteration allocated: " + (after - before));
    }
  }

  @Test
  public void measureFlyWeightIterationAllocation() {
    if (isThreadAllocatedMemorySupported(THREAD_MBEAN)) {
      long before = getThreadAllocatedBytes(THREAD_MBEAN, Thread.currentThread().getId());

      IntIteratorFlyweight intIterator = new IntIteratorFlyweight();
      intIterator.wrap(bitmap_a);

      long result = 0;
      while (intIterator.hasNext()) {
        result += intIterator.next();
      }
      // A small check for iterator consistency
      assertEquals(407, result % 1024);

      long after = getThreadAllocatedBytes(THREAD_MBEAN, Thread.currentThread().getId());
      System.out.println("FlyWeight Iteration allocated: " + (after - before));
    }
  }
}
