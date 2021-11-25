package org.roaringbitmap;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.roaringbitmap.AllocationManager.register;
import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

public class AllocationManagerTest {

  private static final ReferenceCountingAllocator referenceCounter = new ReferenceCountingAllocator();

  @BeforeAll
  public static void before() {
    assertTrue(register(referenceCounter));
  }

  @AfterEach
  public void clear() {
    referenceCounter.printLeaks();
    referenceCounter.clear();
  }

  @Test
  public void testAllocationsBalance() {
    try (RoaringBitmap bitmap = new RoaringBitmap()) {
      for (int i = 0; i < 1 << 16; i += 2) {
        bitmap.add(i);
      }
      for (int i = 1 << 16; i < 1 << 17; i++) {
        bitmap.add(i);
      }
      for (int i = 1 << 17; i < (1 << 17) + 4096; i++) {
        bitmap.add(i);
      }
      bitmap.runOptimize();
      for (int j = 1; j < 4096; j++) {
        for (int i = (1 << 18) + j; i < 1 << 25; i += 1 << 16) {
          bitmap.add(i);
        }
      }
      bitmap.runOptimize();
    }

    referenceCounter.assertBalanced(long[].class);
    referenceCounter.assertBalanced(char[].class);
    referenceCounter.assertBalanced(Container.class);
  }

  @Test
  public void testBalanceAfterOps() {
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withArrayAt(0).withRunAt(1).withBitmapAt(2).build()) {
      ops(left, right);
    }
    clear();
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withRunAt(0).withBitmapAt(1).withArrayAt(2).build()) {
      ops(left, right);
    }
    clear();
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build()) {
      ops(left, right);
    }
    clear();
//    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
//         RoaringBitmap right = testCase().withArrayAt(1).withRunAt(2).withBitmapAt(3).build()) {
//      ops(left, right);
//    }
//    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
//         RoaringBitmap right = testCase().withRunAt(1).withBitmapAt(2).withArrayAt(3).build()) {
//      ops(left, right);
//    }
//    clear();
//    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
//         RoaringBitmap right = testCase().withBitmapAt(1).withArrayAt(2).withRunAt(3).build()) {
//      ops(left, right);
//    }
//    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
//         RoaringBitmap right = testCase().withArrayAt(2).withRunAt(3).withBitmapAt(4).build()) {
//      ops(left, right);
//    }
//    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
//         RoaringBitmap right = testCase().withRunAt(2).withBitmapAt(3).withArrayAt(4).build()) {
//      ops(left, right);
//    }
//    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
//         RoaringBitmap right = testCase().withBitmapAt(2).withArrayAt(3).withRunAt(4).build()) {
//      ops(left, right);
//    }
//    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
//         RoaringBitmap right = testCase().withArrayAt(3).withRunAt(4).withBitmapAt(5).build()) {
//      ops(left, right);
//    }
//    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
//         RoaringBitmap right = testCase().withRunAt(3).withBitmapAt(4).withArrayAt(5).build()) {
//      ops(left, right);
//    }
//    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
//         RoaringBitmap right = testCase().withBitmapAt(3).withArrayAt(4).withRunAt(5).build()) {
//      ops(left, right);
//    }
    referenceCounter.assertBalanced(long[].class);
    referenceCounter.assertBalanced(char[].class);
    referenceCounter.assertBalanced(Container.class);
  }

  private void ops(RoaringBitmap left, RoaringBitmap right) {
    try (RoaringBitmap and = RoaringBitmap.and(left, right);
         RoaringBitmap andNot = RoaringBitmap.andNot(left, right);
         RoaringBitmap or = RoaringBitmap.or(left, right);
         RoaringBitmap xor = RoaringBitmap.xor(left, right);
         RoaringBitmap orNot = RoaringBitmap.orNot(left, right, 10_000_000)) {

    }
    left.and(right);
    left.or(right);
    left.xor(right);
    left.andNot(right);
    left.orNot(right, 10_000_000);
    left.flip(0, 10_000_000L);
    right.flip(0, 10_000_000L);
    try (RoaringBitmap xor2 = RoaringBitmap.xor(left, right)) {
      left.and(xor2);
      xor2.andNot(left);
      right.or(xor2);
    }
    left.and(right);
    try (RoaringBitmap andNot2 = RoaringBitmap.andNot(left, right)) {
      left.and(andNot2);
      left.xor(right);
      left.add(100000, 10_000_000_00L);
      left.xor(andNot2);
      left.flip(0, 1L << 20);
      left.and(right);
    }
  }

  @Test
  public void testCardinalities() {
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withArrayAt(0).withRunAt(1).withBitmapAt(2).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withRunAt(0).withBitmapAt(1).withArrayAt(2).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withArrayAt(1).withRunAt(2).withBitmapAt(3).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withRunAt(1).withBitmapAt(2).withArrayAt(3).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withBitmapAt(1).withArrayAt(2).withRunAt(3).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withArrayAt(2).withRunAt(3).withBitmapAt(4).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withRunAt(2).withBitmapAt(3).withArrayAt(4).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withBitmapAt(2).withArrayAt(3).withRunAt(4).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withArrayAt(3).withRunAt(4).withBitmapAt(5).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withRunAt(3).withBitmapAt(4).withArrayAt(5).build()) {
      cardinalities(left, right);
    }
    try (RoaringBitmap left = testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build();
         RoaringBitmap right = testCase().withBitmapAt(3).withArrayAt(4).withRunAt(5).build()) {
      cardinalities(left, right);
    }
    referenceCounter.assertBalanced(long[].class);
    referenceCounter.assertBalanced(char[].class);
    referenceCounter.assertBalanced(Container.class);
  }

  private void cardinalities(RoaringBitmap left, RoaringBitmap right) {
    RoaringBitmap.xorCardinality(left, right);
    RoaringBitmap.andCardinality(left, right);
    RoaringBitmap.orCardinality(left, right);
    RoaringBitmap.andNotCardinality(left, right);
    RoaringBitmap.xorCardinality(right, left);
    RoaringBitmap.andCardinality(right, left);
    RoaringBitmap.orCardinality(right, left);
    RoaringBitmap.andNotCardinality(right, left);
    try (RoaringBitmap xor1 = RoaringBitmap.xor(left, right);
         RoaringBitmap xor2 = RoaringBitmap.xor(right, left)) {
      RoaringBitmap.xorCardinality(xor1, xor2);
      RoaringBitmap.andCardinality(xor1, xor2);
      RoaringBitmap.orCardinality(xor1, xor2);
      RoaringBitmap.andNotCardinality(xor1, xor2);
    }
  }


  private static final class ReferenceCountingAllocator implements Allocator {

    public void clear() {
      allocated.clear();
      freed.clear();
    }

    public void printLeaks() {
      Sets.difference(allocated.keySet(), freed.keySet())
          .stream()
          .map(allocated::get)
          .forEach(Throwable::printStackTrace);
    }

    public void assertBalanced(Class<?> clazz) {
      assertEquals(0, counter.get(clazz).get(), clazz.getName());
    }

    private final AllocationManager.DefaultAllocator delegate = new AllocationManager.DefaultAllocator();

    private static final ClassValue<AtomicInteger> counter = new ClassValue<AtomicInteger>() {
      @Override
      protected AtomicInteger computeValue(Class<?> type) {
        return new AtomicInteger();
      }
    };

    private final Map<Object, Throwable> allocated = new HashMap<>();
    private final Map<Object, Throwable> freed = new HashMap<>();

    @Override
    public char[] allocateChars(int size) {
      assertTrue(counter.get(char[].class).getAndAdd(size) >= 0);
      return track(delegate.allocateChars(size));
    }

    @Override
    public long[] allocateLongs(int size) {
      assertTrue(counter.get(long[].class).getAndAdd(size) >= 0);
      return track(delegate.allocateLongs(size));
    }

    @Override
    public <T> T[] allocateObjects(Class<T> clazz, int size) {
      assertTrue(counter.get(clazz).getAndAdd(size) >= 0);
      return track(delegate.allocateObjects(clazz, size));
    }

    @Override
    public long[] copy(long[] longs) {
      assertTrue(counter.get(long[].class).getAndAdd(longs.length) >= 0);
      return track(delegate.copy(longs));
    }

    @Override
    public char[] copy(char[] chars) {
      assertTrue(counter.get(char[].class).getAndAdd(chars.length) >= 0);
      return track(delegate.copy(chars));
    }

    @Override
    public char[] copy(char[] chars, int newSize) {
      assertTrue(counter.get(char[].class).getAndAdd(newSize) >= 0);
      return track(delegate.copy(chars, newSize));
    }

    @Override
    public <T> T[] copy(T[] objects) {
      assertTrue(counter.get(objects.getClass().getComponentType()).getAndAdd(objects.length) >= 0);
      return track(delegate.copy(objects));
    }

    @Override
    public <T> T[] copy(T[] objects, int newSize) {
      assertTrue(counter.get(objects.getClass().getComponentType()).getAndAdd(newSize) >= 0);
      return track(delegate.copy(objects, newSize));
    }

    @Override
    public char[] extend(char[] chars, int newSize) {
      free(chars);
      assertTrue(counter.get(char[].class).getAndAdd(newSize) >= 0);
      return track(delegate.extend(chars, newSize));
    }

    @Override
    public <T> T[] extend(T[] objects, int newSize) {
      free(objects);
      assertTrue(counter.get(objects.getClass().getComponentType()).getAndAdd(newSize) >= 0);
      return track(delegate.extend(objects, newSize));
    }

    @Override
    public void free(Object object) {
      String toString = object instanceof char[] ? Arrays.toString((char[]) object) : object.toString();
      Throwable previouslyFreedAt;
      if ((previouslyFreedAt = freed.putIfAbsent(object, new Throwable(toString))) != null) {
        new Throwable(toString + " freed twice at:").printStackTrace(System.err);
        System.err.println("first freed at:");
        previouslyFreedAt.printStackTrace(System.err);
        System.err.println("allocated at:");
        allocated.get(object).printStackTrace(System.err);
      }
      assertTrue(allocated.containsKey(object), "freed but didn't allocate " + object);
      if (object instanceof long[]) {
        assertTrue(counter.get(long[].class).getAndAdd(-((long[]) object).length) >= 0);
      } else if (object instanceof char[]) {
        assertTrue(counter.get(char[].class).getAndAdd(-((char[]) object).length) >= 0);
      } else {
        assertTrue(counter.get(object.getClass().getComponentType()).getAndAdd(-((Object[]) object).length) >= 0, "" + ((Object[]) object).length);
      }
      delegate.free(object);
    }

    private <T> T track(T x) {
      assertNull(allocated.putIfAbsent(x, new Throwable(String.valueOf(x))));
      return x;
    }

    private char[] track(char[] x) {
      assertNull(allocated.putIfAbsent(x, new Throwable(Arrays.toString(x))));
      return x;
    }
  }
}
