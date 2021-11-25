package org.roaringbitmap;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Provides A lazily initialised constant allocator. To override allocation,
 * registration must take place before first use.
 */
public final class AllocationManager {

  private static final AllocationManager INSTANCE = new AllocationManager();

  private static final AtomicReferenceFieldUpdater<AllocationManager, Allocator> UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(AllocationManager.class, Allocator.class,
          "providedAllocator");

  private volatile Allocator providedAllocator;

  private static final class RegisteredAllocator {
    public static final Allocator ALLOCATOR;
    static {
      Allocator allocator = INSTANCE.providedAllocator;
      if (allocator == null) {
        allocator = new DefaultAllocator();
        if (!UPDATER.compareAndSet(INSTANCE, null, allocator)) {
          allocator = INSTANCE.providedAllocator;
        }
      }
      ALLOCATOR = allocator;
    }
  }

  /**
   * Register an allocator. Must be registered before first use, otherwise registration will be
   * unsuccessful
   * @param allocator the allocator
   * @return true if registration succeeded
   */
  public static boolean register(Allocator allocator) {
    return UPDATER.compareAndSet(INSTANCE, null, allocator);
  }

  /**
   * @see Allocator#allocateChars
   */
  public static char[] allocateChars(int size) {
    return RegisteredAllocator.ALLOCATOR.allocateChars(size);
  }

  /**
   * @see Allocator#allocateLongs
   */
  public static long[] allocateLongs(int size) {
    return RegisteredAllocator.ALLOCATOR.allocateLongs(size);
  }

  /**
   * @see Allocator#allocateObjects
   */
  public static Container[] allocateContainers(int size) {
    return RegisteredAllocator.ALLOCATOR.allocateObjects(Container.class, size);
  }

  /**
   * @see Allocator#copy
   */
  public static char[] copy(char[] chars) {
    return RegisteredAllocator.ALLOCATOR.copy(chars);
  }

  /**
   * @see Allocator#copy
   */
  public static char[] copy(char[] chars, int newSize) {
    return RegisteredAllocator.ALLOCATOR.copy(chars, newSize);
  }

  /**
   * @see Allocator#copy
   */
  public static long[] copy(long[] longs) {
    return RegisteredAllocator.ALLOCATOR.copy(longs);
  }

  /**
   * @see Allocator#copy
   */
  public static Container[] copy(Container[] containers) {
    return RegisteredAllocator.ALLOCATOR.copy(containers);
  }

  /**
   * @see Allocator#copy
   */
  public static Container[] copy(Container[] containers, int newSize) {
    return RegisteredAllocator.ALLOCATOR.copy(containers, newSize);
  }

  /**
   * @see Allocator#extend
   */
  public static char[] extend(char[] chars, int newSize) {
    return RegisteredAllocator.ALLOCATOR.extend(chars, newSize);
  }

  /**
   * @see Allocator#extend
   */
  public static Container[] extend(Container[] containers, int newSize) {
    return RegisteredAllocator.ALLOCATOR.extend(containers, newSize);
  }

  /**
   * @see Allocator#free
   */
  public static void free(Object object) {
    RegisteredAllocator.ALLOCATOR.free(object);
  }

  static final class DefaultAllocator implements Allocator {

    @Override
    public char[] allocateChars(int size) {
      return new char[size];
    }

    @Override
    public long[] allocateLongs(int size) {
      return new long[size];
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] allocateObjects(Class<T> clazz, int size) {
      return (T[]) Array.newInstance(clazz, size);
    }

    @Override
    public long[] copy(long[] longs) {
      return Arrays.copyOf(longs, longs.length);
    }

    @Override
    public char[] copy(char[] chars) {
      return Arrays.copyOf(chars, chars.length);
    }

    @Override
    public char[] copy(char[] chars, int newSize) {
      return Arrays.copyOf(chars, newSize);
    }

    @Override
    public <T> T[] copy(T[] objects) {
      return Arrays.copyOf(objects, objects.length);
    }

    @Override
    public <T> T[] copy(T[] objects, int newSize) {
      return Arrays.copyOf(objects, newSize);
    }

    @Override
    public char[] extend(char[] chars, int newSize) {
      return Arrays.copyOf(chars, newSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] extend(T[] objects, int newSize) {
      return Arrays.copyOf(objects, newSize);
    }

    @Override
    public void free(Object object) {
      // do nothing
    }
  }
}
