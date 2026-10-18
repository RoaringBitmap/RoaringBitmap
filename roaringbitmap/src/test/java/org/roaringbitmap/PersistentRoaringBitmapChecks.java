package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Test support for {@link PersistentRoaringBitmap}: an independent model of the values, and checks
 * of the ownership invariants, which read the bitmap's private fields by reflection.
 */
final class PersistentRoaringBitmapChecks {

  static final long UNIVERSE = 1L << 32;

  private static final Field VIEW = field(PersistentRoaringBitmap.class, "view");
  private static final Field SHARED = field(PersistentRoaringBitmap.class, "shared");
  private static final Field LENT = field(PersistentRoaringBitmap.class, "lent");
  private static final Field RUNS = field(RunContainer.class, "valueslength");

  private PersistentRoaringBitmapChecks() {}

  private static Field field(Class<?> type, String name) {
    try {
      Field field = type.getDeclaredField(name);
      field.setAccessible(true);
      return field;
    } catch (NoSuchFieldException e) {
      throw new AssertionError(e);
    }
  }

  private static Object get(Field field, Object target) {
    try {
      return field.get(target);
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    }
  }

  static RoaringArray containers(PersistentRoaringBitmap bitmap) {
    return ((RoaringBitmap) get(VIEW, bitmap)).highLowContainer;
  }

  static boolean[] flags(PersistentRoaringBitmap bitmap) {
    return (boolean[]) get(SHARED, bitmap);
  }

  static boolean lent(PersistentRoaringBitmap bitmap) {
    return (Boolean) get(LENT, bitmap);
  }

  /** Whether the bitmap treats slot {@code i} as possibly held by another bitmap. */
  static boolean isShared(PersistentRoaringBitmap bitmap, int i) {
    return lent(bitmap) || flags(bitmap)[i];
  }

  static Container container(PersistentRoaringBitmap bitmap, int key) {
    RoaringArray array = containers(bitmap);
    int i = array.getIndex((char) key);
    return i < 0 ? null : array.values[i];
  }

  /** The array that holds a container's values. */
  static Object backingArray(Container container) {
    if (container instanceof ArrayContainer) {
      return ((ArrayContainer) container).content;
    } else if (container instanceof BitmapContainer) {
      return ((BitmapContainer) container).bitmap;
    }
    return get(RUNS, container);
  }

  /** A copy of everything a container stores, to detect writes to it. */
  static Storage storage(Container container) {
    return new Storage(container);
  }

  static final class Storage {
    private final Class<?> type;
    private final int cardinality;
    private final int runs;
    private final Object array;

    Storage(Container container) {
      type = container.getClass();
      cardinality = container.getCardinality();
      runs = container instanceof RunContainer ? ((RunContainer) container).numberOfRuns() : -1;
      Object backing = backingArray(container);
      array = backing instanceof char[] ? ((char[]) backing).clone() : ((long[]) backing).clone();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Storage)) {
        return false;
      }
      Storage other = (Storage) o;
      return type == other.type
          && cardinality == other.cardinality
          && runs == other.runs
          && (array instanceof char[]
              ? Arrays.equals((char[]) array, (char[]) other.array)
              : Arrays.equals((long[]) array, (long[]) other.array));
    }

    @Override
    public int hashCode() {
      return cardinality;
    }

    @Override
    public String toString() {
      return type.getSimpleName() + " of " + cardinality;
    }
  }

  /** The storage of every container of the bitmaps, by container identity. */
  static Map<Container, Storage> snapshot(PersistentRoaringBitmap... bitmaps) {
    Map<Container, Storage> snapshot = new IdentityHashMap<>();
    for (PersistentRoaringBitmap bitmap : bitmaps) {
      RoaringArray array = containers(bitmap);
      for (int i = 0; i < array.size; i++) {
        snapshot.put(array.values[i], storage(array.values[i]));
      }
    }
    return snapshot;
  }

  static void assertUnchanged(Map<Container, Storage> snapshot, String message) {
    for (Map.Entry<Container, Storage> entry : snapshot.entrySet()) {
      if (!entry.getValue().equals(storage(entry.getKey()))) {
        fail(message + ": a container was written in place");
      }
    }
  }

  /** The container instances of the bitmap, slot by slot. */
  static Container[] instances(PersistentRoaringBitmap bitmap) {
    RoaringArray array = containers(bitmap);
    return Arrays.copyOf(array.values, array.size);
  }

  /** Asserts that the bitmap holds exactly these container instances, in this order. */
  static void assertSameInstances(
      Container[] expected, PersistentRoaringBitmap bitmap, String message) {
    Container[] actual = instances(bitmap);
    assertEquals(expected.length, actual.length, message);
    for (int i = 0; i < expected.length; i++) {
      assertSame(expected[i], actual[i], message + ", slot " + i);
    }
  }

  /** Normal form and flag shape of one bitmap. */
  static void assertStructure(PersistentRoaringBitmap bitmap) {
    RoaringArray array = containers(bitmap);
    boolean[] flags = flags(bitmap);
    assertTrue(flags.length >= array.size, "a flag for every slot");
    for (int i = array.size; i < flags.length; i++) {
      assertFalse(flags[i], "no flag set past the last slot");
    }
    for (int i = 0; i < array.size; i++) {
      if (i > 0) {
        assertTrue(array.keys[i - 1] < array.keys[i], "keys strictly increase");
      }
      assertStructurallyValid(array.values[i]);
    }
  }

  static void assertStructurallyValid(Container container) {
    assertFalse(container.isEmpty(), "no empty container");
    if (container instanceof ArrayContainer) {
      ArrayContainer array = (ArrayContainer) container;
      assertTrue(array.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE, "array threshold");
      for (int k = 1; k < array.cardinality; k++) {
        assertTrue(array.content[k - 1] < array.content[k], "sorted array");
      }
    } else if (container instanceof BitmapContainer) {
      BitmapContainer bitmap = (BitmapContainer) container;
      long count = 0;
      for (long word : bitmap.bitmap) {
        count += Long.bitCount(word);
      }
      assertEquals(count, bitmap.cardinality, "bitmap cardinality matches its words");
      assertTrue(bitmap.cardinality > ArrayContainer.DEFAULT_MAX_SIZE, "bitmap threshold");
    } else {
      RunContainer runs = (RunContainer) container;
      int previousEnd = -2;
      for (int k = 0; k < runs.numberOfRuns(); k++) {
        int start = runs.getValue(k);
        assertTrue(start > previousEnd + 1, "runs sorted, disjoint and not adjacent");
        previousEnd = start + runs.getLength(k);
      }
      assertTrue(previousEnd <= 0xFFFF, "runs within the block");
    }
  }

  /**
   * Ownership across a pool of bitmaps: a container held by several slots is shared at each of
   * them, and two distinct containers never share a backing array.
   */
  static void assertOwnership(Collection<PersistentRoaringBitmap> pool) {
    Map<Container, List<Boolean>> holders = new IdentityHashMap<>();
    Map<Object, Container> arrays = new IdentityHashMap<>();
    for (PersistentRoaringBitmap bitmap : pool) {
      assertStructure(bitmap);
      RoaringArray array = containers(bitmap);
      for (int i = 0; i < array.size; i++) {
        Container container = array.values[i];
        holders.computeIfAbsent(container, c -> new ArrayList<>()).add(isShared(bitmap, i));
        Container other = arrays.put(backingArray(container), container);
        assertTrue(other == null || other == container, "two containers share a backing array");
      }
    }
    for (List<Boolean> attachments : holders.values()) {
      if (attachments.size() > 1) {
        assertFalse(attachments.contains(false), "a container held twice is shared everywhere");
      }
    }
  }

  /**
   * An independent model of a set of unsigned 32-bit values: the sorted bounds of disjoint,
   * non-adjacent intervals [bounds[0], bounds[1]), [bounds[2], bounds[3]), ...
   */
  static final class Model {
    static final Model EMPTY = new Model(new long[0]);

    private static final int OR = 0;
    private static final int AND = 1;
    private static final int XOR = 2;
    private static final int ANDNOT = 3;

    private final long[] bounds;

    private Model(long[] bounds) {
      this.bounds = bounds;
    }

    static Model range(long start, long end) {
      return start < end ? new Model(new long[] {start, end}) : EMPTY;
    }

    static Model of(int... values) {
      long[] sorted = new long[values.length];
      for (int i = 0; i < values.length; i++) {
        sorted[i] = Integer.toUnsignedLong(values[i]);
      }
      Arrays.sort(sorted);
      long[] bounds = new long[2 * sorted.length];
      int n = 0;
      for (long value : sorted) {
        if (n > 0 && bounds[n - 1] >= value) {
          bounds[n - 1] = Math.max(bounds[n - 1], value + 1);
        } else {
          bounds[n++] = value;
          bounds[n++] = value + 1;
        }
      }
      return new Model(Arrays.copyOf(bounds, n));
    }

    /** Reads the values of a bitmap, one run of consecutive values at a time. */
    static Model of(ImmutableBitmapDataProvider bitmap) {
      long[] bounds = new long[16];
      int n = 0;
      long start = bitmap.isEmpty() ? -1 : Integer.toUnsignedLong(bitmap.first());
      while (start >= 0) {
        long end = bitmap.nextAbsentValue((int) start);
        if (end < 0) {
          end = UNIVERSE;
        }
        if (n + 2 > bounds.length) {
          bounds = Arrays.copyOf(bounds, 2 * bounds.length);
        }
        bounds[n++] = start;
        bounds[n++] = end;
        start = end < UNIVERSE ? bitmap.nextValue((int) end) : -1;
      }
      return new Model(Arrays.copyOf(bounds, n));
    }

    Model or(Model other) {
      return combine(other, OR);
    }

    Model and(Model other) {
      return combine(other, AND);
    }

    Model xor(Model other) {
      return combine(other, XOR);
    }

    Model andNot(Model other) {
      return combine(other, ANDNOT);
    }

    Model add(long value) {
      return or(range(value, value + 1));
    }

    Model remove(long value) {
      return andNot(range(value, value + 1));
    }

    private Model combine(Model other, int op) {
      long[] a = bounds;
      long[] b = other.bounds;
      long[] out = new long[a.length + b.length];
      int n = 0;
      int i = 0;
      int j = 0;
      boolean inA = false;
      boolean inB = false;
      boolean in = false;
      while (i < a.length || j < b.length) {
        long point =
            Math.min(i < a.length ? a[i] : Long.MAX_VALUE, j < b.length ? b[j] : Long.MAX_VALUE);
        if (i < a.length && a[i] == point) {
          inA = !inA;
          i++;
        }
        if (j < b.length && b[j] == point) {
          inB = !inB;
          j++;
        }
        boolean now;
        switch (op) {
          case OR:
            now = inA || inB;
            break;
          case AND:
            now = inA && inB;
            break;
          case XOR:
            now = inA != inB;
            break;
          default:
            now = inA && !inB;
        }
        if (now != in) {
          out[n++] = point;
          in = now;
        }
      }
      return new Model(Arrays.copyOf(out, n));
    }

    long cardinality() {
      long cardinality = 0;
      for (int i = 0; i < bounds.length; i += 2) {
        cardinality += bounds[i + 1] - bounds[i];
      }
      return cardinality;
    }

    boolean isEmpty() {
      return bounds.length == 0;
    }

    boolean contains(long value) {
      int i = Arrays.binarySearch(bounds, value);
      return i >= 0 ? i % 2 == 0 : (-i - 1) % 2 == 1;
    }

    long rangeCardinality(long start, long end) {
      return and(range(start, end)).cardinality();
    }

    long first() {
      return bounds[0];
    }

    long last() {
      return bounds[bounds.length - 1] - 1;
    }

    /** The smallest {@code count} values. */
    Model limit(long count) {
      long[] out = new long[bounds.length];
      long remaining = count;
      int n = 0;
      for (int i = 0; i < bounds.length && remaining > 0; i += 2) {
        long length = Math.min(bounds[i + 1] - bounds[i], remaining);
        out[n++] = bounds[i];
        out[n++] = bounds[i] + length;
        remaining -= length;
      }
      return new Model(Arrays.copyOf(out, n));
    }

    /** Every value as an int, in unsigned order; only for small models. */
    int[] toArray() {
      int[] values = new int[(int) cardinality()];
      int n = 0;
      for (int i = 0; i < bounds.length; i += 2) {
        for (long value = bounds[i]; value < bounds[i + 1]; value++) {
          values[n++] = (int) value;
        }
      }
      return values;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Model && Arrays.equals(bounds, ((Model) o).bounds);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bounds);
    }

    @Override
    public String toString() {
      StringBuilder text = new StringBuilder("{");
      for (int i = 0; i < bounds.length; i += 2) {
        if (i > 0) {
          text.append(", ");
        }
        text.append('[').append(bounds[i]).append(", ").append(bounds[i + 1]).append(')');
        if (text.length() > 2000) {
          return text.append(", ...}").toString();
        }
      }
      return text.append('}').toString();
    }
  }

  /** Asserts that the bitmap holds exactly the model's values and is in normal form. */
  static void assertValues(Model expected, PersistentRoaringBitmap actual, String message) {
    assertEquals(expected, Model.of(actual), message);
    assertEquals(expected.cardinality(), actual.getLongCardinality(), message);
    assertStructure(actual);
  }
}
