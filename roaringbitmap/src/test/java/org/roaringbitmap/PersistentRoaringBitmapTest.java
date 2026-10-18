package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.UNIVERSE;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertStructure;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertValues;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.container;

import org.roaringbitmap.PersistentRoaringBitmapChecks.Model;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

/** Behaviour of {@link PersistentRoaringBitmap} against an independent model of its values. */
public class PersistentRoaringBitmapTest {

  static final int[] SPARSE = {0, 3, 70, 1_000, 40_000, 65_535};

  /** Adds an array container at {@code key}. */
  static void addSparse(PersistentRoaringBitmap bitmap, int key) {
    for (int low : SPARSE) {
      bitmap.add(key << 16 | low);
    }
  }

  /** Adds a bitmap container at {@code key}: 6,000 values in 3,000 runs, too many for a run. */
  static void addDense(PersistentRoaringBitmap bitmap, int key) {
    for (int low = 0; low < 9_000; low++) {
      if (low % 3 != 0) {
        bitmap.add(key << 16 | low);
      }
    }
  }

  /** Adds a run container at {@code key}. */
  static void addRuns(PersistentRoaringBitmap bitmap, int key) {
    long base = (long) key << 16;
    bitmap.add(base + 100, base + 200);
    bitmap.add(base + 5_000, base + 6_000);
    bitmap.add(base + 60_000, base + 65_536);
  }

  static void addFull(PersistentRoaringBitmap bitmap, int key) {
    bitmap.add((long) key << 16, (long) (key + 1) << 16);
  }

  /** Every container type, at the edge keys and on both sides of 2^31. */
  static PersistentRoaringBitmap varied() {
    PersistentRoaringBitmap bitmap = new PersistentRoaringBitmap();
    addSparse(bitmap, 0);
    addDense(bitmap, 1);
    addRuns(bitmap, 3);
    addFull(bitmap, 5);
    addSparse(bitmap, 0x7FFF);
    addDense(bitmap, 0x8000);
    addRuns(bitmap, 0x8001);
    addSparse(bitmap, 0xFFFF);
    return bitmap;
  }

  static Model variedModel() {
    Model model = Model.EMPTY;
    for (int key : new int[] {0, 0x7FFF, 0xFFFF}) {
      for (int low : SPARSE) {
        model = model.add((long) key << 16 | low);
      }
    }
    for (int key : new int[] {1, 0x8000}) {
      for (int low = 0; low < 9_000; low++) {
        if (low % 3 != 0) {
          model = model.add((long) key << 16 | low);
        }
      }
    }
    for (int key : new int[] {3, 0x8001}) {
      long base = (long) key << 16;
      model = model.or(Model.range(base + 100, base + 200));
      model = model.or(Model.range(base + 5_000, base + 6_000));
      model = model.or(Model.range(base + 60_000, base + 65_536));
    }
    return model.or(Model.range(5L << 16, 6L << 16));
  }

  static byte[] serialize(ImmutableBitmapDataProvider bitmap) {
    ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
    bitmap.serialize(buffer);
    return buffer.array();
  }

  @Test
  public void variedHasEveryContainerType() {
    PersistentRoaringBitmap bitmap = varied();
    assertValues(variedModel(), bitmap, "varied");
    assertTrue(container(bitmap, 0) instanceof ArrayContainer);
    assertTrue(container(bitmap, 1) instanceof BitmapContainer);
    assertTrue(container(bitmap, 3) instanceof RunContainer);
    assertTrue(container(bitmap, 5) instanceof RunContainer);
    assertTrue(container(bitmap, 0x8000) instanceof BitmapContainer);
    assertEquals(8, bitmap.getContainerCount());
  }

  @Test
  public void emptyBitmap() {
    PersistentRoaringBitmap empty = new PersistentRoaringBitmap();
    assertTrue(empty.isEmpty());
    assertEquals(0, empty.getLongCardinality());
    assertFalse(empty.contains(0));
    assertFalse(empty.getIntIterator().hasNext());
    assertEquals("{}", empty.toString());
    assertTrue(empty.validate());
    assertEquals(new PersistentRoaringBitmap(), empty);
    assertEquals(new PersistentRoaringBitmap().hashCode(), empty.hashCode());
    assertStructure(empty);
  }

  @Test
  public void factories() {
    assertValues(
        Model.of(5, -1, 70_000, 5), PersistentRoaringBitmap.bitmapOf(5, -1, 70_000, 5), "bitmapOf");
    assertValues(
        Model.range(10, 300_000),
        PersistentRoaringBitmap.bitmapOfRange(10, 300_000),
        "bitmapOfRange");
    assertValues(
        Model.range(UNIVERSE - 5, UNIVERSE),
        PersistentRoaringBitmap.bitmapOfRange(UNIVERSE - 5, UNIVERSE),
        "range at 2^32");
    assertTrue(PersistentRoaringBitmap.bitmapOfRange(7, 7).isEmpty());
    assertThrows(
        IllegalArgumentException.class,
        () -> PersistentRoaringBitmap.bitmapOfRange(0, UNIVERSE + 1));
  }

  @Test
  public void delegatedReadsMatchRoaringBitmap() {
    PersistentRoaringBitmap bitmap = varied();
    RoaringBitmap plain = bitmap.toRoaringBitmap();
    assertEquals(plain.getCardinality(), bitmap.getCardinality());
    assertEquals(plain.getLongCardinality(), bitmap.getLongCardinality());
    assertEquals(plain.getContainerCount(), bitmap.getContainerCount());
    assertEquals(plain.first(), bitmap.first());
    assertEquals(plain.last(), bitmap.last());
    assertEquals(plain.firstSigned(), bitmap.firstSigned());
    assertEquals(plain.lastSigned(), bitmap.lastSigned());
    assertEquals(plain.getSizeInBytes(), bitmap.getSizeInBytes());
    assertEquals(plain.getLongSizeInBytes(), bitmap.getLongSizeInBytes());
    assertEquals(plain.serializedSizeInBytes(), bitmap.serializedSizeInBytes());
    assertEquals(plain.hasRunCompression(), bitmap.hasRunCompression());
    assertEquals(plain.validate(), bitmap.validate());
    assertEquals(plain.toString(), bitmap.toString());
    assertArrayEquals(plain.toArray(), bitmap.toArray());
    assertArrayEquals(plain.stream().toArray(), bitmap.stream().toArray());
    assertArrayEquals(plain.reverseStream().toArray(), bitmap.reverseStream().toArray());
    assertEquals(values(plain.getSignedIntIterator()), values(bitmap.getSignedIntIterator()));
    assertEquals(values(plain.getReverseIntIterator()), values(bitmap.getReverseIntIterator()));
    List<Integer> forEach = new ArrayList<>();
    bitmap.forEach((IntConsumer) forEach::add);
    assertEquals(values(plain.getIntIterator()), forEach);
    List<Integer> iterated = new ArrayList<>();
    for (Iterator<Integer> it = bitmap.iterator(); it.hasNext(); ) {
      iterated.add(it.next());
    }
    assertEquals(forEach, iterated);
    int[] batch = new int[bitmap.getCardinality()];
    int n = 0;
    BatchIterator batches = bitmap.getBatchIterator();
    int[] buffer = new int[256];
    while (batches.hasNext()) {
      int count = batches.nextBatch(buffer);
      System.arraycopy(buffer, 0, batch, n, count);
      n += count;
    }
    assertArrayEquals(plain.toArray(), batch);

    for (long probe : probes(bitmap)) {
      int x = (int) probe;
      String at = "at " + probe;
      assertEquals(plain.contains(x), bitmap.contains(x), at);
      assertEquals(plain.rankLong(x), bitmap.rankLong(x), at);
      assertEquals(plain.rank(x), bitmap.rank(x), at);
      assertEquals(plain.nextValue(x), bitmap.nextValue(x), at);
      assertEquals(plain.previousValue(x), bitmap.previousValue(x), at);
      assertEquals(plain.nextAbsentValue(x), bitmap.nextAbsentValue(x), at);
      if (plain.rangeCardinality(0, probe + 1) <= probe) {
        // with no absent value up to the probe, RoaringBitmap's own assertion fails when the
        // bitmap holds 0xFFFFFFFF, so only probes with an answer are compared
        assertEquals(plain.previousAbsentValue(x), bitmap.previousAbsentValue(x), at);
      }
      for (long other : new long[] {probe + 1, probe + 70_000, UNIVERSE}) {
        if (other <= UNIVERSE) {
          assertEquals(
              plain.rangeCardinality(probe, other), bitmap.rangeCardinality(probe, other), at);
          assertEquals(plain.intersects(probe, other), bitmap.intersects(probe, other), at);
        }
      }
    }
    for (int j : new int[] {0, 1, 5, 6, 6_000, 12_000, bitmap.getCardinality() - 1}) {
      assertEquals(plain.select(j), bitmap.select(j));
    }
    assertThrows(IllegalArgumentException.class, () -> bitmap.select(bitmap.getCardinality()));
  }

  private static List<Integer> values(IntIterator iterator) {
    List<Integer> values = new ArrayList<>();
    while (iterator.hasNext()) {
      values.add(iterator.next());
    }
    return values;
  }

  /** Values at and around the bounds of every container, of absent keys and of the universe. */
  static long[] probes(PersistentRoaringBitmap bitmap) {
    Set<Long> probes = new HashSet<>();
    for (long fixed : new long[] {0, 1, 0x7FFF_FFFFL, 0x8000_0000L, UNIVERSE - 1}) {
      probes.add(fixed);
    }
    RoaringArray array = PersistentRoaringBitmapChecks.containers(bitmap);
    for (int i = 0; i < array.size; i++) {
      long base = (long) array.keys[i] << 16;
      Container c = array.values[i];
      for (long low :
          new long[] {-1, 0, c.first() - 1, c.first(), c.last(), c.last() + 1, 65_535}) {
        probes.add(base + low);
      }
      probes.add(base + 65_536 + 5);
    }
    return probes.stream()
        .filter(p -> p >= 0 && p < UNIVERSE)
        .mapToLong(Long::longValue)
        .sorted()
        .toArray();
  }

  @Test
  public void pointMutators() {
    PersistentRoaringBitmap bitmap = varied();
    Model model = variedModel();
    SplittableRandom random = new SplittableRandom(1);
    for (int step = 0; step < 3_000; step++) {
      long base = (long) new int[] {0, 1, 2, 3, 5, 0x7FFF, 0x8000, 0xFFFF}[random.nextInt(8)] << 16;
      long value = base + random.nextInt(65_536);
      if (random.nextBoolean()) {
        bitmap.add((int) value);
        model = model.add(value);
      } else {
        bitmap.remove((int) value);
        model = model.remove(value);
      }
    }
    assertValues(model, bitmap, "after point writes");
  }

  @Test
  public void pointMutatorsCrossTheArrayBitmapThreshold() {
    PersistentRoaringBitmap bitmap = new PersistentRoaringBitmap();
    for (int i = 0; i < 4_097; i++) {
      bitmap.add(2 * i);
    }
    assertTrue(container(bitmap, 0) instanceof BitmapContainer);
    bitmap.remove(0);
    assertTrue(container(bitmap, 0) instanceof ArrayContainer);
    for (int i = 1; i < 4_097; i++) {
      bitmap.remove(2 * i);
    }
    assertTrue(bitmap.isEmpty());
    assertStructure(bitmap);
  }

  @Test
  public void rangeMutators() {
    long[][] ranges = {
      {0, 1},
      {0, 65_536},
      {0, 65_537},
      {65_535, 65_537},
      {100, 150},
      {150, 5_500},
      {3L << 16, 4L << 16},
      {(3L << 16) + 150, (5L << 16) + 10},
      {(2L << 16) + 7, (2L << 16) + 9},
      {(1L << 16) - 3, (6L << 16) + 3},
      {0x7FFF_0000L, 0x8001_0000L},
      {0x8000_0000L - 1, 0x8000_0000L + 1},
      {UNIVERSE - 65_536, UNIVERSE},
      {UNIVERSE - 1, UNIVERSE},
      {0, UNIVERSE},
      {77, 77},
    };
    for (long[] range : ranges) {
      for (int add = 0; add < 2; add++) {
        PersistentRoaringBitmap bitmap = varied();
        Model model = variedModel();
        String name = (add == 1 ? "add" : "remove") + Arrays.toString(range);
        if (add == 1) {
          bitmap.add(range[0], range[1]);
          model = model.or(Model.range(range[0], range[1]));
        } else {
          bitmap.remove(range[0], range[1]);
          model = model.andNot(Model.range(range[0], range[1]));
        }
        assertValues(model, bitmap, name);
        // then the inverse write on the result
        if (add == 1) {
          bitmap.remove(range[0], range[1]);
          model = model.andNot(Model.range(range[0], range[1]));
        } else {
          bitmap.add(range[0], range[1]);
          model = model.or(Model.range(range[0], range[1]));
        }
        assertValues(model, bitmap, name + " and back");
      }
    }
  }

  @Test
  public void addRangeTurnsEveryCoveredKeyIntoOneRun() {
    PersistentRoaringBitmap bitmap = varied();
    // covers keys 0 to 5: an array, a bitmap, an absent key, runs and a full run
    bitmap.add(0, 6L << 16);
    for (int key = 0; key <= 5; key++) {
      Container c = container(bitmap, key);
      assertTrue(c instanceof RunContainer && c.getCardinality() == 65_536, "key " + key);
    }
    assertValues(variedModel().or(Model.range(0, 6L << 16)), bitmap, "covered keys");
  }

  @Test
  public void invalidRangesThrowAndChangeNothing() {
    PersistentRoaringBitmap bitmap = varied();
    long[][] invalid = {{-1, 5}, {0, UNIVERSE + 1}, {UNIVERSE, UNIVERSE + 1}, {5, -1}};
    for (long[] range : invalid) {
      assertThrows(IllegalArgumentException.class, () -> bitmap.add(range[0], range[1]));
      assertThrows(IllegalArgumentException.class, () -> bitmap.remove(range[0], range[1]));
      assertThrows(IllegalArgumentException.class, () -> bitmap.contains(range[0], range[1]));
    }
    assertValues(variedModel(), bitmap, "unchanged");
    // reversed and empty ranges are no-ops, as in RoaringBitmap
    bitmap.add(10, 5);
    bitmap.remove(10, 5);
    assertValues(variedModel(), bitmap, "unchanged");
  }

  @Test
  public void containsRange() {
    PersistentRoaringBitmap firstBlock = PersistentRoaringBitmap.bitmapOfRange(0, 65_536);
    // RoaringBitmap.contains(long, long) answers true for both (A13)
    assertFalse(firstBlock.contains(0, 65_537));
    assertFalse(firstBlock.contains(0, UNIVERSE));
    assertTrue(firstBlock.contains(0, 65_536));
    PersistentRoaringBitmap last = PersistentRoaringBitmap.bitmapOf(-1);
    // RoaringBitmap.contains(long, long) answers false (A13)
    assertTrue(last.contains(UNIVERSE - 1, UNIVERSE));
    assertFalse(last.contains(UNIVERSE - 2, UNIVERSE));
    assertFalse(last.contains(5, 5));
    assertFalse(last.contains(6, 5));
    assertTrue(PersistentRoaringBitmap.bitmapOfRange(0, UNIVERSE).contains(0, UNIVERSE));

    PersistentRoaringBitmap bitmap = varied();
    Model model = variedModel();
    long[] probes = probes(bitmap);
    for (long start : probes) {
      for (long end : probes) {
        if (start < end) {
          boolean expected = model.rangeCardinality(start, end) == end - start;
          assertEquals(expected, bitmap.contains(start, end), "[" + start + ", " + end + ")");
        }
      }
    }
  }

  @Test
  public void limit() {
    PersistentRoaringBitmap bitmap = varied();
    Model model = variedModel();
    long cardinality = model.cardinality();
    for (long n :
        new long[] {-5, 0, 1, 2, 6, 7, 6_006, 6_007, 70_000, cardinality - 1, cardinality}) {
      assertValues(model.limit(Math.max(n, 0)), bitmap.limit((int) n), "limit " + n);
    }
    assertValues(model, bitmap.limit(Integer.MAX_VALUE), "limit MAX_VALUE");
    assertValues(model, bitmap, "the source is unchanged");
  }

  @Test
  public void limitDoesNotOverflow() {
    // RoaringBitmap.limit returns every value here (A11); the expectations are symbolic
    PersistentRoaringBitmap universe = PersistentRoaringBitmap.bitmapOfRange(0, UNIVERSE);
    assertTrue(universe.validate());
    for (int n : new int[] {Integer.MAX_VALUE, Integer.MAX_VALUE - 1, 1 << 30, (1 << 30) + 1}) {
      PersistentRoaringBitmap limited = universe.limit(n);
      assertEquals(n, limited.getLongCardinality(), "limit " + n);
      assertEquals(0, limited.first());
      assertEquals(n - 1, limited.last());
      assertTrue(limited.contains(0, n));
      assertStructure(limited);
    }
    assertEquals(UNIVERSE, universe.getLongCardinality());
  }

  @Test
  public void runOptimizeReportsWhetherAnythingChanged() {
    PersistentRoaringBitmap runs = PersistentRoaringBitmap.bitmapOfRange(0, 200_000);
    // RoaringBitmap.runOptimize() returns true here although nothing changes
    assertFalse(runs.runOptimize());
    PersistentRoaringBitmap values = new PersistentRoaringBitmap();
    for (int i = 0; i < 1_000; i++) {
      values.add(i);
    }
    assertTrue(container(values, 0) instanceof ArrayContainer);
    assertTrue(values.runOptimize());
    assertTrue(container(values, 0) instanceof RunContainer);
    assertFalse(values.runOptimize());
    assertValues(Model.range(0, 1_000), values, "run-optimized");
  }

  @Test
  public void smallRunHistoriesStayStructurallyValid() {
    // the run container of {2} is larger than an array: validate() rejects it (A12), J4 does not
    PersistentRoaringBitmap three = PersistentRoaringBitmap.bitmapOfRange(0, 3);
    three.remove(0);
    three.remove(1);
    assertValues(Model.of(2), three, "{2}");
    assertTrue(container(three, 0) instanceof RunContainer);
    assertFalse(three.validate());
    assertTrue(three.runOptimize());
    assertTrue(three.validate());
    PersistentRoaringBitmap limited = PersistentRoaringBitmap.bitmapOfRange(0, 100).limit(1);
    assertValues(Model.of(0), limited, "limit(1)");
    limited.runOptimize();
    assertTrue(limited.validate());
  }

  @Test
  public void trimAndClear() {
    PersistentRoaringBitmap bitmap = varied();
    bitmap.trim();
    assertValues(variedModel(), bitmap, "trimmed");
    assertEquals(bitmap.getContainerCount(), PersistentRoaringBitmapChecks.flags(bitmap).length);
    bitmap.add(70_000); // grows the trimmed arrays again
    assertValues(variedModel().add(70_000), bitmap, "trimmed, then added");
    bitmap.clear();
    assertTrue(bitmap.isEmpty());
    assertStructure(bitmap);
    bitmap.add(5);
    assertValues(Model.of(5), bitmap, "cleared, then added");
  }

  @Test
  public void conversionsCopy() {
    RoaringBitmap source = varied().toRoaringBitmap();
    PersistentRoaringBitmap imported = PersistentRoaringBitmap.fromBitmap(source);
    assertValues(variedModel(), imported, "imported");
    source.add(2 << 16);
    imported.add(4 << 16);
    assertFalse(imported.contains(2 << 16));
    assertFalse(source.contains(4 << 16));
    RoaringBitmap exported = imported.toRoaringBitmap();
    exported.remove(0, UNIVERSE);
    assertValues(variedModel().add(4 << 16), imported, "after the export was cleared");
  }

  @Test
  public void serializedBytesAreThoseOfRoaringBitmap() throws IOException {
    for (PersistentRoaringBitmap bitmap :
        new PersistentRoaringBitmap[] {
          new PersistentRoaringBitmap(),
          varied(),
          PersistentRoaringBitmap.bitmapOf(1, 2, 3),
          PersistentRoaringBitmap.bitmapOfRange(0, UNIVERSE)
        }) {
      byte[] expected = serialize(bitmap.toRoaringBitmap());
      assertArrayEquals(expected, serialize(bitmap));
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      bitmap.serialize(new DataOutputStream(stream));
      assertArrayEquals(expected, stream.toByteArray());
      assertEquals(expected.length, bitmap.serializedSizeInBytes());
    }
  }

  @Test
  public void deserializeRoundTrips() throws IOException {
    PersistentRoaringBitmap bitmap = varied();
    byte[] bytes = serialize(bitmap);

    PersistentRoaringBitmap fromBuffer = PersistentRoaringBitmap.bitmapOf(42);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    fromBuffer.deserialize(buffer);
    assertEquals(0, buffer.position());
    assertValues(variedModel(), fromBuffer, "from a ByteBuffer");
    assertEquals(bitmap, fromBuffer);

    PersistentRoaringBitmap fromStream = new PersistentRoaringBitmap();
    fromStream.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    assertValues(variedModel(), fromStream, "from a DataInput");

    RoaringBitmap plain = new RoaringBitmap();
    plain.deserialize(ByteBuffer.wrap(bytes));
    assertEquals(bitmap.toRoaringBitmap(), plain);
    assertEquals(bitmap, PersistentRoaringBitmap.fromBitmap(plain));
  }

  @Test
  public void failedDecodingKeepsThePreviousValues() {
    byte[] bytes = serialize(varied());
    byte[] truncated = Arrays.copyOf(bytes, bytes.length / 2);
    PersistentRoaringBitmap bitmap = PersistentRoaringBitmap.bitmapOf(7, 70_000);
    assertThrows(
        BufferUnderflowException.class, () -> bitmap.deserialize(ByteBuffer.wrap(truncated)));
    assertThrows(
        IOException.class,
        () -> bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(truncated))));
    byte[] badCookie = bytes.clone();
    badCookie[0] = 0;
    badCookie[1] = 0;
    assertThrows(IOException.class, () -> bitmap.deserialize(ByteBuffer.wrap(badCookie)));
    assertValues(Model.of(7, 70_000), bitmap, "after failed decodes");
  }

  @Test
  public void runOptimizedDataValidates() throws IOException {
    PersistentRoaringBitmap bitmap = varied();
    bitmap.remove(3L << 16, (3L << 16) + 5_500);
    bitmap.remove((5L << 16) + 2, (6L << 16) - 2);
    bitmap.runOptimize();
    PersistentRoaringBitmap decoded = new PersistentRoaringBitmap();
    decoded.deserialize(ByteBuffer.wrap(serialize(bitmap)));
    assertTrue(decoded.validate());
    assertEquals(bitmap, decoded);
  }

  /** Equal values in every encoding and history. */
  static List<PersistentRoaringBitmap> equalBitmaps(Model model) {
    List<PersistentRoaringBitmap> bitmaps = new ArrayList<>();
    PersistentRoaringBitmap byValues = new PersistentRoaringBitmap();
    for (int value : model.toArray()) {
      byValues.add(value);
    }
    bitmaps.add(byValues);
    PersistentRoaringBitmap optimized = byValues.clone();
    optimized.runOptimize();
    bitmaps.add(optimized);
    PersistentRoaringBitmap byRanges = new PersistentRoaringBitmap();
    long[] values = Arrays.stream(model.toArray()).mapToLong(Integer::toUnsignedLong).toArray();
    for (int i = 0; i < values.length; ) {
      int j = i;
      while (j + 1 < values.length && values[j + 1] == values[j] + 1) {
        j++;
      }
      byRanges.add(values[i], values[j] + 1);
      i = j + 1;
    }
    bitmaps.add(byRanges);
    RoaringBitmap plain = byRanges.toRoaringBitmap();
    plain.removeRunCompression();
    bitmaps.add(PersistentRoaringBitmap.fromBitmap(plain));
    bitmaps.add(PersistentRoaringBitmap.or(byRanges, new PersistentRoaringBitmap()));
    return bitmaps;
  }

  @Test
  public void equalBitmapsHaveEqualHashCodes() {
    Model[] models = {
      Model.range(0, 100),
      Model.range(65_530, 65_542),
      Model.range(0, 65_536).or(Model.range(70_000, 70_004)),
      Model.range(0, 5_000).or(Model.range(5_001, 9_000)),
      Model.of(1, 3, 5, 7, 1 << 20, -1),
      Model.range(0x7FFF_FFF0L, 0x8000_0010L),
    };
    boolean differentEncodings = false;
    for (Model model : models) {
      List<PersistentRoaringBitmap> bitmaps = equalBitmaps(model);
      Set<String> encodings = new HashSet<>();
      Set<PersistentRoaringBitmap> set = new HashSet<>();
      Map<PersistentRoaringBitmap, String> map = new HashMap<>();
      set.add(bitmaps.get(0));
      map.put(bitmaps.get(0), "first");
      for (PersistentRoaringBitmap bitmap : bitmaps) {
        assertValues(model, bitmap, model.toString());
        assertEquals(bitmaps.get(0), bitmap, model.toString());
        assertEquals(bitmaps.get(0).hashCode(), bitmap.hashCode(), model.toString());
        assertTrue(set.contains(bitmap));
        assertEquals("first", map.get(bitmap));
        encodings.add(encodings(bitmap));
      }
      differentEncodings |= encodings.size() > 1;
    }
    assertTrue(differentEncodings);
  }

  private static String encodings(PersistentRoaringBitmap bitmap) {
    RoaringArray array = PersistentRoaringBitmapChecks.containers(bitmap);
    return Arrays.stream(array.values, 0, array.size)
        .map(c -> c.getClass().getSimpleName())
        .collect(Collectors.joining(","));
  }

  @Test
  public void hashCodeDependsOnTheValues() {
    Set<Integer> hashes = new HashSet<>();
    for (int i = 0; i < 1_000; i++) {
      PersistentRoaringBitmap bitmap = PersistentRoaringBitmap.bitmapOf(i, 20_000, 20_001);
      bitmap.add(70_000L + i, 80_000L);
      hashes.add(bitmap.hashCode());
    }
    assertTrue(hashes.size() > 990, hashes.size() + " distinct hash codes");
  }

  @Test
  public void equalsComparesValuesOnly() {
    PersistentRoaringBitmap bitmap = varied();
    assertEquals(bitmap, bitmap);
    assertEquals(bitmap, varied());
    assertNotEquals(bitmap, bitmap.toRoaringBitmap());
    assertNotEquals(bitmap.toRoaringBitmap(), bitmap);
    PersistentRoaringBitmap other = varied();
    other.add(2);
    assertNotEquals(bitmap, other);
    assertNotEquals(bitmap, null);
  }
}
