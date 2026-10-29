package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.UNIVERSE;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertOwnership;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertSameInstances;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertUnchanged;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertValues;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.backingArray;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.container;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.containers;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.flags;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.instances;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.isShared;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.lent;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.snapshot;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.storage;

import org.roaringbitmap.PersistentRoaringBitmapChecks.Model;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/** Sharing and copy-on-write: no write to one bitmap is ever observable through another. */
public class PersistentRoaringBitmapSharingTest {

  // keys only in A: 0, 5; only in B: 2, 6; in both: 1, 3, 0x8000
  private static final RoaringBitmap A = new RoaringBitmap();
  private static final RoaringBitmap B = new RoaringBitmap();

  static {
    for (int low : PersistentRoaringBitmapTest.SPARSE) {
      A.add(low); // array at key 0
      A.add(0x8000 << 16 | low); // array at key 0x8000
      B.add(1 << 16 | low); // array at key 1
      B.add(6 << 16 | low); // array at key 6
    }
    for (int low = 0; low < 9_000; low++) {
      if (low % 3 != 0) {
        A.add(1 << 16 | low); // bitmap at key 1
        B.add(3 << 16 | low); // bitmap at key 3
      }
    }
    A.add((3L << 16) + 100, (3L << 16) + 6_000); // run at key 3
    A.add(5L << 16, 6L << 16); // full run at key 5
    B.add((2L << 16) + 10, (2L << 16) + 20); // run at key 2
    B.add((0x8000L << 16) + 3, (0x8000L << 16) + 50_000); // run at key 0x8000
  }

  static PersistentRoaringBitmap a() {
    return PersistentRoaringBitmap.fromBitmap(A);
  }

  static PersistentRoaringBitmap b() {
    return PersistentRoaringBitmap.fromBitmap(B);
  }

  /** A write to one bitmap and the matching change to its model. */
  static final class Write {
    final String name;
    final Consumer<PersistentRoaringBitmap> write;
    final UnaryOperator<Model> model;
    final long start; // the values a point or range write writes, or an empty range
    final long end;

    Write(String name, Consumer<PersistentRoaringBitmap> write, UnaryOperator<Model> model) {
      this(name, write, model, 0, 0);
    }

    Write(
        String name,
        Consumer<PersistentRoaringBitmap> write,
        UnaryOperator<Model> model,
        long start,
        long end) {
      this.name = name;
      this.write = write;
      this.model = model;
      this.start = start;
      this.end = end;
    }
  }

  static Write add(long value) {
    return new Write(
        "add(" + value + ")", b -> b.add((int) value), m -> m.add(value), value, value + 1);
  }

  static Write remove(long value) {
    return new Write(
        "remove(" + value + ")",
        b -> b.remove((int) value),
        m -> m.remove(value),
        value,
        value + 1);
  }

  static Write addRange(long start, long end) {
    return new Write(
        "add(" + start + ", " + end + ")",
        b -> b.add(start, end),
        m -> m.or(Model.range(start, end)),
        start,
        end);
  }

  static Write removeRange(long start, long end) {
    return new Write(
        "remove(" + start + ", " + end + ")",
        b -> b.remove(start, end),
        m -> m.andNot(Model.range(start, end)),
        start,
        end);
  }

  static final List<Write> WRITES =
      Arrays.asList(
          add(7), // array
          add((1 << 16) + 3), // bitmap
          add((3L << 16) + 50), // run
          add((2L << 16) + 5), // run of B, absent in A
          add(4L << 16), // absent key
          add(UNIVERSE - 1),
          remove(3), // array
          remove((1 << 16) + 1), // bitmap
          remove((3L << 16) + 150), // run
          remove((5L << 16) + 7), // full run
          addRange(70, 80), // within an array
          addRange((1L << 16) + 4_000, (3L << 16) + 4), // partial, absent key, partial
          addRange(0, 7L << 16), // covers keys 0 to 6
          addRange((0x8000L << 16) + 2, (0x8001L << 16) + 2),
          removeRange(0, 4), // part of an array
          removeRange(0, 65_536), // a whole container
          removeRange((1L << 16) + 4_500, (5L << 16) + 4), // partial, interior, partial
          removeRange((3L << 16) + 100, (3L << 16) + 6_000), // exactly the run of A
          removeRange(0, UNIVERSE),
          new Write("runOptimize", PersistentRoaringBitmap::runOptimize, m -> m),
          new Write("trim", PersistentRoaringBitmap::trim, m -> m),
          new Write("clear", PersistentRoaringBitmap::clear, m -> Model.EMPTY),
          new Write(
              "deserialize",
              b -> deserialize(b, PersistentRoaringBitmap.bitmapOf(9, 1 << 20)),
              m -> Model.of(9, 1 << 20)));

  static void deserialize(PersistentRoaringBitmap target, PersistentRoaringBitmap source) {
    try {
      target.deserialize(ByteBuffer.wrap(PersistentRoaringBitmapTest.serialize(source)));
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /** A sharing operation: builds fresh participants, some of which share containers. */
  static final class Sharing {
    final String name;
    final Function<PersistentRoaringBitmap[], List<PersistentRoaringBitmap>> build;

    Sharing(String name, Function<PersistentRoaringBitmap[], List<PersistentRoaringBitmap>> build) {
      this.name = name;
      this.build = build;
    }

    /**
     * The operands and results, with their per-slot flags as the operation left them, and with
     * clones of each when {@code withClones} (which marks every one of them as lending).
     */
    List<PersistentRoaringBitmap> participants(boolean withClones) {
      List<PersistentRoaringBitmap> participants =
          new ArrayList<>(build.apply(new PersistentRoaringBitmap[] {a(), b()}));
      int n = participants.size();
      for (int i = 0; withClones && i < n; i++) {
        participants.add(participants.get(i).clone());
      }
      return participants;
    }
  }

  static List<PersistentRoaringBitmap> list(PersistentRoaringBitmap... bitmaps) {
    return new ArrayList<>(Arrays.asList(bitmaps));
  }

  static final List<Sharing> SHARINGS =
      Arrays.asList(
          new Sharing("clone", x -> list(x[0], x[0].clone())),
          new Sharing("limit", x -> list(x[0], x[0].limit(65_536 + 6_000 + 3))),
          new Sharing("or", x -> list(x[0], x[1], PersistentRoaringBitmap.or(x[0], x[1]))),
          new Sharing("xor", x -> list(x[0], x[1], PersistentRoaringBitmap.xor(x[0], x[1]))),
          new Sharing("andNot", x -> list(x[0], x[1], PersistentRoaringBitmap.andNot(x[0], x[1]))),
          new Sharing("and", x -> list(x[0], x[1], PersistentRoaringBitmap.and(x[0], x[1]))),
          new Sharing("or, same instances", x -> identical(x[0], PersistentRoaringBitmap::or)),
          new Sharing("and, same instances", x -> identical(x[0], PersistentRoaringBitmap::and)),
          new Sharing("xor, same instances", x -> identical(x[0], PersistentRoaringBitmap::xor)),
          new Sharing(
              "andNot, same instances", x -> identical(x[0], PersistentRoaringBitmap::andNot)),
          new Sharing("or(x, x)", x -> list(x[0], PersistentRoaringBitmap.or(x[0], x[0]))),
          new Sharing("and(x, x)", x -> list(x[0], PersistentRoaringBitmap.and(x[0], x[0]))),
          new Sharing("xor(x, x)", x -> list(x[0], PersistentRoaringBitmap.xor(x[0], x[0]))),
          new Sharing("andNot(x, x)", x -> list(x[0], PersistentRoaringBitmap.andNot(x[0], x[0]))),
          new Sharing("versions", x -> versions(x[0], x[1])),
          new Sharing(
              "chain",
              x -> {
                PersistentRoaringBitmap ab = PersistentRoaringBitmap.or(x[0], x[1]);
                PersistentRoaringBitmap c = x[0].limit(10);
                return list(x[0], x[1], c, ab, PersistentRoaringBitmap.or(ab, c));
              }));

  interface Operation {
    PersistentRoaringBitmap apply(PersistentRoaringBitmap x1, PersistentRoaringBitmap x2);
  }

  /** Operands that hold the same container instances under the same keys. */
  static List<PersistentRoaringBitmap> identical(PersistentRoaringBitmap x, Operation op) {
    PersistentRoaringBitmap copy = x.clone();
    return list(x, copy, op.apply(x, copy), op.apply(copy, x));
  }

  /** Versions derived from each other: some keys share instances, others differ. */
  static List<PersistentRoaringBitmap> versions(
      PersistentRoaringBitmap x, PersistentRoaringBitmap y) {
    PersistentRoaringBitmap next = x.clone();
    next.add(7);
    next.remove((3L << 16) + 200, (3L << 16) + 300);
    return list(
        x,
        next,
        PersistentRoaringBitmap.or(x, next),
        PersistentRoaringBitmap.and(x, next),
        PersistentRoaringBitmap.xor(x, next),
        PersistentRoaringBitmap.andNot(next, x),
        PersistentRoaringBitmap.or(PersistentRoaringBitmap.and(x, next), y));
  }

  private static List<Model> models(List<PersistentRoaringBitmap> bitmaps) {
    List<Model> models = new ArrayList<>();
    for (PersistentRoaringBitmap bitmap : bitmaps) {
      models.add(Model.of(bitmap));
    }
    return models;
  }

  @Test
  public void sharingOperationsComputeTheRightValues() {
    Model a = Model.of(a());
    Model b = Model.of(b());
    assertValues(a.or(b), PersistentRoaringBitmap.or(a(), b()), "or");
    assertValues(a.xor(b), PersistentRoaringBitmap.xor(a(), b()), "xor");
    assertValues(a.andNot(b), PersistentRoaringBitmap.andNot(a(), b()), "andNot");
    assertValues(b.andNot(a), PersistentRoaringBitmap.andNot(b(), a()), "andNot");
    assertValues(a.and(b), PersistentRoaringBitmap.and(a(), b()), "and");
    assertEquals(a.and(b).cardinality(), PersistentRoaringBitmap.andCardinality(a(), b()));
    assertEquals(a.or(b).cardinality(), PersistentRoaringBitmap.orCardinality(a(), b()));
    assertEquals(a.xor(b).cardinality(), PersistentRoaringBitmap.xorCardinality(a(), b()));
    assertEquals(a.andNot(b).cardinality(), PersistentRoaringBitmap.andNotCardinality(a(), b()));
    assertTrue(PersistentRoaringBitmap.intersects(a(), b()));
    // a common key, no common value
    PersistentRoaringBitmap low = PersistentRoaringBitmap.bitmapOfRange(0, 10);
    PersistentRoaringBitmap high = PersistentRoaringBitmap.bitmapOfRange(100, 110);
    assertFalse(PersistentRoaringBitmap.intersects(low, high));
    assertEquals(0, PersistentRoaringBitmap.andCardinality(low, high));
    assertValues(Model.EMPTY, PersistentRoaringBitmap.and(low, high), "and of disjoint values");
    PersistentRoaringBitmap empty = new PersistentRoaringBitmap();
    assertValues(a, PersistentRoaringBitmap.or(a(), empty), "or empty");
    assertValues(a, PersistentRoaringBitmap.or(empty, a()), "or empty");
    assertValues(Model.EMPTY, PersistentRoaringBitmap.and(a(), empty), "and empty");
    assertValues(a, PersistentRoaringBitmap.andNot(a(), empty), "andNot empty");
    assertValues(Model.EMPTY, PersistentRoaringBitmap.andNot(empty, a()), "andNot empty");
    assertValues(a, PersistentRoaringBitmap.xor(empty, a()), "xor empty");
  }

  @Test
  public void writesAfterSharingAreIsolated() {
    for (boolean withClones : new boolean[] {false, true}) {
      for (Sharing sharing : SHARINGS) {
        writesAfterSharingAreIsolated(sharing, withClones);
      }
    }
  }

  private static void writesAfterSharingAreIsolated(Sharing sharing, boolean withClones) {
    int size = sharing.participants(withClones).size();
    for (int target = 0; target < size; target++) {
      for (Write write : WRITES) {
        String name =
            sharing.name
                + (withClones ? " with clones, " : ", ")
                + write.name
                + " on participant "
                + target;
        List<PersistentRoaringBitmap> participants = sharing.participants(withClones);
        List<Model> models = models(participants);
        assertOwnership(participants);
        write.write.accept(participants.get(target));
        models.set(target, write.model.apply(models.get(target)));
        // a second write, to another participant, after the first one unshared
        int second = (target + 1) % size;
        write.write.accept(participants.get(second));
        models.set(second, write.model.apply(models.get(second)));
        for (int i = 0; i < size; i++) {
          assertValues(models.get(i), participants.get(i), name + ", participant " + i);
        }
        assertOwnership(participants);
      }
    }
  }

  @Test
  public void passThroughContainersAreShared() {
    PersistentRoaringBitmap a = a();
    PersistentRoaringBitmap b = b();
    PersistentRoaringBitmap union = PersistentRoaringBitmap.or(a, b);
    assertSame(container(a, 0), container(union, 0));
    assertSame(container(a, 5), container(union, 5));
    assertSame(container(b, 2), container(union, 2));
    assertSame(container(b, 6), container(union, 6));
    assertNotSame(container(a, 1), container(union, 1));
    assertTrue(lent(a));
    assertTrue(lent(b));
    assertFalse(lent(union));

    PersistentRoaringBitmap c = a();
    PersistentRoaringBitmap d = b();
    PersistentRoaringBitmap difference = PersistentRoaringBitmap.andNot(c, d);
    assertSame(container(c, 0), container(difference, 0));
    assertTrue(lent(c));
    assertFalse(lent(d));

    PersistentRoaringBitmap e = a();
    PersistentRoaringBitmap f = b();
    PersistentRoaringBitmap intersection = PersistentRoaringBitmap.and(e, f);
    assertFalse(lent(e), "and writes nothing to its operands");
    assertFalse(lent(f), "and writes nothing to its operands");
    assertOwnership(list(e, f, intersection));
  }

  @Test
  public void identicalInstancesAreDecidedWithoutCopying() {
    PersistentRoaringBitmap x = a();
    PersistentRoaringBitmap y = x.clone();
    for (PersistentRoaringBitmap same :
        new PersistentRoaringBitmap[] {
          PersistentRoaringBitmap.or(x, y), PersistentRoaringBitmap.and(x, y)
        }) {
      RoaringArray array = containers(same);
      for (int i = 0; i < array.size; i++) {
        assertSame(containers(x).values[i], array.values[i]);
        assertTrue(isShared(same, i));
      }
    }
    assertTrue(PersistentRoaringBitmap.xor(x, y).isEmpty());
    assertTrue(PersistentRoaringBitmap.andNot(x, y).isEmpty());
    // and(x, y) reuses instances that are already shared on both sides: no operand is written
    PersistentRoaringBitmap z = a();
    PersistentRoaringBitmap version = PersistentRoaringBitmap.or(z, new PersistentRoaringBitmap());
    z.add(7); // z owns its key-0 container again, the other keys stay shared with version
    boolean[] before = flags(z).clone();
    PersistentRoaringBitmap common = PersistentRoaringBitmap.and(z, version);
    assertFalse(lent(z));
    assertTrue(Arrays.equals(before, flags(z)));
    assertSame(container(z, 5), container(common, 5));
    assertOwnership(list(z, version, common));
  }

  @Test
  public void selfOperations() {
    PersistentRoaringBitmap x = a();
    PersistentRoaringBitmap union = PersistentRoaringBitmap.or(x, x);
    assertEquals(x, union);
    assertTrue(lent(x));
    PersistentRoaringBitmap y = a();
    assertTrue(PersistentRoaringBitmap.xor(y, y).isEmpty());
    assertTrue(PersistentRoaringBitmap.andNot(y, y).isEmpty());
    assertFalse(lent(y));
    assertEquals(y, PersistentRoaringBitmap.and(y, y));
    assertOwnership(list(x, union, y));
  }

  @Test
  public void cloneSharesEveryContainer() {
    PersistentRoaringBitmap x = a();
    PersistentRoaringBitmap copy = x.clone();
    RoaringArray original = containers(x);
    RoaringArray cloned = containers(copy);
    assertNotSame(original.keys, cloned.keys);
    assertNotSame(original.values, cloned.values);
    for (int i = 0; i < original.size; i++) {
      assertSame(original.values[i], cloned.values[i]);
    }
    assertTrue(lent(x));
    assertTrue(lent(copy));
  }

  @Test
  public void limitSharesWholeContainers() {
    PersistentRoaringBitmap x = a();
    PersistentRoaringBitmap limited = x.limit(PersistentRoaringBitmapTest.SPARSE.length + 10);
    assertSame(container(x, 0), container(limited, 0));
    assertNotSame(container(x, 1), container(limited, 1));
    assertEquals(10, container(limited, 1).getCardinality());
    assertTrue(lent(x));
    assertTrue(isShared(limited, 0));
    assertFalse(isShared(limited, 1));
    PersistentRoaringBitmap partialOnly = a().limit(3);
    assertFalse(isShared(partialOnly, 0));
  }

  @Test
  public void epochs() {
    PersistentRoaringBitmap x = a();
    Model model = Model.of(x);
    PersistentRoaringBitmap first = x.clone();
    x.add(7); // first epoch ends: x unlends and copies the key-0 container
    PersistentRoaringBitmap second = PersistentRoaringBitmap.or(x, new PersistentRoaringBitmap());
    assertTrue(lent(x));
    x.add(8); // second epoch: copies the key-0 container again
    x.remove(5L << 16, 6L << 16); // whole-container removal while shared
    x.trim();
    PersistentRoaringBitmap third = x.clone();
    x.add(100L << 16, 102L << 16); // inserts keys
    x.clear();
    x.add(9);
    deserialize(third, PersistentRoaringBitmap.bitmapOf(1, 2));
    assertValues(model, first, "first");
    assertValues(model.add(7), second, "second");
    assertValues(Model.of(1, 2), third, "third");
    assertValues(Model.of(9), x, "x");
    assertOwnership(list(x, first, second, third));
  }

  @Test
  public void mixedFlagsGrowthAndThresholds() {
    PersistentRoaringBitmap x = a();
    PersistentRoaringBitmap y = b();
    PersistentRoaringBitmap union = PersistentRoaringBitmap.or(x, y); // per-slot flags, mixed
    union.trim(); // no spare capacity left
    PersistentRoaringBitmap copy = union.clone(); // and lent on top
    Model model = Model.of(union);
    // inserts many keys, so the container arrays grow and the flags must grow with them
    union.add((7L << 16) + 5, (40L << 16) + 5);
    model = model.or(Model.range((7L << 16) + 5, (40L << 16) + 5));
    assertValues(model, union, "grown");
    assertOwnership(list(x, y, union, copy));

    // array to bitmap and back, on a shared container
    PersistentRoaringBitmap full = new PersistentRoaringBitmap();
    for (int i = 0; i < 4_096; i++) {
      full.add(2 * i);
    }
    PersistentRoaringBitmap before = full.clone();
    full.add(1);
    assertTrue(container(full, 0) instanceof BitmapContainer);
    assertTrue(container(before, 0) instanceof ArrayContainer);
    PersistentRoaringBitmap after = full.clone();
    full.remove(1);
    assertTrue(container(full, 0) instanceof ArrayContainer);
    assertEquals(4_097, after.getCardinality());
    assertEquals(4_096, before.getCardinality());
    assertOwnership(list(full, before, after));
  }

  @Test
  public void noOpWritesChangeNothing() {
    List<Write> noOps =
        Arrays.asList(
            add(3),
            add((5L << 16) + 9),
            remove(4),
            remove(9L << 16),
            addRange(0, 1),
            addRange(5L << 16, 6L << 16),
            addRange((3L << 16) + 200, (3L << 16) + 6_000),
            addRange(5, 5),
            removeRange(10, 60),
            removeRange(7L << 16, 0x8000L << 16),
            removeRange(0x8001L << 16, UNIVERSE),
            removeRange(9, 3));
    for (Write write : noOps) {
      for (int state = 0; state < 4; state++) {
        PersistentRoaringBitmap x = a();
        PersistentRoaringBitmap other = null; // another holder of x's containers
        if (state == 1) {
          other = x.clone(); // lent
        } else if (state == 2) {
          other = x.clone();
          x.add(7); // the write turns lent into per-slot flags
          x.remove(7);
        } else if (state == 3) {
          other = x;
          x = PersistentRoaringBitmap.or(x, new PersistentRoaringBitmap()); // per-slot flags
        }
        boolean wasLent = lent(x);
        boolean[] flags = flags(x).clone();
        Container[] values = instances(x);
        Map<Container, PersistentRoaringBitmapChecks.Storage> storage = snapshot(x);
        write.write.accept(x);
        String name = write.name + " in state " + state;
        assertEquals(wasLent, lent(x), name);
        assertTrue(Arrays.equals(flags, flags(x)), name);
        assertSameInstances(values, x, name);
        assertUnchanged(storage, name);
        if (other != null) {
          assertValues(Model.of(a()), other, name + ", the other holder");
        }
      }
    }
  }

  @Test
  public void rangeWritesThatLookLikeNoOps() {
    // RoaringBitmap.contains(long, long) answered true for these ranges before #863
    for (long end : new long[] {65_537, UNIVERSE}) {
      PersistentRoaringBitmap x = PersistentRoaringBitmap.bitmapOfRange(0, 65_536);
      PersistentRoaringBitmap copy = x.clone();
      x.add(0, end);
      assertValues(Model.range(0, end), x, "add(0, " + end + ")");
      assertValues(Model.range(0, 65_536), copy, "the copy");
    }
  }

  /** The delegated mutators never write a container that another bitmap holds. */
  @Test
  public void delegatedMutatorsWriteOnlyOwnedContainers() {
    for (Write write : WRITES) {
      for (int state = 0; state < 2; state++) {
        PersistentRoaringBitmap x = a();
        PersistentRoaringBitmap holder =
            state == 0 ? x.clone() : PersistentRoaringBitmap.or(x, new PersistentRoaringBitmap());
        Map<Container, PersistentRoaringBitmapChecks.Storage> storage = snapshot(holder);
        Model model = Model.of(holder);
        write.write.accept(x);
        String name = write.name + (state == 0 ? " after clone" : " after or");
        assertUnchanged(storage, name);
        assertValues(model, holder, name);
        assertOwnership(list(x, holder));
        if (write.end > write.start && !Model.of(x).equals(model)) {
          // every slot the write reached is owned now, so it is never copied again
          RoaringArray array = containers(x);
          for (int i = 0; i < array.size; i++) {
            long key = array.keys[i];
            if (key >= write.start >>> 16 && key <= (write.end - 1) >>> 16) {
              assertFalse(isShared(x, i), name + ", key " + key);
              assertNotSame(container(holder, (int) key), array.values[i], name);
            }
          }
        }
      }
    }
  }

  @Test
  public void replacedAndDroppedContainersAreNotCopied() {
    PersistentRoaringBitmap x = a();
    PersistentRoaringBitmap copy = x.clone();
    Container dense = container(x, 1);
    Map<Container, PersistentRoaringBitmapChecks.Storage> storage = snapshot(copy);
    x.add(1L << 16, 2L << 16); // the shared bitmap container is replaced, not copied
    assertTrue(container(x, 1) instanceof RunContainer);
    x.remove(0, 65_536); // the shared array container is dropped, not copied
    assertNull(container(x, 0));
    assertSame(dense, container(copy, 1));
    assertUnchanged(storage, "replaced and dropped");
  }

  @Test
  public void runOptimizeAndTrimLeaveSharedContainersAlone() {
    PersistentRoaringBitmap x = new PersistentRoaringBitmap();
    for (int i = 0; i < 1_000; i++) {
      x.add(i); // an array container that runOptimize converts
      x.add(70_000 + 2 * i); // an array container that it keeps
    }
    PersistentRoaringBitmap copy = x.clone();
    Map<Container, PersistentRoaringBitmapChecks.Storage> storage = snapshot(copy);
    Object kept = backingArray(container(x, 1));
    assertTrue(x.runOptimize());
    assertFalse(isShared(x, 0), "a converted container is owned, so it is not copied again");
    assertTrue(isShared(x, 1));
    x.trim();
    assertTrue(container(x, 0) instanceof RunContainer);
    assertTrue(container(copy, 0) instanceof ArrayContainer);
    assertSame(kept, backingArray(container(copy, 1)), "a shared container is not trimmed");
    assertUnchanged(storage, "runOptimize and trim");
    assertOwnership(list(x, copy));
  }

  // the container methods that sharing relies on

  /** One container of each type, plus a full run, in a fresh copy each time. */
  static List<Container> shapes() {
    PersistentRoaringBitmap x = a();
    PersistentRoaringBitmap y = b();
    return Arrays.asList(
        container(x, 0).clone(), // array
        container(x, 1).clone(), // bitmap
        container(x, 3).clone(), // run
        container(x, 5).clone(), // full run
        container(y, 1).clone(), // array overlapping the bitmap
        container(y, 3).clone(), // bitmap overlapping the run
        container(y, 0x8000).clone()); // run
  }

  interface ContainerOperation {
    Container apply(Container c1, Container c2);
  }

  @Test
  public void outOfPlaceOperationsReturnFreshContainers() {
    Map<String, ContainerOperation> operations = new java.util.LinkedHashMap<>();
    operations.put("and", Container::and);
    operations.put("or", Container::or);
    operations.put("xor", Container::xor);
    operations.put("andNot", Container::andNot);
    operations.put("not", (c1, c2) -> c1.not(10, 30_000));
    operations.put("limit", (c1, c2) -> c1.limit(100));
    operations.put("add", (c1, c2) -> c1.add(50, 7_000));
    operations.put("remove", (c1, c2) -> c1.remove(50, 7_000));
    operations.put("clone", (c1, c2) -> c1.clone());
    int shapes = shapes().size();
    for (Map.Entry<String, ContainerOperation> operation : operations.entrySet()) {
      for (int i = 0; i < shapes; i++) {
        for (int j = 0; j < shapes; j++) {
          List<Container> containers = shapes();
          Container c1 = containers.get(i);
          Container c2 = containers.get(j);
          PersistentRoaringBitmapChecks.Storage before1 = storage(c1);
          PersistentRoaringBitmapChecks.Storage before2 = storage(c2);
          Container result = operation.getValue().apply(c1, c2);
          String name = operation.getKey() + " of shapes " + i + " and " + j;
          assertNotSame(c1, result, name);
          assertNotSame(c2, result, name);
          assertNotSame(backingArray(c1), backingArray(result), name);
          assertNotSame(backingArray(c2), backingArray(result), name);
          assertEquals(before1, storage(c1), name);
          assertEquals(before2, storage(c2), name);
          result.iadd(0, 65_536).iremove(0, 65_536); // scribble over the result
          assertEquals(before1, storage(c1), name);
          assertEquals(before2, storage(c2), name);
        }
      }
    }
  }

  @Test
  public void runOptimizeIsAPureConversion() {
    for (Container container : shapes()) {
      PersistentRoaringBitmapChecks.Storage before = storage(container);
      Container optimized = container.runOptimize();
      assertEquals(before, storage(container));
      if (optimized != container) {
        assertNotSame(backingArray(container), backingArray(optimized));
        assertEquals(container, optimized);
      }
    }
    // an array container that runOptimize does convert
    Container array = new ArrayContainer(0, 1_000);
    PersistentRoaringBitmapChecks.Storage before = storage(array);
    Container optimized = array.runOptimize();
    assertTrue(optimized instanceof RunContainer);
    assertEquals(before, storage(array));
  }

  /** In-place calls return their receiver, or a container that shares nothing with others. */
  @Test
  public void inPlaceCallsKeepTheirResultsPrivate() {
    List<Function<Container, Container>> calls =
        Arrays.asList(
            c -> c.add((char) 4),
            c -> c.add((char) 8_999),
            c -> c.remove((char) 1),
            c -> c.remove((char) 150),
            c -> c.iadd(10, 20),
            c -> c.iadd(0, 65_536),
            c -> c.iremove(0, 8_000),
            c -> c.iremove(0, 65_536),
            c -> c.iremove(10, 20));
    for (Function<Container, Container> call : calls) {
      List<Container> receivers = shapes();
      List<Container> others = shapes();
      Map<Object, Container> arrays = new IdentityHashMap<>();
      for (Container other : others) {
        arrays.put(backingArray(other), other);
      }
      for (Container receiver : receivers) {
        Container result = call.apply(receiver);
        assertTrue(result == receiver || !arrays.containsKey(backingArray(result)));
      }
      // crossing the array/bitmap threshold in both directions
      ArrayContainer array = new ArrayContainer();
      for (int i = 0; i < 4_096; i++) {
        array.add((char) (2 * i));
      }
      Container bitmap = array.add((char) 1);
      Container grown = call.apply(bitmap);
      assertTrue(grown == bitmap || !arrays.containsKey(backingArray(grown)));
    }
  }

  // concurrent non-mutating operations, then a mutation by the owner

  /**
   * Readers share containers concurrently, the owner writes after joining them, and every result
   * keeps its values. The operands start owned, so the readers' own lending is what protects the
   * results; the second epoch starts after the owner's write has made a container owned again.
   */
  @Test
  public void concurrentSharingThenOwnerWrites() throws InterruptedException {
    for (Write write : WRITES) {
      PersistentRoaringBitmap x = a();
      PersistentRoaringBitmap y = b();
      // versions that share containers, which and() reuses without writing either of them
      PersistentRoaringBitmap older = a();
      PersistentRoaringBitmap newer =
          PersistentRoaringBitmap.or(older, new PersistentRoaringBitmap());
      newer.add(7);
      Model mx = Model.of(x);
      Model my = Model.of(y);
      List<PersistentRoaringBitmap> results = new ArrayList<>();
      List<Model> expected = new ArrayList<>();
      for (int epoch = 0; epoch < 2; epoch++) {
        assertFalse(lent(x), "x starts owned");
        assertFalse(lent(y), "y starts owned");
        for (List<PersistentRoaringBitmap> mine : readConcurrently(x, y, older, newer)) {
          for (int i = 0; i < mine.size(); i += 6) {
            results.addAll(mine.subList(i, i + 6));
            expected.add(mx.or(my));
            expected.add(mx.xor(my));
            expected.add(mx.andNot(my));
            expected.add(Model.of(older).and(Model.of(newer)));
            expected.add(mx);
            expected.add(my.limit(100 + i / 6));
          }
        }
        assertTrue(lent(x) && lent(y), "the readers lent the operands' containers");
        // the owner writes after joining every reader: key 0 in both, owned after epoch 0
        x.add(7 + epoch);
        y.add(7 + epoch);
        mx = mx.add(7 + epoch);
        my = my.add(7 + epoch);
        for (int i = 0; i < results.size(); i++) {
          assertValues(expected.get(i), results.get(i), "result " + i + ", epoch " + epoch);
        }
      }
      write.write.accept(x);
      write.write.accept(y);
      for (int i = 0; i < results.size(); i++) {
        assertValues(expected.get(i), results.get(i), "result " + i + " after " + write.name);
      }
      assertValues(write.model.apply(mx), x, "x");
      assertValues(write.model.apply(my), y, "y");
    }
  }

  private static List<List<PersistentRoaringBitmap>> readConcurrently(
      PersistentRoaringBitmap x,
      PersistentRoaringBitmap y,
      PersistentRoaringBitmap older,
      PersistentRoaringBitmap newer)
      throws InterruptedException {
    final int threads = 4;
    final List<List<PersistentRoaringBitmap>> results = new ArrayList<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final long cardinality = x.getLongCardinality();
    Thread[] workers = new Thread[threads];
    for (int t = 0; t < threads; t++) {
      final List<PersistentRoaringBitmap> mine = new ArrayList<>();
      results.add(mine);
      workers[t] =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < 8; i++) {
                    mine.add(PersistentRoaringBitmap.or(x, y));
                    mine.add(PersistentRoaringBitmap.xor(x, y));
                    mine.add(PersistentRoaringBitmap.andNot(x, y));
                    mine.add(PersistentRoaringBitmap.and(newer, older));
                    mine.add(x.clone());
                    mine.add(y.limit(100 + i));
                    assertEquals(cardinality, x.getLongCardinality());
                  }
                } catch (Throwable e) {
                  failure.compareAndSet(null, e);
                }
              });
      workers[t].start();
    }
    for (Thread worker : workers) {
      worker.join();
    }
    if (failure.get() != null) {
      throw new AssertionError(failure.get());
    }
    return results;
  }
}
