package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
          new Sharing(
              "clone of a clone",
              x -> {
                PersistentRoaringBitmap copy = x[0].clone();
                return list(x[0], copy, copy.clone());
              }),
          new Sharing(
              "clone, then a write",
              x -> {
                PersistentRoaringBitmap copy = x[0].clone();
                copy.add(7);
                return list(x[0], copy, copy.clone());
              }));

  private static List<Model> models(List<PersistentRoaringBitmap> bitmaps) {
    List<Model> models = new ArrayList<>();
    for (PersistentRoaringBitmap bitmap : bitmaps) {
      models.add(Model.of(bitmap));
    }
    return models;
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
      for (int state = 0; state < 3; state++) {
        PersistentRoaringBitmap x = a();
        PersistentRoaringBitmap other = null; // keeps the co-owner reachable
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
        assertTrue(other == null || other.getCardinality() > 0);
      }
    }
  }

  @Test
  public void rangeWritesThatLookLikeNoOps() {
    // RoaringBitmap.contains(long, long) answers true for these ranges (A13)
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
        PersistentRoaringBitmap holder = x.clone();
        if (state == 1) {
          holder.add(7); // the holder's write turns lent into per-slot flags
          holder.remove(7);
        }
        Map<Container, PersistentRoaringBitmapChecks.Storage> storage = snapshot(holder);
        Model model = Model.of(holder);
        write.write.accept(x);
        String name = write.name + (state == 0 ? " after clone" : " after clone and write");
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
    assertEquals(null, container(x, 0));
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

  // the container methods that copy-on-write relies on

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
}
