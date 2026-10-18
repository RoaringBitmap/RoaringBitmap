package org.roaringbitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Compares the binary operations of {@link PersistentRoaringBitmap}, which share the containers
 * that pass through, with those of {@link RoaringBitmap}, which copy them. The {@code ...Write}
 * cases charge the copies that sharing defers: writes after an operation, writes on both sides of
 * a clone, a bitmap that alternates between being an operand and being written, and the deep
 * copies at the boundaries with {@link RoaringBitmap}. Run with {@code -prof gc} to see allocation.
 *
 * <p>Each bitmap has {@code containers} containers of about 1,000 random values. {@code overlap}
 * is the percentage of keys that the two operands have in common.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class PersistentRoaringBitmapBenchmark {

  @Param({"1000"})
  public int containers;

  @Param({"0", "20", "100"})
  public int overlap;

  private RoaringBitmap a;
  private RoaringBitmap b;
  private PersistentRoaringBitmap pa;
  private PersistentRoaringBitmap pb;
  private PersistentRoaringBitmap version; // pa with one container changed: shares the others
  private int[] tenPercent; // one value in every tenth key of pa

  @Setup
  public void setup() {
    SplittableRandom random = new SplittableRandom(42);
    int shared = containers * overlap / 100;
    a = randomBitmap(random, 0);
    b = randomBitmap(random, containers - shared);
    pa = PersistentRoaringBitmap.fromBitmap(a);
    pb = PersistentRoaringBitmap.fromBitmap(b);
    version = pa.clone();
    version.add(7);
    tenPercent = new int[containers / 10];
    for (int i = 0; i < tenPercent.length; i++) {
      tenPercent[i] = (10 * i) << 16 | 3;
    }
  }

  private RoaringBitmap randomBitmap(SplittableRandom random, int firstKey) {
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int key = firstKey; key < firstKey + containers; key++) {
      for (int i = 0; i < 1_000; i++) {
        bitmap.add(key << 16 | random.nextInt(1 << 16));
      }
    }
    return bitmap;
  }

  @Benchmark
  public RoaringBitmap roaringOr() {
    return RoaringBitmap.or(a, b);
  }

  @Benchmark
  public PersistentRoaringBitmap persistentOr() {
    return PersistentRoaringBitmap.or(pa, pb);
  }

  @Benchmark
  public RoaringBitmap roaringXor() {
    return RoaringBitmap.xor(a, b);
  }

  @Benchmark
  public PersistentRoaringBitmap persistentXor() {
    return PersistentRoaringBitmap.xor(pa, pb);
  }

  @Benchmark
  public RoaringBitmap roaringAndNot() {
    return RoaringBitmap.andNot(a, b);
  }

  @Benchmark
  public PersistentRoaringBitmap persistentAndNot() {
    return PersistentRoaringBitmap.andNot(pa, pb);
  }

  @Benchmark
  public RoaringBitmap roaringAnd() {
    return RoaringBitmap.and(a, b);
  }

  @Benchmark
  public PersistentRoaringBitmap persistentAnd() {
    return PersistentRoaringBitmap.and(pa, pb);
  }

  /** Operands derived from each other: and reuses the containers they still share. */
  @Benchmark
  public PersistentRoaringBitmap persistentAndOfVersions() {
    return PersistentRoaringBitmap.and(pa, version);
  }

  @Benchmark
  public RoaringBitmap roaringOrThenWrite() {
    RoaringBitmap union = RoaringBitmap.or(a, b);
    for (int value : tenPercent) {
      union.add(value);
    }
    return union;
  }

  /** Writes to 10 % of the result's containers, each of which is copied first. */
  @Benchmark
  public PersistentRoaringBitmap persistentOrThenWrite() {
    PersistentRoaringBitmap union = PersistentRoaringBitmap.or(pa, pb);
    for (int value : tenPercent) {
      union.add(value);
    }
    return union;
  }

  @Benchmark
  public RoaringBitmap roaringCloneThenWrite() {
    RoaringBitmap copy = a.clone();
    for (int value : tenPercent) {
      copy.add(value);
    }
    return copy;
  }

  @Benchmark
  public PersistentRoaringBitmap persistentCloneThenWrite() {
    PersistentRoaringBitmap copy = pa.clone();
    for (int value : tenPercent) {
      copy.add(value);
    }
    return copy;
  }

  /** Both sides of a clone write to every container: about twice the copies of a deep clone. */
  @Benchmark
  public PersistentRoaringBitmap persistentCloneThenWriteBothSides() {
    PersistentRoaringBitmap original = pa.clone();
    PersistentRoaringBitmap copy = original.clone();
    for (int key = 0; key < containers; key++) {
      original.add(key << 16 | 5);
      copy.add(key << 16 | 6);
    }
    return copy;
  }

  /** A bitmap used as an operand before each write copies the container it writes, every time. */
  @Benchmark
  public PersistentRoaringBitmap persistentOperandThenWrite() {
    PersistentRoaringBitmap bitmap = pa.clone();
    PersistentRoaringBitmap empty = new PersistentRoaringBitmap();
    PersistentRoaringBitmap last = null;
    for (int value : tenPercent) {
      last = PersistentRoaringBitmap.or(bitmap, empty);
      bitmap.add(value);
    }
    return last;
  }

  /** A one-off union of plain bitmaps through the persistent class: three deep copies. */
  @Benchmark
  public RoaringBitmap persistentOrOfPlainBitmaps() {
    return PersistentRoaringBitmap.or(
            PersistentRoaringBitmap.fromBitmap(a), PersistentRoaringBitmap.fromBitmap(b))
        .toRoaringBitmap();
  }
}
