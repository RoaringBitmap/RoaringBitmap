package org.roaringbitmap.contains;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Point membership queries: 4096 random queries, half hits and half misses, so that branch
 * prediction cannot learn the pattern. Reported time is per query.
 */
@BenchmarkMode(AverageTime)
@OutputTimeUnit(NANOSECONDS)
@State(Scope.Benchmark)
public class PointContainsBenchmark {

  static final int NQ = 4096;

  @Param({
    "array_1c_4000",
    "array_1c_64",
    "bitmap_64c",
    "run_1c_1000runs",
    "run_1c_16runs",
    "keys_4096c_sparse",
    "keys_64c_sparse",
    "mixed_1024c"
  })
  public String config;

  RoaringBitmap bitmap;
  ImmutableRoaringBitmap immutable;
  MutableRoaringBitmap mutable;
  Roaring64Bitmap roaring64;
  int[] queries;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random r = new Random(42);
    RoaringBitmap rb = new RoaringBitmap();
    long universe;
    switch (config) {
      case "array_1c_4000":
        while (rb.getCardinality() < 4000) {
          rb.add(r.nextInt(1 << 16));
        }
        universe = 1 << 16;
        break;
      case "array_1c_64":
        while (rb.getCardinality() < 64) {
          rb.add(r.nextInt(1 << 16));
        }
        universe = 1 << 16;
        break;
      case "bitmap_64c":
        for (int i = 0; i < 64 << 16; i++) {
          if (r.nextBoolean()) {
            rb.add(i);
          }
        }
        universe = 64 << 16;
        break;
      case "run_1c_1000runs":
        for (int i = 0; i < 1000; i++) {
          long s = i * 65L;
          rb.add(s, s + 1 + r.nextInt(40));
        }
        universe = 1 << 16;
        break;
      case "run_1c_16runs":
        for (int i = 0; i < 16; i++) {
          long s = i * 4000L;
          rb.add(s, s + 1 + r.nextInt(2000));
        }
        universe = 1 << 16;
        break;
      case "keys_4096c_sparse":
        for (int i = 0; i < 4096; i++) {
          int k = i * 15;
          for (int j = 0; j < 4; j++) {
            rb.add(k << 16 | r.nextInt(1 << 16));
          }
        }
        universe = 4096L * 15 << 16;
        break;
      case "keys_64c_sparse":
        for (int i = 0; i < 64; i++) {
          int k = i * 2;
          for (int j = 0; j < 4; j++) {
            rb.add(k << 16 | r.nextInt(1 << 16));
          }
        }
        universe = 128L << 16;
        break;
      case "mixed_1024c":
        for (int i = 0; i < 1024; i++) {
          long base = (long) i << 16;
          switch (i % 3) {
            case 0:
              for (int j = 0; j < 1000; j++) {
                rb.add((int) base | r.nextInt(1 << 16));
              }
              break;
            case 1:
              for (int j = 0; j < 20000; j++) {
                rb.add((int) base | r.nextInt(1 << 16));
              }
              break;
            default:
              for (int j = 0; j < 50; j++) {
                long s = base + j * 1300L;
                rb.add(s, s + 1 + r.nextInt(600));
              }
          }
        }
        universe = 1024L << 16;
        break;
      default:
        throw new IllegalArgumentException(config);
    }
    rb.runOptimize();
    bitmap = rb;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    rb.serialize(new DataOutputStream(bos));
    immutable = new ImmutableRoaringBitmap(ByteBuffer.wrap(bos.toByteArray()));
    mutable = rb.toMutableRoaringBitmap();
    roaring64 = new Roaring64Bitmap();
    for (int v : rb) {
      roaring64.addLong(Integer.toUnsignedLong(v));
    }
    roaring64.runOptimize();

    int[] vals = rb.toArray();
    queries = new int[NQ];
    for (int i = 0; i < NQ; i++) {
      queries[i] =
          (i % 2 == 0) ? vals[r.nextInt(vals.length)] : (int) (Math.abs(r.nextLong()) % universe);
    }
    for (int i = NQ - 1; i > 0; i--) {
      int j = r.nextInt(i + 1);
      int t = queries[i];
      queries[i] = queries[j];
      queries[j] = t;
    }
  }

  @Benchmark
  @OperationsPerInvocation(NQ)
  public int roaring() {
    RoaringBitmap rb = bitmap;
    int c = 0;
    for (int x : queries) {
      if (rb.contains(x)) {
        c++;
      }
    }
    return c;
  }

  @Benchmark
  @OperationsPerInvocation(NQ)
  public int immutable() {
    ImmutableRoaringBitmap rb = immutable;
    int c = 0;
    for (int x : queries) {
      if (rb.contains(x)) {
        c++;
      }
    }
    return c;
  }

  @Benchmark
  @OperationsPerInvocation(NQ)
  public int mutable() {
    MutableRoaringBitmap rb = mutable;
    int c = 0;
    for (int x : queries) {
      if (rb.contains(x)) {
        c++;
      }
    }
    return c;
  }

  @Benchmark
  @OperationsPerInvocation(NQ)
  public int roaring64() {
    Roaring64Bitmap rb = roaring64;
    int c = 0;
    for (int x : queries) {
      if (rb.contains(Integer.toUnsignedLong(x))) {
        c++;
      }
    }
    return c;
  }
}
