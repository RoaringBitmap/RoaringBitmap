package org.roaringbitmap.serialization;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class Roaring64BmpSerializationBenchmark {

  @BenchmarkMode(Mode.AverageTime)
  @Benchmark
  public int testDeserialize(BenchmarkState benchmarkState) throws IOException {
    benchmarkState.presoutbb.rewind();
    try (ByteBufferBackedInputStream in =
        new ByteBufferBackedInputStream(benchmarkState.presoutbb)) {
      benchmarkState.bitmap_b.deserialize(new DataInputStream(in));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return benchmarkState.presoutbb.limit();
  }

  @BenchmarkMode(Mode.AverageTime)
  @Benchmark
  public int testSerialize(BenchmarkState benchmarkState) throws IOException {
    benchmarkState.outbb.rewind();
    try (ByteBufferBackedOutputStream out =
        new ByteBufferBackedOutputStream(benchmarkState.outbb)) {
      benchmarkState.bitmap_a.serialize(new DataOutputStream(out));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return benchmarkState.outbb.limit();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    final Roaring64Bitmap bitmap_a;

    final Roaring64Bitmap bitmap_b = new Roaring64Bitmap();

    final ByteBuffer outbb;

    final ByteBuffer presoutbb;

    public BenchmarkState() {

      final long[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6l), 100000);
      bitmap_a = Roaring64Bitmap.bitmapOf(data);
      for (long k = 100000 + Integer.MAX_VALUE; k < 200000 + Integer.MAX_VALUE; ++k) {
        bitmap_a.add(2 * k);
      }
      long sizeLonga = bitmap_a.serializedSizeInBytes();
      if (sizeLonga >= Integer.MAX_VALUE) {
        throw new RuntimeException("Not supported size");
      }
      int sizeInteger = (int) sizeLonga;
      outbb = ByteBuffer.allocate(sizeInteger);
      presoutbb = ByteBuffer.allocate(sizeInteger);
      ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(presoutbb);
      try (DataOutputStream outputStream = new DataOutputStream(out)) {
        bitmap_a.serialize(outputStream);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      presoutbb.flip();
    }

    private long[] takeSortedAndDistinct(Random source, int count) {

      LinkedHashSet<Long> longs = new LinkedHashSet<Long>(count);

      for (int size = 0; size < count; size++) {
        long next;
        do {
          next = Math.abs(source.nextLong());
        } while (!longs.add(next));
      }

      long[] unboxed = toArray(longs);
      Arrays.sort(unboxed);
      return unboxed;
    }

    private long[] toArray(LinkedHashSet<Long> longLinkedHashSet) {
      long[] longs = new long[longLinkedHashSet.size()];
      int i = 0;
      for (Long n : longLinkedHashSet) {
        longs[i++] = n;
      }
      return longs;
    }
  }
}
