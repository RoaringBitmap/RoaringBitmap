package org.roaringbitmap.serialization;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SerializationBenchmark {

  @BenchmarkMode(Mode.AverageTime)
  @Benchmark
  public int testDeserialize(BenchmarkState benchmarkState) throws IOException {
    benchmarkState.presoutbb.rewind();
    try (ByteBufferBackedInputStream in =
        new ByteBufferBackedInputStream(benchmarkState.presoutbb)) {
      benchmarkState.bitmap_b.deserialize(new DataInputStream(in));
    }
    return benchmarkState.presoutbb.limit();
  }

  @BenchmarkMode(Mode.AverageTime)
  @Benchmark
  public int testMutableDeserializeMutable(BenchmarkState benchmarkState) throws IOException {
    benchmarkState.presoutbb.rewind();
    try (ByteBufferBackedInputStream in =
        new ByteBufferBackedInputStream(benchmarkState.presoutbb)) {
      benchmarkState.bitmap_br.deserialize(new DataInputStream(in));
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
    }
    return benchmarkState.outbb.limit();
  }

  @BenchmarkMode(Mode.AverageTime)
  @Benchmark
  public int testMutableSerialize(BenchmarkState benchmarkState) throws IOException {
    benchmarkState.outbb.rewind();
    try (ByteBufferBackedOutputStream out =
        new ByteBufferBackedOutputStream(benchmarkState.outbb)) {
      benchmarkState.bitmap_ar.serialize(new DataOutputStream(out));
    }
    return benchmarkState.outbb.limit();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    final RoaringBitmap bitmap_a;

    final RoaringBitmap bitmap_b = new RoaringBitmap();

    final MutableRoaringBitmap bitmap_ar;

    final MutableRoaringBitmap bitmap_br = new MutableRoaringBitmap();

    final ByteBuffer outbb;

    final ByteBuffer presoutbb;

    public BenchmarkState() {

      final int[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6l), 100000);
      bitmap_a = RoaringBitmap.bitmapOf(data);
      bitmap_ar = MutableRoaringBitmap.bitmapOf(data);
      for (int k = 100000; k < 200000; ++k) {
        bitmap_a.add(2 * k);
        bitmap_ar.add(2 * k);
      }
      outbb = ByteBuffer.allocate(bitmap_a.serializedSizeInBytes());
      presoutbb = ByteBuffer.allocate(bitmap_a.serializedSizeInBytes());
      ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(presoutbb);
      try {
        bitmap_a.serialize(new DataOutputStream(out));
      } catch (Exception e) {
        e.printStackTrace();
      }
      presoutbb.flip();
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
  }
}

class ByteBufferBackedInputStream extends InputStream {

  ByteBuffer buf;

  ByteBufferBackedInputStream(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public int read() throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    return 0xFF & buf.get();
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    int len = Math.min(bytes.length, buf.remaining());
    buf.get(bytes, 0, len);
    return len;
  }

  @Override
  public long skip(long n) {
    int len = Math.min((int) n, buf.remaining());
    buf.position(buf.position() + (int) n);
    return len;
  }

  @Override
  public int available() throws IOException {
    return buf.remaining();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public int read(byte[] bytes, int off, int len) throws IOException {
    len = Math.min(len, buf.remaining());
    buf.get(bytes, off, len);
    return len;
  }
}

class ByteBufferBackedOutputStream extends OutputStream {
  ByteBuffer buf;

  ByteBufferBackedOutputStream(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    buf.put((byte) b);
  }

  @Override
  public synchronized void write(byte[] bytes) throws IOException {
    buf.put(bytes);
  }

  @Override
  public synchronized void write(byte[] bytes, int off, int len) throws IOException {
    buf.put(bytes, off, len);
  }
}
