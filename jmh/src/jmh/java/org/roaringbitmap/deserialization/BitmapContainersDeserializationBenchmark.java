package org.roaringbitmap.deserialization;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@BenchmarkMode(Mode.Throughput)
public class BitmapContainersDeserializationBenchmark {

  private ByteBuffer buffer;

  @Setup
  public void prepare() throws IOException {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    Random r = new Random();
    for (int containerIndex = 0; containerIndex < 1000; containerIndex++) {
      for (int j = 0; j < 10_000; j++) {
        int value = r.nextInt(65536);
        bitmap.add(value + containerIndex * 65536);
      }
    }
    byte[] x = serialise(bitmap);
    buffer = ByteBuffer.allocate(x.length);
    bitmap.serialize(buffer);
  }

  @Benchmark
  public void deserialize(Blackhole blackhole) throws IOException {
    buffer.rewind();
    MutableRoaringBitmap l = new MutableRoaringBitmap();
    l.deserialize(buffer);
    blackhole.consume(buffer);
  }

  private static byte[] serialise(MutableRoaringBitmap input) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(input.serializedSizeInBytes());
         DataOutputStream dos = new DataOutputStream(bos)) {
      input.serialize(dos);
      return bos.toByteArray();
    }
  }
}