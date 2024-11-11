package org.roaringbitmap.realdata;

import static org.roaringbitmap.RealDataset.CENSUS1881;
import static org.roaringbitmap.RealDataset.CENSUS1881_SRT;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME_SRT;
import static org.roaringbitmap.RealDataset.DIMENSION_003;
import static org.roaringbitmap.RealDataset.DIMENSION_008;
import static org.roaringbitmap.RealDataset.DIMENSION_033;
import static org.roaringbitmap.RealDataset.USCENSUS2000;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85_SRT;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES_SRT;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

/**
 *
 * @author Richard Startin
 *
 */
public class RealDataSerializationBenchmark {

  public void launchBenchmark() throws Exception {

    Options opt =
        new OptionsBuilder()
            // Specify which benchmarks to run.
            // You can be more specific if you'd like to run only one benchmark per test.
            .include(this.getClass().getName() + ".*")
            // Set the following options as needed
            .mode(Mode.AverageTime)
            .timeUnit(TimeUnit.MICROSECONDS)
            .warmupTime(TimeValue.seconds(1))
            .warmupIterations(2)
            .measurementTime(TimeValue.seconds(10))
            .measurementIterations(5)
            .threads(1)
            .forks(1)
            .shouldFailOnError(true)
            .shouldDoGC(true)
            // .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
            // .addProfiler(WinPerfAsmProfiler.class)
            .build();

    new Runner(opt).run();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({
      CENSUS_INCOME,
      CENSUS1881,
      DIMENSION_008,
      DIMENSION_003,
      DIMENSION_033,
      USCENSUS2000,
      WEATHER_SEPT_85,
      WIKILEAKS_NOQUOTES,
      CENSUS_INCOME_SRT,
      CENSUS1881_SRT,
      WEATHER_SEPT_85_SRT,
      WIKILEAKS_NOQUOTES_SRT
    })
    public String dataset;

    @Param({"true", "false"})
    boolean runOptimise;

    byte[][] buffers;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
      RoaringBitmap[] bitmaps =
          StreamSupport.stream(dataRetriever.fetchBitPositions().spliterator(), false)
              .map(RoaringBitmap::bitmapOf)
              .toArray(RoaringBitmap[]::new);
      buffers = new byte[bitmaps.length][];
      int i = 0;
      for (RoaringBitmap bitmap : bitmaps) {
        if (runOptimise) {
          bitmap.runOptimize();
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream(bitmap.serializedSizeInBytes());
        bitmap.serialize(new DataOutputStream(bos));
        buffers[i++] = bos.toByteArray();
      }
    }
  }

  @Benchmark
  public void bufferBackedDataInput(BenchmarkState state, Blackhole bh) throws IOException {
    byte[][] buffers = state.buffers;
    for (int i = 0; i < buffers.length; ++i) {
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(new BufferDataInput(ByteBuffer.wrap(state.buffers[i])));
      bh.consume(bitmap);
    }
  }

  @Benchmark
  public void streamBackedDataInputWithBuffer(BenchmarkState state, Blackhole bh)
      throws IOException {
    byte[][] buffers = state.buffers;
    for (int i = 0; i < buffers.length; ++i) {
      RoaringBitmap bitmap = new RoaringBitmap();
      ByteArrayInputStream bis = new ByteArrayInputStream(state.buffers[i]);
      bitmap.deserialize(new DataInputStream(bis), null);
      bh.consume(bitmap);
    }
  }

  @Benchmark
  public void streamBackedDataInput(BenchmarkState state, Blackhole bh) throws IOException {
    byte[][] buffers = state.buffers;
    for (int i = 0; i < buffers.length; ++i) {
      RoaringBitmap bitmap = new RoaringBitmap();
      ByteArrayInputStream bis = new ByteArrayInputStream(state.buffers[i]);
      bitmap.deserialize(new DataInputStream(bis));
      bh.consume(bitmap);
    }
  }

  @Benchmark
  public void directToBuffer(BenchmarkState state, Blackhole bh) throws IOException {
    byte[][] buffers = state.buffers;
    for (int i = 0; i < buffers.length; ++i) {
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(ByteBuffer.wrap(state.buffers[i]));
      bh.consume(bitmap);
    }
  }

  @Benchmark
  public void viaImmutable(BenchmarkState state, Blackhole bh) {
    byte[][] buffers = state.buffers;
    for (int i = 0; i < buffers.length; ++i) {
      RoaringBitmap bitmap =
          new ImmutableRoaringBitmap(ByteBuffer.wrap(state.buffers[i])).toRoaringBitmap();
      bh.consume(bitmap);
    }
  }

  public static class BufferDataInput implements DataInput {

    private final ByteBuffer data;

    public BufferDataInput(ByteBuffer data) {
      this.data = data;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      data.get(bytes);
    }

    @Override
    public void readFully(byte[] bytes, int i, int i1) throws IOException {
      data.get(bytes, i, i1);
    }

    @Override
    public int skipBytes(int i) throws IOException {
      data.position(data.position() + i);
      return data.position();
    }

    @Override
    public boolean readBoolean() throws IOException {
      return data.get() != 0;
    }

    @Override
    public byte readByte() throws IOException {
      return data.get();
    }

    @Override
    public int readUnsignedByte() throws IOException {
      return data.get() & 0xFF;
    }

    @Override
    public short readShort() throws IOException {
      return data.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
      return data.getShort() & 0xffff;
    }

    @Override
    public char readChar() throws IOException {
      return data.getChar();
    }

    @Override
    public int readInt() throws IOException {
      return data.getInt();
    }

    @Override
    public long readLong() throws IOException {
      return data.getLong();
    }

    @Override
    public float readFloat() throws IOException {
      return data.getFloat();
    }

    @Override
    public double readDouble() throws IOException {
      return data.getDouble();
    }

    @Override
    public String readLine() throws IOException {
      return null;
    }

    @Override
    public String readUTF() throws IOException {
      return null;
    }
  }
}
