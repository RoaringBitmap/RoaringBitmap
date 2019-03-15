package org.roaringbitmap.realdata;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.stream.StreamSupport;

import static org.roaringbitmap.RealDataset.*;

public class RealDataSerializationBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {


    @Param({
            CENSUS_INCOME, CENSUS1881, DIMENSION_008,
            DIMENSION_003, DIMENSION_033, USCENSUS2000,
            WEATHER_SEPT_85, WIKILEAKS_NOQUOTES, CENSUS_INCOME_SRT, CENSUS1881_SRT, WEATHER_SEPT_85_SRT,
            WIKILEAKS_NOQUOTES_SRT
    })
    public String dataset;

    byte[][] buffers;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
      RoaringBitmap[] bitmaps = StreamSupport.stream(dataRetriever.fetchBitPositions().spliterator(), false)
              .map(RoaringBitmap::bitmapOf)
              .toArray(RoaringBitmap[]::new);
      buffers = new byte[bitmaps.length][];
      int i = 0;
      for (RoaringBitmap bitmap : bitmaps) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(bitmap.serializedSizeInBytes());
        bitmap.serialize(new DataOutputStream(bos));
        buffers[i++] = bos.toByteArray();
      }
    }
  }


  @Benchmark
  public void roaring(BenchmarkState state, Blackhole bh) throws IOException {
      byte[][] buffers = state.buffers;
      for (int i = 0; i < buffers.length; ++i) {
          RoaringBitmap bitmap = new RoaringBitmap();
          ByteArrayInputStream bis = new ByteArrayInputStream(state.buffers[i]);
          bitmap.deserialize(new DataInputStream(bis));
          bh.consume(bitmap);
      }
  }

  @Benchmark
  public void buffer(BenchmarkState state, Blackhole bh) {
    byte[][] buffers = state.buffers;
    for (int i = 0; i < buffers.length; ++i) {
      RoaringBitmap bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(state.buffers[i])).toRoaringBitmap();
      bh.consume(bitmap);
    }
  }
}
