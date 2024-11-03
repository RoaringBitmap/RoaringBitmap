package org.roaringbitmap.needwork;

import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SlowMappedORaggregate1 {

  @Benchmark
  public MutableRoaringBitmap RoaringWithRun(BenchmarkState benchmarkState) {
    MutableRoaringBitmap answer = ImmutableRoaringBitmap.or(benchmarkState.rc.iterator());
    return answer;
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({ // putting the data sets in alpha. order
      "wikileaks-noquotes",
    })
    String dataset;

    public List<ImmutableRoaringBitmap> convertToImmutableRoaring(List<MutableRoaringBitmap> source)
        throws IOException {
      File tmpfile = File.createTempFile("roaring", "bin");
      tmpfile.deleteOnExit();
      final FileOutputStream fos = new FileOutputStream(tmpfile);
      final DataOutputStream dos = new DataOutputStream(fos);

      for (MutableRoaringBitmap rb1 : source) rb1.serialize(dos);

      final long totalcount = fos.getChannel().position();
      dos.close();
      final RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
      ByteBuffer out =
          memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalcount);
      ArrayList<ImmutableRoaringBitmap> answer =
          new ArrayList<ImmutableRoaringBitmap>(source.size());
      while (out.position() < out.limit()) {
        final ByteBuffer bb = out.slice();
        MutableRoaringBitmap equiv = source.get(answer.size());
        ImmutableRoaringBitmap newbitmap = new ImmutableRoaringBitmap(bb);
        if (!equiv.equals(newbitmap)) throw new RuntimeException("bitmaps do not match");
        answer.add(newbitmap);
        out.position(out.position() + newbitmap.serializedSizeInBytes());
      }
      memoryMappedFile.close();
      return answer;
    }

    List<ImmutableRoaringBitmap> rc;

    public BenchmarkState() {}

    @Setup
    public void setup() throws Exception {
      ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
      System.out.println();
      System.out.println("Loading files from " + dataRetriever.getName());
      ArrayList<MutableRoaringBitmap> tmprc = new ArrayList<MutableRoaringBitmap>();

      for (int[] data : dataRetriever.fetchBitPositions()) {
        MutableRoaringBitmap basic = MutableRoaringBitmap.bitmapOf(data);
        basic.runOptimize();
        tmprc.add(basic);
      }
      rc = convertToImmutableRoaring(tmprc);
    }
  }
}
