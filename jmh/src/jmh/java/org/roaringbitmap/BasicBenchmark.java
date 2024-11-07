package org.roaringbitmap;

import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.zaxxer.sparsebits.SparseBitSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Borislav Ivanov on 4/2/15.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BasicBenchmark {

  @Benchmark
  public int bigunion_standard(StandardState state) {
    int bogus = 0;

    for (int k = 1, i = 0; k < state.N; k += 10, i++) {
      RoaringBitmap bitmapor = FastAggregation.horizontal_or(state.ors_standard.get(i));
      bogus += bitmapor.getCardinality();
    }

    return bogus;
  }

  @Benchmark
  public int bigunion_mutable(MutableState state) {
    int bogus = 0;

    for (int k = 1, i = 0; k < state.N; k += 10, i++) {
      MutableRoaringBitmap bitmapor = BufferFastAggregation.horizontal_or(state.ors_mutable.get(i));
      bogus += bitmapor.getCardinality();
    }

    return bogus;
  }

  @Benchmark
  public int bigunion_immutable(ImmutableState state) {
    int bogus = 0;

    for (int k = 1, i = 0; k < state.N; k += 10, i++) {
      MutableRoaringBitmap bitmapor =
          BufferFastAggregation.horizontal_or(state.ors_immutable.get(i));
      bogus += bitmapor.getCardinality();
    }

    return bogus;
  }

  @Benchmark
  public int toarray_standard(StandardState state) throws Exception {

    int bogus = 0;
    for (int k = 1; k < state.N * 100; ++k) {
      bogus += state.ewah_standard[k % state.N].toArray().length;
    }
    return bogus;
  }

  @Benchmark
  public int toarray_mutable(MutableState state) throws Exception {

    int bogus = 0;
    for (int k = 1; k < state.N * 100; ++k) {
      bogus += state.ewah_mutable[k % state.N].toArray().length;
    }
    return bogus;
  }

  @Benchmark
  public int toarray_immutable(ImmutableState state) throws Exception {

    int bogus = 0;
    for (int k = 1; k < state.N * 100; ++k) {
      bogus += state.ewah_immutable[k % state.N].toArray().length;
    }
    return bogus;
  }

  @Benchmark
  public int cardinality_standard(StandardState state) throws Exception {
    int bogus = 0;
    for (int k = 1; k < state.N * 100; ++k) {
      bogus += state.ewah_standard[k % state.N].getCardinality();
    }
    return bogus;
  }

  @Benchmark
  public int cardinality_mutable(MutableState state) throws Exception {
    int bogus = 0;
    for (int k = 1; k < state.N * 100; ++k) {
      bogus += state.ewah_mutable[k % state.N].getCardinality();
    }
    return bogus;
  }

  @Benchmark
  public int cardinality_immutable(ImmutableState state) throws Exception {
    int bogus = 0;
    for (int k = 1; k < state.N * 100; ++k) {
      bogus += state.ewah_immutable[k % state.N].getCardinality();
    }
    return bogus;
  }

  @Benchmark
  public RoaringBitmap createBitmapOrdered_standard() {

    RoaringBitmap r = new RoaringBitmap();

    for (int k = 0; k < 65536; k++) {
      r.add(k * 32);
    }
    return r;
  }

  @Benchmark
  public MutableRoaringBitmap createBitmapOrdered_mutable() {

    MutableRoaringBitmap r = new MutableRoaringBitmap();

    for (int k = 0; k < 65536; k++) {
      r.add(k * 32);
    }
    return r;
  }

  @Benchmark
  public RoaringBitmap createBitmapUnordered_standard() {

    RoaringBitmap r = new RoaringBitmap();

    for (int k = 65536 - 1; k >= 0; k--) {
      r.add(k * 32);
    }
    return r;
  }

  @Benchmark
  public RoaringBitmap createBitmapRange_standard() {
    RoaringBitmap r = new RoaringBitmap();
    r.add(0L, 300_000_000L);
    return r;
  }

  @Benchmark
  public MutableRoaringBitmap createBitmapRange_mutable() {
    MutableRoaringBitmap r = new MutableRoaringBitmap();
    r.add(0L, 300_000_000L);
    return r;
  }

  @Benchmark
  public MutableRoaringBitmap createBitmapUnordered_mutable() {

    MutableRoaringBitmap r = new MutableRoaringBitmap();

    for (int k = 65536 - 1; k >= 0; k--) {
      r.add(k * 32);
    }
    return r;
  }

  @Benchmark
  public SparseBitSet createSparseBitSetOrdered() {

    SparseBitSet r = new SparseBitSet();

    for (int k = 0; k < 65536; k++) {
      r.set(k * 32);
    }
    return r;
  }

  @Benchmark
  public SparseBitSet createSparseBitSetUnordered() {

    SparseBitSet r = new SparseBitSet();

    for (int k = 65536 - 1; k >= 0; k--) {
      r.set(k * 32);
    }
    return r;
  }

  @State(Scope.Benchmark)
  public static class StandardState {

    public final int N = 1000;
    public final int M = 1000;

    public final RoaringBitmap[] ewah_standard = new RoaringBitmap[N];

    private final List<RoaringBitmap[]> ors_standard;

    public StandardState() {

      /**
       * Standard
       */
      for (int k = 0; k < N; ++k) {
        ewah_standard[k] = new RoaringBitmap();
        for (int x = 0; x < M; ++x) {
          ewah_standard[k].add(x * (N - k + 2));
        }
        ewah_standard[k].trim();
      }

      ors_standard = new ArrayList<RoaringBitmap[]>();

      for (int k = 1; k < N; k += 10) {
        ors_standard.add(Arrays.copyOf(ewah_standard, k + 1));
      }
    }
  }

  @State(Scope.Benchmark)
  public static class MutableState {

    public final int N = 1000;
    public final int M = 1000;

    public final MutableRoaringBitmap[] ewah_mutable = new MutableRoaringBitmap[N];
    private final List<MutableRoaringBitmap[]> ors_mutable;

    public MutableState() {

      for (int k = 0; k < N; ++k) {
        ewah_mutable[k] = new MutableRoaringBitmap();
        for (int x = 0; x < M; ++x) {
          ewah_mutable[k].add(x * (N - k + 2));
        }
        ewah_mutable[k].trim();
      }

      ors_mutable = new ArrayList<MutableRoaringBitmap[]>();

      for (int k = 1; k < N; k += 10) {
        ors_mutable.add(Arrays.copyOf(ewah_mutable, k + 1));
      }
    }
  }

  @State(Scope.Benchmark)
  public static class ImmutableState {

    public final int N = 1000;
    public final int M = 1000;

    private final MutableRoaringBitmap[] ewah_mutable = new MutableRoaringBitmap[N];
    public final ImmutableRoaringBitmap[] ewah_immutable = new ImmutableRoaringBitmap[N];

    private final List<ImmutableRoaringBitmap[]> ors_immutable;

    public ImmutableState() {

      /**
       * Mutable & Immutable
       */
      for (int k = 0; k < N; ++k) {
        ewah_mutable[k] = new MutableRoaringBitmap();
        for (int x = 0; x < M; ++x) {
          ewah_mutable[k].add(x * (N - k + 2));
        }
        ewah_mutable[k].trim();
        try {
          ewah_immutable[k] = convertToMappedBitmap(ewah_mutable[k]);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      ors_immutable = new ArrayList<ImmutableRoaringBitmap[]>();
      for (int k = 1; k < N; k += 10) {
        ors_immutable.add(Arrays.copyOf(ewah_immutable, k + 1));
      }
    }

    private ImmutableRoaringBitmap convertToMappedBitmap(MutableRoaringBitmap orig)
        throws IOException {
      File tmpfile = File.createTempFile("roaring", ".bin");
      tmpfile.deleteOnExit();
      final FileOutputStream fos = new FileOutputStream(tmpfile);
      orig.serialize(new DataOutputStream(fos));
      long totalcount = fos.getChannel().position();
      fos.close();
      RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
      ByteBuffer bb =
          memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalcount);
      memoryMappedFile.close();
      return new ImmutableRoaringBitmap(bb);
    }
  }
}
