package org.roaringbitmap;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BitSetUtilBenchmark {

  @Benchmark
  public long BitSetToRoaringByAddingBitByBit(Data d) {
    long bogus = 0;
    for (int i = 0; i < d.bitsets.length; i++) {
      bogus += bitmapTheNaiveWay(d.bitsets[i]).getCardinality();
    }
    return bogus;
  }


  @Benchmark
  public long BitSetToRoaringUsingBitSetUtil(Data d) {
    long bogus = 0;
    for (int i = 0; i < d.bitsets.length; i++) {
      bogus += BitSetUtil.bitmapOf(d.bitsets[i]).getCardinality();
    }
    return bogus;
  }


  private static RoaringBitmap bitmapTheNaiveWay(final long[] words) {
    int cardinality = 0;
    for (int i = 0; i < words.length; i++) {
      cardinality += Long.bitCount(words[i]);
    }

    RoaringBitmap bitmap = new RoaringBitmap();

    // for each word, unless we already have reached cardinality
    long word = 0;
    int index = 0;
    for (int i = 0, socket = 0; i < words.length && index < cardinality; i++, socket += Long.SIZE) {
      if (words[i] == 0)
        continue;

      // for each bit, unless updated word has become 0 (no more bits left) or we already have
      // reached cardinality
      word = words[i];
      for (int bitIndex = 0; word != 0 && bitIndex < Long.SIZE && index < cardinality; word >>=
          1, bitIndex++) {
        if ((word & 1l) != 0) {
          bitmap.add(socket + bitIndex);
        }
      }
    }
    return bitmap;
  }

  @State(Scope.Benchmark)
  public static class Data {
    long[][] bitsets;

    @Setup
    public void setup() throws IOException {
      final String bitset = "/real-roaring-dataset/bitsets_1925630_96.gz";
      this.getClass().getResourceAsStream(bitset);
      this.bitsets = deserialize(bitset);
    }

    private long[][] deserialize(final String bitsetResource) throws IOException {
      final DataInputStream dos = new DataInputStream(
          new GZIPInputStream(this.getClass().getResourceAsStream(bitsetResource)));
      try {
        final long[][] bitset = new long[dos.readInt()][];
        for (int i = 0; i < bitset.length; i++) {
          final int wordSize = dos.readInt();

          // for duplication, to make bitsets wider
          final int clone = 0;
          final long words[] = new long[wordSize * (clone + 1)];
          for (int j = 0; j < wordSize; j++) {
            words[j] = dos.readLong();
          }

          // duplicate long[] n times to the right
          for (int j = 0; j < clone; j++) {
            System.arraycopy(words, 0, words, (j + 1) * wordSize, wordSize);
          }
          bitset[i] = words;
        }
        return bitset;
      } finally {
        if (dos != null) {
          dos.close();
        }
      }
    }
  }


}
