package org.roaringbitmap;

import org.openjdk.jmh.annotations.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

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

  private static final ThreadLocal<long[]> WORD_BLOCK = ThreadLocal.withInitial(() ->
      new long[BitSetUtil.BLOCK_LENGTH]);

  /*
    Given an uncompressed bitset represented as a byte array (basically, as read on wire)
    Below benchmarks the perf difference you will get when:
    1. ByteArrayToRoaring - Directly convert the byte array to a roaring bitmap by wrapping it in a ByteBuffer
    2. ByteArrayToBitsetToRoaring - Convert the byte array to a BitSet and then create the bitmap using it
    3. ByteArrayToRoaringWithCachedBuffer - Directly convert and use a cached reused buffer
   */

  @Benchmark
  public long ByteArrayToRoaring(Data d) {
    long bogus = 0;
    for (int i = 0; i < d.bitsetsAsBytes.length; i++) {
      ByteBuffer bb = ByteBuffer.wrap(d.bitsetsAsBytes[i]);
      bogus += BitSetUtil.bitmapOf(bb, false).getCardinality();
    }
    return bogus;
  }

  @Benchmark
  public long ByteArrayToRoaringWithCachedBuffer(Data d) {
    long bogus = 0;
    for (int i = 0; i < d.bitsetsAsBytes.length; i++) {
      ByteBuffer bb = ByteBuffer.wrap(d.bitsetsAsBytes[i]);
      bogus += BitSetUtil.bitmapOf(bb, false, WORD_BLOCK.get()).getCardinality();
    }
    return bogus;
  }


  @Benchmark
  public long ByteArrayToBitsetToRoaring(Data d) {
    long bogus = 0;
    for (int i = 0; i < d.bitsetsAsBytes.length; i++) {
      BitSet bitset = BitSet.valueOf(d.bitsetsAsBytes[i]);
      bogus += BitSetUtil.bitmapOf(bitset).getCardinality();
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
      for (int bitIndex = 0; word != 0 && bitIndex < Long.SIZE; word >>=
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
    byte[][] bitsetsAsBytes;

    @Setup
    public void setup() throws IOException {
      final String bitset = "/real-roaring-dataset/bitsets_1925630_96.gz";
      this.getClass().getResourceAsStream(bitset);
      this.bitsets = deserialize(bitset);
      this.bitsetsAsBytes = bitsetsAsBytes(bitsets);
    }

    private byte[][] bitsetsAsBytes(long[][] bitsets) {
      byte[][] bitsetsAsBytes = new byte[bitsets.length][];
      for (int i = 0; i < bitsets.length; i++) {
        long[] bitset = bitsets[i];
        bitsetsAsBytes[i] = BitSet.valueOf(bitset).toByteArray();
      }
      return bitsetsAsBytes;
    }

    private long[][] deserialize(final String bitsetResource) throws IOException {
      final DataInputStream dos = new DataInputStream(
          new GZIPInputStream(this.getClass().getResourceAsStream(bitsetResource)));
      try {
        /* Change this value to see number for small vs large bitsets
           wordSize = 64 represents 4096 bits (512 bytes)
           wordSize = 512 represents 32768 bits (~4kb)
           wordSize = 8192 represents 524288 bits (~64kb)
           wordSize = 131072 represents 8388608 bits (~8.3 million, ~1mb)
         */
        final int minTotalWordSize = 512;
        // Try to keep size of bitsets created below 1 gb
        final int bitsetCnt = Math.min((1024 * 1024 * 1024) / (minTotalWordSize * 8), dos.readInt());

        final long[][] bitset = new long[bitsetCnt][];
        for (int i = 0; i < bitsetCnt; i++) {
          final int wordSize = dos.readInt();

          // for duplication, to make bitsets wider
          final int clone = (minTotalWordSize + wordSize) / wordSize;
          final long[] words = new long[wordSize * (clone + 1)];
          for (int j = 0; j < wordSize; j++) {
            words[j] = dos.readLong();
          }

          // duplicate long[] n times to the right
          for(int j = 0; j < clone; j++) {
            System.arraycopy(words, 0, words, (j+1)*wordSize, wordSize);
          }
          bitset[i] = words;
        }
        return bitset;
      } finally {
        dos.close();
      }
    }
  }

}
