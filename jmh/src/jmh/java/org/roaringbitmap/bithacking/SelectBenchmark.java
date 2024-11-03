package org.roaringbitmap.bithacking;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SelectBenchmark {
  long[] bitmap;
  int[] key;

  @Setup
  public void setup() {
    Random r = new Random();
    final int N = 1024;
    bitmap = new long[N];
    key = new int[N];
    for (int k = 0; k < bitmap.length; ++k) {
      while (bitmap[k] == 0) bitmap[k] = r.nextInt();
    }
    for (int k = 0; k < key.length; ++k) key[k] = r.nextInt() % (Long.bitCount(bitmap[k]));
    for (int k = 0; k < 64; ++k) {
      if (select(0xFFFFFFFFFFFFFFFFL, k) != k) throw new RuntimeException("bug " + k);
    }
    for (int k = 0; k < 64; ++k) {
      if (selectBitPosition(0xFFFFFFFFFFFFFFFFL, k) != k) throw new RuntimeException("bug " + k);
    }

    for (int k = 0; k < bitmap.length; ++k)
      if (selectBitPosition(bitmap[k], key[k]) != select(bitmap[k], key[k]))
        throw new RuntimeException(
            "bug "
                + bitmap[k]
                + " "
                + key[k]
                + " : "
                + selectBitPosition(bitmap[k], key[k])
                + " : "
                + select(bitmap[k], key[k]));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int onefunction() {
    int answer = 0;
    for (int k = 0; k < bitmap.length; ++k) answer += selectBitPosition(bitmap[k], key[k]);
    return answer;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int manyfunctions() {
    int answer = 0;
    for (int k = 0; k < bitmap.length; ++k) answer += select(bitmap[k], key[k]);
    return answer;
  }

  public static int selectBitPosition(long w, int j) {
    int seen = 0;
    // Divide 64bit
    int part = (int) (w & 0xFFFFFFFF);
    int n = Integer.bitCount(part);
    if (n <= j) {
      part = (int) (w >>> 32);
      seen += 32;
      j -= n;
    }
    int ww = part;

    // Divide 32bit
    part = ww & 0xFFFF;

    n = Integer.bitCount(part);
    if (n <= j) {

      part = ww >>> 16;
      seen += 16;
      j -= n;
    }
    ww = part;

    // Divide 16bit
    part = ww & 0xFF;
    n = Integer.bitCount(part);
    if (n <= j) {
      part = ww >>> 8;
      seen += 8;
      j -= n;
    }
    ww = part;

    // Lookup in final byte
    int counter;
    for (counter = 0; counter < 8; counter++) {
      j -= (ww >>> counter) & 1;
      if (j < 0) {
        break;
      }
    }
    return seen + counter;
  }

  /**
   * Given a word w, return the position of the jth true bit.
   *
   * @param w word
   * @param j index
   * @return position of jth true bit in w
   */
  public static int select(long w, int j) {
    int part1 = (int) (w & 0xFFFFFFFF);
    int wfirsthalf = Integer.bitCount(part1);
    if (wfirsthalf > j) {
      return select(part1, j);
    } else {
      return select((int) (w >>> 32), j - wfirsthalf) + 32;
    }
  }

  /**
   * Given a word w, return the position of the jth true bit.
   *
   * @param w word
   * @param j index
   * @return position of jth true bit in w
   */
  public static int select(int w, int j) {
    int part1 = w & 0xFFFF;
    int wfirsthalf = Integer.bitCount(part1);
    if (wfirsthalf > j) {
      return select((short) part1, j);
    } else {
      return select((short) (w >>> 16), j - wfirsthalf) + 16;
    }
  }

  /**
   * Given a word w, return the position of the jth true bit.
   *
   * @param w word
   * @param j index
   * @return position of jth true bit in w
   */
  public static int select(short w, int j) {
    int sumtotal = 0;
    for (int counter = 0; counter < 16; ++counter) {
      sumtotal += (w >> counter) & 1;
      if (sumtotal > j) return counter;
    }
    throw new IllegalArgumentException(
        "cannot locate " + j + "th bit in " + w + " weight is " + Integer.bitCount(w));
  }
}
