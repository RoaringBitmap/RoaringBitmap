package org.roaringbitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.SplittableRandom;

@State(Scope.Benchmark)
public class RadixSort {

  @Param({"23", "25"})
  int bits;

  @Param("100000000")
  int size;

  @Param("0")
  int seed;

  private int[] data;
  private int[] input;

  @Setup(Level.Trial)
  public void setup() {
    SplittableRandom random = new SplittableRandom(seed);
    data = new int[size];
    input = new int[size];
    int mask = (1 << bits) - 1;
    for (int i = 0; i < size; ++i) {
      // this means with the same seed the unmasked bits will be identical
      // which improves comparability
      int value = random.nextInt() & mask;
      data[i] = value;
      input[i] = value;
    }
  }

  @TearDown(Level.Invocation)
  public void restore() {
    System.arraycopy(input, 0, data, 0, input.length);
  }

  @Benchmark
  public int[] partialSort() {
    Util.partialRadixSort(data);
    return data;
  }
}
