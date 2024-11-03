package org.roaringbitmap.iteration;

import static org.roaringbitmap.RoaringBitmapWriter.writer;

import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RandomData;
import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.BitSet;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgsPrepend = {
      "-XX:+UseG1GC",
      "-XX:-TieredCompilation",
      "-XX:+AlwaysPreTouch",
      "-ms4G",
      "-mx4G"
    })
public class Concatenation {

  /**
   * Using lots and lots of parameter sets is clever, but it takes a long
   * time to run, produces results that are difficult to interpret.
   * When we want a ballpark result... a single set of parameters is more than
   * enough.
   * Using a long, long time to run each and every benchmark makes you less
   * likely to run benchmarks which makes you less likely to work with hard
   * numbers.
   *
   */
  @Param({"16"})
  int count;

  @Param({"8"})
  int size;

  @Setup(Level.Trial)
  public void init() {
    bitSets =
        IntStream.range(0, count)
            .mapToObj(
                i -> {
                  RoaringBitmap rb = RandomData.randomContiguousBitmap(0, size, 0.1, 0.8);
                  return new BitSetWithOffset(rb, toBitSet(rb), i * size * 65536);
                })
            .toArray(BitSetWithOffset[]::new);
  }

  private BitSetWithOffset[] bitSets;

  private static class BitSetWithOffset {
    public final RoaringBitmap bitmap;
    public final BitSet bitset;
    public final int offset;

    public BitSetWithOffset(RoaringBitmap bitmap, BitSet bitset, int offset) {
      this.bitmap = bitmap;
      this.bitset = bitset;
      this.offset = offset;
    }
  }

  @Benchmark
  public BitSet bitset() {
    BitSet result = new BitSet();
    for (int i = 0; i < bitSets.length; ++i) {
      BitSetWithOffset bit = bitSets[i];
      int currentBit = bit.bitset.nextSetBit(0);
      while (currentBit != -1) {
        result.set(currentBit + bit.offset);
        currentBit = bit.bitset.nextSetBit(currentBit + 1);
      }
    }
    return result;
  }

  @Benchmark
  public RoaringBitmap roaringNaive() {
    RoaringBitmap result = new RoaringBitmap();
    for (int i = 0; i < bitSets.length; ++i) {
      BitSetWithOffset bit = bitSets[i];
      PeekableIntIterator peekableIter = bit.bitmap.getIntIterator();
      while (peekableIter.hasNext()) {
        int currentBit = peekableIter.next();
        result.add(currentBit + bit.offset);
      }
    }
    return result;
  }

  @Benchmark
  public RoaringBitmap roaringOffset() {
    RoaringBitmap result = new RoaringBitmap();
    for (int i = 0; i < bitSets.length; ++i) {
      BitSetWithOffset bit = bitSets[i];
      RoaringBitmap shifted = RoaringBitmap.addOffset(bit.bitmap, bit.offset);
      result.or(shifted);
    }
    return result;
  }

  @Benchmark
  public RoaringBitmap roaringBatchOrderedWriter() {
    int[] buffer = new int[256];
    RoaringBitmapWriter<RoaringBitmap> writer = writer().constantMemory().get();
    for (int i = 0; i < bitSets.length; ++i) {
      BitSetWithOffset bit = bitSets[i];
      RoaringBatchIterator iterator = bit.bitmap.getBatchIterator();
      while (iterator.hasNext()) {
        int count = iterator.nextBatch(buffer);
        for (int j = 0; j < count; ++j) {
          writer.add(buffer[j] + bit.offset);
        }
      }
    }
    writer.flush();
    return writer.getUnderlying();
  }

  private static BitSet toBitSet(RoaringBitmap rb) {
    BitSet bs = new BitSet();
    rb.forEach((IntConsumer) bs::set);
    return bs;
  }
}
