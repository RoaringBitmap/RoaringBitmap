package org.roaringbitmap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.jupiter.api.Assertions.*;
import static org.roaringbitmap.RangeBitmapTest.Distribution.*;

@Execution(ExecutionMode.CONCURRENT)
public class RangeBitmapTest {

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10001, 100_000, 0x110001, 1_000_000})
  public void testInsertContiguousValues(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, size).forEach(appender::add);
    RangeBitmap range = appender.build();
    assertEquals(RoaringBitmap.bitmapOfRange(0, size), range.lte(size));
    for (long upper = 1; upper < size; upper *= 10) {
      RoaringBitmap expected = RoaringBitmap.bitmapOfRange(0, upper + 1);
      assertEquals(expected, range.lte(upper));
      expected.flip(expected.last());
      assertEquals(expected, range.lt(upper));
    }
    for (long lower = 1; lower < size; lower *= 10) {
      RoaringBitmap expected = RoaringBitmap.bitmapOfRange(lower, size);
      assertEquals(expected, range.gte(lower));
      expected.flip(expected.first());
      assertEquals(expected, range.gt(lower));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10001, 100_000, 0x110001, 1_000_000})
  public void testInsertReversedContiguousValues(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, size).map(i -> size - i).forEach(appender::add);
    RangeBitmap range = appender.build();
    for (long upper = 1; upper < size; upper *= 10) {
      RoaringBitmap expected = RoaringBitmap.bitmapOfRange(size - upper, size);
      assertEquals(expected, range.lte(upper));
      expected.flip(expected.first());
      assertEquals(expected, range.lt(upper));
    }
    for (long lower = 1; lower < size; lower *= 10) {
      RoaringBitmap expected = RoaringBitmap.bitmapOfRange(0, size + 1 - lower);
      assertEquals(expected, range.gte(lower));
      expected.flip(expected.last());
      assertEquals(expected, range.gt(lower));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10001, 100_000, 0x110001, 1_000_000})
  public void testLessThanZeroEmpty(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, Math.min(size, 10)).forEach(appender::add);
    RangeBitmap range = appender.build();
    RoaringBitmap expected = new RoaringBitmap();
    assertEquals(expected, range.lt(0));
  }

  @Test
  public void testInsertContiguousValuesAboveRange() {
    RangeBitmap.Appender appender = RangeBitmap.appender(1_000_000);
    LongStream.range(0, 1_000_000).forEach(appender::add);
    RangeBitmap range = appender.build();
    RoaringBitmap expected = RoaringBitmap.bitmapOfRange(0, 1_000_000);
    assertEquals(expected, range.lte(999_999));
    assertEquals(expected, range.lte(1_000_000));
    assertEquals(expected, range.lt(1_000_000));
    assertEquals(expected, range.lte(1_000_000_000));
    assertEquals(expected, range.lt(1_000_000_000));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10001, 100_000, 0x110001, 1_000_000})
  public void testInsertContiguousValuesAboveRangeReversed(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, size).map(i -> size - i).forEach(appender::add);
    RangeBitmap range = appender.build();
    assertEquals(RoaringBitmap.bitmapOfRange(0, size), range.lte(size));
    assertEquals(RoaringBitmap.bitmapOfRange(0, size), range.lte(size + 1));
    assertEquals(RoaringBitmap.bitmapOfRange(0, size), range.lte(size * 10L));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10001, 100_000, 0x110001, 1_000_000})
  public void monotonicLTEResultCardinality(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, size).forEach(appender::add);
    RangeBitmap range = appender.build();
    int cardinality = 0;
    for (int i = size - 2; i <= size + 2; ++i) {
      int resultCardinality = range.lte(i).getCardinality();
      assertTrue(resultCardinality >= cardinality);
      cardinality = resultCardinality;
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10001, 100_000, 0x110001, 1_000_000})
  public void monotonicLTEResultCardinalityReversed(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, size).map(i -> size - i).forEach(appender::add);
    RangeBitmap range = appender.build();
    int cardinality = 0;
    for (int i = size - 2; i <= size + 2; ++i) {
      int resultCardinality = range.lte(i).getCardinality();
      assertTrue(resultCardinality >= cardinality);
      cardinality = resultCardinality;
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10001, 100_000, 0x110001, 1_000_000})
  public void monotonicGTResultCardinality(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, size).forEach(appender::add);
    RangeBitmap range = appender.build();
    int cardinality = size;
    for (int i = size - 2; i <= size + 2; ++i) {
      int resultCardinality = range.gt(i).getCardinality();
      assertTrue(resultCardinality <= cardinality);
      cardinality = resultCardinality;
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10001, 100_000, 0x110001, 1_000_000})
  public void monotonicGTResultCardinalityReversed(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, size).map(i -> size - i).forEach(appender::add);
    RangeBitmap range = appender.build();
    int cardinality = size;
    for (int i = size - 2; i <= size + 2; ++i) {
      int resultCardinality = range.gt(i).getCardinality();
      assertTrue(resultCardinality <= cardinality);
      cardinality = resultCardinality;
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10000, 0x10001, 100_000, 0x110001, 1_000_000})
  public void unionOfComplementsMatchesAll(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, size).forEach(appender::add);
    RangeBitmap range = appender.build();
    RoaringBitmap all = RoaringBitmap.bitmapOfRange(0, size);
    for (int i = size - 2; i <= size + 2; ++i) {
      assertEquals(all, RoaringBitmap.or(range.gte(i), range.lt(i)));
    }
  }

  @Test
  public void verifyBufferCleanerCalled() {
    AtomicInteger cleanerInvocations = new AtomicInteger();
    AtomicInteger supplierInvocations = new AtomicInteger();
    Consumer<ByteBuffer> cleaner = buffer -> cleanerInvocations.incrementAndGet();
    IntFunction<ByteBuffer> supplier = capacity -> {
      supplierInvocations.incrementAndGet();
      return ByteBuffer.allocate(capacity).order(LITTLE_ENDIAN);
    };
    RangeBitmap.Appender appender = RangeBitmap.appender(1_000_000, supplier, cleaner);
    LongStream.range(0, 1_000_000).forEach(appender::add);
    ByteBuffer target = ByteBuffer.allocate(appender.serializedSizeInBytes());
    appender.serialize(target);
    assertEquals(supplierInvocations.get() - 2, cleanerInvocations.get(),
        "two internal buffers remain active and uncleaned at any time, the rest must be cleaned");
    assertTrue(supplierInvocations.get() > 2, "this test requires more than 2 buffer allocations to ensure cleaning occurs");
  }

  @Test
  public void testSerializeToBigEndianBuffer() {
    RangeBitmap.Appender appender = RangeBitmap.appender(1_000_000);
    LongStream.range(0, 1_000_000).forEach(appender::add);
    ByteBuffer buffer = ByteBuffer.allocate(appender.serializedSizeInBytes());
    appender.serialize(buffer);
    buffer.flip();
    RangeBitmap bitmap = RangeBitmap.map(buffer);
  }

  @Test
  public void testSerializeBigSlices() {
    Random random = new Random(42);
    RangeBitmap.Appender appender = RangeBitmap.appender(-1L);
    IntStream.range(0, 1_000_000)
        .mapToDouble(i -> random.nextGaussian())
        .mapToLong(value -> {
          long bits = Double.doubleToLongBits(value);
          if ((bits & Long.MIN_VALUE) == Long.MIN_VALUE) {
            bits = bits == Long.MIN_VALUE ? Long.MIN_VALUE : ~bits;
          } else {
            bits ^= Long.MIN_VALUE;
          }
          return bits;
        }).forEach(appender::add);
    ByteBuffer buffer = ByteBuffer.allocate(appender.serializedSizeInBytes());
    appender.serialize(buffer);
    buffer.flip();
    RangeBitmap bitmap = RangeBitmap.map(buffer);
    assertFalse(bitmap.lte(Long.MIN_VALUE).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(longs = {1L, 0xFL, 0xFFL, 0xFFFL, 0xFFFFL, 
      0xFFFFFL, 0xFFFFFFL, 0xFFFFFFL, 0xFFFFFFFL, 0xFFFFFFFFL,
      0xFFFFFFFFFL, 0xFFFFFFFFFFL, 0xFFFFFFFFFFFL,
      0xFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFL,  0xFFFFFFFFFFFFFFL,
      0xFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL})
  public void testSerializeEmpty(long maxValue) {
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    ByteBuffer buffer = ByteBuffer.allocate(appender.serializedSizeInBytes());
    appender.serialize(buffer);
    buffer.flip();
    RangeBitmap bitmap = RangeBitmap.map(buffer);
    assertTrue(bitmap.lte(Long.MIN_VALUE).isEmpty());
  }

  @ParameterizedTest
  @MethodSource("distributions")
  public void testAppenderReuseAfterClear(LongSupplier dist) {
    RangeBitmap.Appender appender = RangeBitmap.appender(10_000_000);
    long[] values = new long[100_000];
    for (int i = 0; i < values.length; ++i) {
      values[i] = Math.min(10_000_000, dist.getAsLong());
    }
    for (long value : values) {
      appender.add(value);
    }
    RangeBitmap first = appender.build();
    appender.clear();
    for (long value : values) {
      appender.add(value);
    }
    RangeBitmap second = appender.build();
    // check the bitmaps answer queries identically
    for (int upper = 1; upper < 1_000_000; upper *= 10) {
      assertEquals(first.lte(upper), second.lte(upper));
      assertEquals(first.gt(upper), second.gt(upper));
    }
  }

  @ParameterizedTest
  @MethodSource("distributions")
  public void testConstructRelativeToMinValue(LongSupplier dist) {
    int[] values = IntStream.range(0, 1_000_000).map(i -> (int) dist.getAsLong()).toArray();
    int min = IntStream.of(values).min().orElse(0);
    int max = IntStream.of(values).max().orElse(Integer.MAX_VALUE) - min;
    RangeBitmap.Appender appender = RangeBitmap.appender(max);
    IntStream.of(values).map(i -> i - min).forEach(appender::add);
    RangeBitmap bitmap = appender.build();
    assertEquals(values.length, bitmap.lte(max).getCardinality());
    assertEquals(values.length, bitmap.gte(0).getCardinality());
  }

  public static Stream<Arguments> distributions() {
    return Stream.of(
       // NORMAL.of(42, 1_000, 100),
        NORMAL.of(42, 10_000, 10),
        NORMAL.of(42, 1_000_000, 1000),
        UNIFORM.of(42, 0, 1_000_000),
        UNIFORM.of(42, 500_000, 10_000_000),
        EXP.of(42, 0.0001),
        EXP.of(42, 0.9999),
        POINT.of(0, 0),
        POINT.of(0, Long.MAX_VALUE)
    ).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("distributions")
  public void testAgainstReferenceImplementation(LongSupplier dist) {
    long maxValue = 10_000_000;
    ReferenceImplementation.Builder builder = ReferenceImplementation.builder();
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    LongStream.range(0, 1_000_000)
        .map(i -> Math.min(dist.getAsLong(), maxValue))
        .forEach(v -> {
          builder.add(v);
          appender.add(v);
        });
    ReferenceImplementation referenceImplementation = builder.seal();
    RangeBitmap sut = appender.build();
    assertAll(LongStream.range(0, 7).map(i -> (long) Math.pow(10, i)).mapToObj(threshold -> () -> assertEquals(referenceImplementation.lessThanOrEqualTo(threshold), sut.lte(threshold))));
  }

  @ParameterizedTest
  @MethodSource("distributions")
  @SuppressWarnings("unchecked")
  public void testAgainstPrecomputed(LongSupplier dist) {
    long maxValue = 10_000_000;
    ReferenceImplementation.Builder builder = ReferenceImplementation.builder();
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    RoaringBitmapWriter<RoaringBitmap>[] recorders =
        LongStream.range(0, 7).map(i -> (long) Math.pow(10, i)).mapToObj(i -> RoaringBitmapWriter.writer().runCompress(true).get()).toArray(RoaringBitmapWriter[]::new);
    LongStream.range(0, 1_000_000)
        .forEach(i -> {
          long v = Math.min(dist.getAsLong(), maxValue);
          for (int p = 0; p < 7; ++p) {
            if (v <= (long) Math.pow(10, p)) {
              recorders[p].add((int) i);
            }
          }
          builder.add(v);
          appender.add(v);
        });
    RoaringBitmap[] precomputed = Arrays.stream(recorders).map(RoaringBitmapWriter::get).toArray(RoaringBitmap[]::new);
    RoaringBitmap all = RoaringBitmap.bitmapOfRange(0, 1_000_000);
    RangeBitmap sut = appender.build();
    assertAll(IntStream.range(0, 7).mapToObj(i -> () -> assertEquals(precomputed[i], sut.lte((long) Math.pow(10, i)))));
    assertAll(IntStream.range(0, 7).mapToObj(i -> () -> assertEquals(all, RoaringBitmap.or((sut.lte((long) Math.pow(10, i))), sut.gt((long) Math.pow(10, i))))));
    assertAll(IntStream.range(0, 7).mapToObj(i -> () -> assertEquals(all, RoaringBitmap.or((sut.lt((long) Math.pow(10, i))), sut.gte((long) Math.pow(10, i))))));
    assertAll(IntStream.range(0, 7).mapToObj(i -> () -> assertEquals(RoaringBitmap.andNot(all, precomputed[i]), sut.gt((long) Math.pow(10, i)))));
  }


  @ParameterizedTest
  @MethodSource("distributions")
  public void testContextualRangeEvaluationAgainstNonContextual(LongSupplier dist) {
    long maxValue = 10_000_000;
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    LongStream.range(0, 1_000_000)
        .forEach(i -> {
          long v = Math.min(dist.getAsLong(), maxValue);
          appender.add(v);
        });
    RangeBitmap sut = appender.build();
    IntStream.range(1, 8).forEach(i -> {
      long min = (long) Math.pow(10, i - 1);
      long max = (long) Math.pow(10, i);
      RoaringBitmap lte = sut.lte(max);
      RoaringBitmap gte = sut.gte(min);
      RoaringBitmap expected = RoaringBitmap.and(lte, gte);
      assertEquals(expected, sut.gte(min, lte));
      assertEquals(expected, sut.lte(max, gte));
      assertEquals(expected, sut.lt(max + 1, gte));
      assertEquals(expected, sut.gt(min - 1, lte));
    });
  }


  @ParameterizedTest
  @MethodSource("distributions")
  public void testDoubleEndedRangeEvaluationAgainstNonContextual(LongSupplier dist) {
    long maxValue = 10_000_000;
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    LongStream.range(0, 1_000_000)
        .forEach(i -> {
          long v = Math.min(dist.getAsLong(), maxValue);
          appender.add(v);
        });
    RangeBitmap sut = appender.build();
    IntStream.range(1, 8).forEach(i -> {
      long min = (long) Math.pow(10, i - 1);
      long max = (long) Math.pow(10, i);
      RoaringBitmap lte = sut.lte(max);
      RoaringBitmap gte = sut.gte(min);
      RoaringBitmap expected = RoaringBitmap.and(lte, gte);
      assertEquals(expected, sut.between(min, max));
    });
  }

  @Test
  public void testBetween1() {
    long maxValue = 10;
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    LongStream.range(0, maxValue).forEach(appender::add);
    RangeBitmap sut = appender.build();
    assertEquals(sut.between(0, 10), RoaringBitmap.bitmapOfRange(0, 10));
    assertEquals(sut.between(1, 10), RoaringBitmap.bitmapOfRange(1, 10));
    assertEquals(sut.between(1, 9), RoaringBitmap.bitmapOfRange(1, 10));
    assertEquals(sut.between(1, 9), RoaringBitmap.bitmapOfRange(1, 10));
    assertEquals(sut.between(2, 8), RoaringBitmap.bitmapOfRange(2, 9));
    assertEquals(sut.between(3, 7), RoaringBitmap.bitmapOfRange(3, 8));
  }

  @Test
  public void testBetween2() {
    long maxValue = 10 + 0x10000;
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    LongStream.range(0, maxValue).forEach(appender::add);
    RangeBitmap sut = appender.build();
    assertEquals(RoaringBitmap.bitmapOfRange(0, 11), sut.between(0, 10));
    assertEquals(RoaringBitmap.bitmapOfRange(1, 11), sut.between(1, 10));
    assertEquals(RoaringBitmap.bitmapOfRange(1, 10), sut.between(1, 9));
    assertEquals(RoaringBitmap.bitmapOfRange(1, 10), sut.between(1, 9));
    assertEquals(RoaringBitmap.bitmapOfRange(2, 9), sut.between(2, 8));
    assertEquals(RoaringBitmap.bitmapOfRange(3, 8), sut.between(3, 7));
    assertEquals(RoaringBitmap.bitmapOfRange(0x10000 - 5, 0x10000 + 6), sut.between(0x10000 - 5, 0x10000 + 5));
  }

  @Test
  public void testContextualEvaluationOnEmptyRange() {
    RangeBitmap empty = RangeBitmap.appender(10_000_000).build();
    RoaringBitmap nonEmpty = RoaringBitmap.bitmapOfRange(0, 100_000);
    assertEquals(new RoaringBitmap(), empty.lte(10, nonEmpty));
    assertEquals(new RoaringBitmap(), empty.lt(10, nonEmpty));
    assertEquals(new RoaringBitmap(), empty.gt(10, nonEmpty));
    assertEquals(new RoaringBitmap(), empty.gte(10, nonEmpty));
  }

  @Test
  public void testExtremeValues() {
    RangeBitmap.Appender appender = RangeBitmap.appender(-1L);
    appender.add(0L);
    appender.add(Long.MIN_VALUE);
    appender.add(-1L);
    RangeBitmap bitmap = appender.build();
    assertEquals(RoaringBitmap.bitmapOf(), bitmap.gt(-1L));
    assertEquals(RoaringBitmap.bitmapOf(2), bitmap.gte(-1L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1, 2), bitmap.lte(-1L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), bitmap.lte(-2L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), bitmap.lt(-1L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), bitmap.lt(-2L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), bitmap.lte(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(0), bitmap.lt(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(2), bitmap.gt(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(1, 2), bitmap.gte(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(0), bitmap.lte(0));
    assertEquals(RoaringBitmap.bitmapOf(), bitmap.lt(0));
    assertEquals(RoaringBitmap.bitmapOf(0, 1, 2), bitmap.gte(0));
    assertEquals(RoaringBitmap.bitmapOf(1, 2), bitmap.gt(0));
  }

  @Test
  public void testIndexDoubleValues() {
    // creates very large integer values so stresses edge cases in the top slice
    DoubleToLongFunction encoder = value -> {
      if (value == Double.NEGATIVE_INFINITY) {
        return 0;
      }
      if (value == Double.POSITIVE_INFINITY || Double.isNaN(value)) {
        return 0xFFFFFFFFFFFFFFFFL;
      }
      long bits = Double.doubleToLongBits(value);
      if ((bits & Long.MIN_VALUE) == Long.MIN_VALUE) {
        bits = bits == Long.MIN_VALUE ? Long.MIN_VALUE : ~bits;
      } else {
        bits ^= Long.MIN_VALUE;
      }
      return bits;
    };
    RangeBitmap.Appender appender = RangeBitmap.appender(-1L);
    double[] doubles = IntStream.range(0, 200).mapToDouble(i -> Math.pow(-1, i) * Math.pow(10, i)).toArray();
    Arrays.stream(doubles).mapToLong(encoder).forEach(appender::add);
    RangeBitmap bitmap = appender.build();
    for (double value : doubles) {
      RoaringBitmap expected = new RoaringBitmap();
      for (int j = 0; j < doubles.length; j++) {
        if (doubles[j] <= value) {
          expected.add(j);
        }
      }
      RoaringBitmap answer = bitmap.lte(encoder.applyAsLong(value));
      assertEquals(expected, answer);
    }

  }

  public static class ReferenceImplementation {

    public static Builder builder() {
      return new Builder();
    }

    ReferenceImplementation(long maxRid, RoaringBitmap[] bitmaps) {
      this.maxRid = maxRid;
      this.bitmaps = bitmaps;
    }

    private final long maxRid;
    private final RoaringBitmap[] bitmaps;

    public RoaringBitmap lessThanOrEqualTo(long threshold) {
      if (63 - Long.numberOfLeadingZeros(threshold) > bitmaps.length) {
        return all();
      }
      RoaringBitmap bitmap = (threshold & 1) == 0 ? bitmaps[0].clone() : all();
      long mask = 2;
      for (int i = 1; i < bitmaps.length; ++i) {
        if ((threshold & mask) != mask) {
          bitmap.and(bitmaps[i]);
        } else {
          bitmap.or(bitmaps[i]);
        }
        mask <<= 1;
      }
      return bitmap;
    }

    private RoaringBitmap all() {
      RoaringBitmap all = new RoaringBitmap();
      all.add(0, maxRid);
      return all;
    }

    private static final class Builder {

      private final RoaringBitmapWriter<RoaringBitmap>[] writers;
      private int rid = 0;
      private long mask = 0;

      @SuppressWarnings("unchecked")
      public Builder() {
        writers = new RoaringBitmapWriter[64];
        Arrays.setAll(writers, i -> RoaringBitmapWriter.writer().runCompress(true).get());
      }

      public void add(long value) {
        long bits = ~value;
        mask |= value;
        while (bits != 0) {
          int index = Long.numberOfTrailingZeros(bits);
          writers[index].add(rid);
          bits &= (bits - 1);
        }
        rid++;
      }

      public ReferenceImplementation seal() {
        int numDiscarded = Long.numberOfLeadingZeros(mask);
        RoaringBitmap[] bitmaps = new RoaringBitmap[writers.length - numDiscarded];
        for (int i = 0; i < bitmaps.length; ++i) {
          bitmaps[i] = writers[i].get();
        }
        return new ReferenceImplementation(rid, bitmaps);
      }
    }
  }

  public enum Distribution {
    UNIFORM {
      LongSupplier of(long seed, double... params) {
        long min = (long) params[0];
        long max = (long) params[1];
        SplittableRandom random = new SplittableRandom(seed);
        return () -> random.nextLong(min, max);
      }
    },
    NORMAL {
      LongSupplier of(long seed, double... params) {
        double mean = params[0];
        double stddev = params[1];
        Random random = new Random(seed);
        return () -> (long) (stddev * random.nextGaussian() + mean);
      }
    },
    EXP {
      LongSupplier of(long seed, double... params) {
        double rate = params[0];
        SplittableRandom random = new SplittableRandom(seed);
        return () -> (long) -(Math.log(random.nextDouble()) / rate);
      }
    },
    POINT {
      @Override
      LongSupplier of(long seed, double... params) {
        return () -> (long)params[0];
      }
    };

    abstract LongSupplier of(long seed, double... params);
  }
}
