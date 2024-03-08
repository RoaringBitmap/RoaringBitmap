package org.roaringbitmap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.DoubleToLongFunction;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.RangeBitmapTest.Distribution.EXP;
import static org.roaringbitmap.RangeBitmapTest.Distribution.NORMAL;
import static org.roaringbitmap.RangeBitmapTest.Distribution.POINT;
import static org.roaringbitmap.RangeBitmapTest.Distribution.UNIFORM;

/**
 * When contributing test, please be mindful of the computational requirements.
 * We can use fuzzers to hunt for difficult-to-find bugs, but we should not
 * abuse the continuous-integration infrastructure.
 */

@Execution(ExecutionMode.CONCURRENT)
public class RangeBitmapTest {

  @Test
  public void betweenRegressionTest() throws IOException {
    String[] lines = new String(Files.readAllBytes(Paths.get("src/test/resources/testdata/rangebitmap_regression.txt"))).split(",");
    RangeBitmap.Appender appender = RangeBitmap.appender(2175288L);
    for (String line : lines) {
        appender.add(Long.parseLong(line));
    }
    RangeBitmap bitmap = appender.build();
    for (int i = 0; i < 4; i++) {
      long lowerTs = 263501 + i;
      RoaringBitmap eqLower = bitmap.eq(lowerTs); // eq
      RoaringBitmap eqUpper = bitmap.eq(lowerTs + 1); // eq
      // [x,y] both inclusive, so it should be union of the two above
      RoaringBitmap range = bitmap.between(lowerTs, lowerTs + 1);
      assertEquals(RoaringBitmap.or(eqLower, eqUpper), range);
    }
  }

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
      assertEquals(expected.getCardinality(), range.lteCardinality(upper));
      expected.flip(expected.last());
      assertEquals(expected, range.lt(upper));
      assertEquals(expected.getCardinality(), range.ltCardinality(upper));
      assertEquals(RoaringBitmap.bitmapOf((int) upper), range.eq(upper));
    }
    for (long lower = 1; lower < size; lower *= 10) {
      RoaringBitmap expected = RoaringBitmap.bitmapOfRange(lower, size);
      assertEquals(expected, range.gte(lower));
      assertEquals(expected.getCardinality(), range.gteCardinality(lower));
      expected.flip(expected.first());
      assertEquals(expected, range.gt(lower));
      assertEquals(expected.getCardinality(), range.gtCardinality(lower));
      assertEquals(RoaringBitmap.bitmapOf((int) lower), range.eq(lower));
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
      assertEquals(expected.getCardinality(), range.lteCardinality(upper));
      expected.flip(expected.first());
      assertEquals(expected, range.lt(upper));
      assertEquals(expected.getCardinality(), range.ltCardinality(upper));
    }
    for (long lower = 1; lower < size; lower *= 10) {
      RoaringBitmap expected = RoaringBitmap.bitmapOfRange(0, size + 1 - lower);
      assertEquals(expected, range.gte(lower));
      assertEquals(expected.getCardinality(), range.gteCardinality(lower));
      expected.flip(expected.last());
      assertEquals(expected, range.gt(lower));
      assertEquals(expected.getCardinality(), range.gtCardinality(lower));
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
    assertEquals(expected.getCardinality(), range.ltCardinality(0));
  }

  @Test
  public void testInsertContiguousValuesAboveRange() {
    RangeBitmap.Appender appender = RangeBitmap.appender(1_000_000);
    LongStream.range(0, 1_000_000).forEach(appender::add);
    RangeBitmap range = appender.build();
    RoaringBitmap expected = RoaringBitmap.bitmapOfRange(0, 1_000_000);
    assertEquals(expected, range.lte(999_999));
    assertEquals(expected.getCardinality(), range.lteCardinality(999_999));
    assertEquals(expected, range.lte(1_000_000));
    assertEquals(expected.getCardinality(), range.lteCardinality(1_000_000));
    assertEquals(expected, range.lt(1_000_000));
    assertEquals(expected.getCardinality(), range.ltCardinality(1_000_000));
    assertEquals(expected, range.lte(1_000_000_000));
    assertEquals(expected.getCardinality(), range.lteCardinality(1_000_000_000));
    assertEquals(expected, range.lt(1_000_000_000));
    assertEquals(expected.getCardinality(), range.ltCardinality(1_000_000_000));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0xFFFF, 0x10001, 100_000, 0x110001, 1_000_000})
  public void testInsertContiguousValuesAboveRangeReversed(int size) {
    RangeBitmap.Appender appender = RangeBitmap.appender(size);
    LongStream.range(0, size).map(i -> size - i).forEach(appender::add);
    RangeBitmap range = appender.build();
    assertEquals(RoaringBitmap.bitmapOfRange(0, size), range.lte(size));
    assertEquals(size, range.lteCardinality(size));
    assertEquals(RoaringBitmap.bitmapOfRange(0, size), range.lte(size + 1));
    assertEquals(size, range.lteCardinality(size + 1));
    assertEquals(RoaringBitmap.bitmapOfRange(0, size), range.lte(size * 10L));
    assertEquals(size, range.lteCardinality(size * 10L));
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
      assertEquals(resultCardinality, range.lteCardinality(i));
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
      assertEquals(resultCardinality, range.lteCardinality(i));
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
      assertEquals(resultCardinality, range.gtCardinality(i));
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
      assertEquals(resultCardinality, range.gtCardinality(i));
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
      assertEquals(all.getCardinality(), range.gteCardinality(i) + range.ltCardinality(i));
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
    assertTrue(bitmap.lteCardinality(Long.MIN_VALUE) > 0);
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
    assertEquals(0, bitmap.lteCardinality(Long.MIN_VALUE));
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
      assertEquals(first.lteCardinality(upper), second.lteCardinality(upper));
      assertEquals(first.gt(upper), second.gt(upper));
      assertEquals(first.gtCardinality(upper), second.gtCardinality(upper));
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
    assertEquals(values.length, bitmap.lteCardinality(max));
    assertEquals(values.length, bitmap.gte(0).getCardinality());
    assertEquals(values.length, bitmap.gteCardinality(0));
  }

  public static Stream<Arguments> distributions() {
    return Stream.of(
        NORMAL.of(42, 1_000, 100),
        NORMAL.of(42, 10_000, 10),
        NORMAL.of(42, 1_000_000, 1000),
        UNIFORM.of(42, 0, 1_000_000),
        UNIFORM.of(42, 500_000, 10_000_000),
        EXP.of(42, 0.0001),
        EXP.of(42, 0.9999),
        POINT.of(0, 0),
        POINT.of(0, 1),
        POINT.of(0, 2),
        POINT.of(0, 3),
        POINT.of(0, 4),
        POINT.of(0, 7),
        POINT.of(0, 8),
        POINT.of(0, 15),
        POINT.of(0, 31),
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
    assertAll(LongStream.range(0, 7)
        .map(i -> (long) Math.pow(10, i))
        .mapToObj(threshold -> () -> {
          assertEquals(referenceImplementation.lessThanOrEqualTo(threshold), sut.lte(threshold));
          assertEquals(referenceImplementation.lessThanOrEqualTo(threshold).getCardinality(), sut.lteCardinality(threshold));
        }));
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
    assertAll(IntStream.range(0, 7).mapToObj(i -> () -> {
      assertEquals(precomputed[i], sut.lte((long) Math.pow(10, i)));
      assertEquals(precomputed[i].getCardinality(), sut.lteCardinality((long) Math.pow(10, i)));
    }));
    assertAll(IntStream.range(0, 7).mapToObj(i -> () -> {
      assertEquals(all, RoaringBitmap.or((sut.lte((long) Math.pow(10, i))), sut.gt((long) Math.pow(10, i))));
      assertEquals(all.getCardinality(), sut.lteCardinality((long) Math.pow(10, i)) + sut.gtCardinality((long) Math.pow(10, i)));
    }));
    assertAll(IntStream.range(0, 7).mapToObj(i -> () -> {
      assertEquals(all, RoaringBitmap.or((sut.lt((long) Math.pow(10, i))), sut.gte((long) Math.pow(10, i))));
      assertEquals(all.getCardinality(), sut.ltCardinality((long) Math.pow(10, i)) + sut.gteCardinality((long) Math.pow(10, i)));
    }));
    assertAll(IntStream.range(0, 7).mapToObj(i -> () -> {
      assertEquals(RoaringBitmap.andNot(all, precomputed[i]), sut.gt((long) Math.pow(10, i)));
      assertEquals(RoaringBitmap.andNot(all, precomputed[i]).getCardinality(), sut.gtCardinality((long) Math.pow(10, i)));
    }));
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
      assertEquals(expected.getCardinality(), sut.gteCardinality(min, lte), "" + i);
      assertEquals(expected, sut.lte(max, gte));
      assertEquals(expected.getCardinality(), sut.lteCardinality(max, gte));
      assertEquals(expected, sut.lt(max + 1, gte));
      assertEquals(expected.getCardinality(), sut.ltCardinality(max + 1, gte));
      assertEquals(expected, sut.gt(min - 1, lte));
      assertEquals(expected.getCardinality(), sut.gtCardinality(min - 1, lte));
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
      assertEquals(expected.getCardinality(), sut.betweenCardinality(min, max));
    });
  }

  @Test
  public void testBetween1() {
    long maxValue = 10;
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    LongStream.range(0, maxValue).forEach(appender::add);
    RangeBitmap sut = appender.build();
    assertEquals(sut.between(0, 10), RoaringBitmap.bitmapOfRange(0, 10));
    assertEquals(sut.betweenCardinality(0, 10), 10);
    assertEquals(sut.between(1, 10), RoaringBitmap.bitmapOfRange(1, 10));
    assertEquals(sut.betweenCardinality(1, 10), 9);
    assertEquals(sut.between(1, 9), RoaringBitmap.bitmapOfRange(1, 10));
    assertEquals(sut.betweenCardinality(1, 9), 9);
    assertEquals(sut.between(1, 9), RoaringBitmap.bitmapOfRange(1, 10));
    assertEquals(sut.betweenCardinality(1, 9), 9);
    assertEquals(sut.between(2, 8), RoaringBitmap.bitmapOfRange(2, 9));
    assertEquals(sut.betweenCardinality(2, 8), 7);
    assertEquals(sut.between(3, 7), RoaringBitmap.bitmapOfRange(3, 8));
    assertEquals(sut.betweenCardinality(3, 7), 5);
  }

  @Test
  public void testBetween2() {
    long maxValue = 10 + 0x10000;
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    LongStream.range(0, maxValue).forEach(appender::add);
    RangeBitmap sut = appender.build();
    assertEquals(RoaringBitmap.bitmapOfRange(0, 11), sut.between(0, 10));
    assertEquals(11, sut.betweenCardinality(0, 10));
    assertEquals(RoaringBitmap.bitmapOfRange(1, 11), sut.between(1, 10));
    assertEquals(10, sut.betweenCardinality(1, 10));
    assertEquals(RoaringBitmap.bitmapOfRange(1, 10), sut.between(1, 9));
    assertEquals(9, sut.betweenCardinality(1, 9));
    assertEquals(RoaringBitmap.bitmapOfRange(1, 10), sut.between(1, 9));
    assertEquals(9, sut.betweenCardinality(1, 9));
    assertEquals(RoaringBitmap.bitmapOfRange(2, 9), sut.between(2, 8));
    assertEquals(7, sut.betweenCardinality(2, 8));
    assertEquals(RoaringBitmap.bitmapOfRange(3, 8), sut.between(3, 7));
    assertEquals(5, sut.betweenCardinality(3, 7));
    assertEquals(RoaringBitmap.bitmapOfRange(0x10000 - 5, 0x10000 + 6), sut.between(0x10000 - 5, 0x10000 + 5));
    assertEquals(11, RoaringBitmap.bitmapOfRange(0x10000 - 5, 0x10000 + 6).getCardinality());
    assertEquals(11, sut.betweenCardinality(0x10000 - 5, 0x10000 + 5));
  }

  @Test
  public void testBetween3() {
    long[] values = {-4616189618054758400L, 4601552919265804287L, -4586634745500139520L, 4571364728013586431L};
    RangeBitmap.Appender appender = RangeBitmap.appender(-4586634745500139520L);
    Arrays.stream(values).forEach(appender::add);
    int numSequentialValues = 1 << 20;
    LongStream.range(0, numSequentialValues).forEach(appender::add);
    RangeBitmap sut = appender.build();
    RoaringBitmap sequentialValues = RoaringBitmap.bitmapOfRange(4, numSequentialValues + 4);
    assertEquals(RoaringBitmap.bitmapOf(0), sut.between(-4620693217682128896L, -4616189618054758400L));
    assertEquals(1, sut.betweenCardinality(-4620693217682128896L, -4616189618054758400L));
    assertEquals(RoaringBitmap.bitmapOfRange(5, 47), sut.between(1, 42));
    assertEquals(42, sut.betweenCardinality(1, 42));
    assertEquals(RoaringBitmap.or(RoaringBitmap.bitmapOf(3), sequentialValues), sut.between(0, 4571364728013586431L));
    assertEquals(RoaringBitmap.or(RoaringBitmap.bitmapOf(3), sequentialValues).getCardinality(), sut.betweenCardinality(0, 4571364728013586431L));
    assertEquals(RoaringBitmap.or(RoaringBitmap.bitmapOf(1, 3), sequentialValues), sut.between(0, 4601552919265804287L));
    assertEquals(RoaringBitmap.or(RoaringBitmap.bitmapOf(1, 3), sequentialValues).getCardinality(), sut.betweenCardinality(0, 4601552919265804287L));
    assertEquals(RoaringBitmap.bitmapOf(0), sut.between(Long.MAX_VALUE, -4616189618054758400L));
    assertEquals(1, sut.betweenCardinality(Long.MAX_VALUE, -4616189618054758400L));
    assertEquals(RoaringBitmap.bitmapOf(0, 2), sut.between(Long.MAX_VALUE, -4586634745500139520L));
    assertEquals(2, sut.betweenCardinality(Long.MAX_VALUE, -4586634745500139520L));
    assertEquals(RoaringBitmap.bitmapOfRange(0, numSequentialValues + 4), sut.between(0, 0xFFFFFFFFFFFFFFFFL));
    assertEquals(RoaringBitmap.bitmapOfRange(0, numSequentialValues + 4).getCardinality(), sut.betweenCardinality(0, 0xFFFFFFFFFFFFFFFFL));
    assertEquals(RoaringBitmap.bitmapOfRange(0, 4), sut.between(4571364728013586431L, -4586634745500139520L));
    assertEquals(4, sut.betweenCardinality(4571364728013586431L, -4586634745500139520L));
    assertEquals(RoaringBitmap.bitmapOf(0, 2), sut.between(Long.MAX_VALUE, 0xFFFFFFFFFFFFFFFFL));
    assertEquals(2, sut.betweenCardinality(Long.MAX_VALUE, 0xFFFFFFFFFFFFFFFFL));
    assertEquals(RoaringBitmap.or(RoaringBitmap.bitmapOf(1, 3), sequentialValues), sut.between(0, Long.MAX_VALUE));
    assertEquals(RoaringBitmap.orCardinality(RoaringBitmap.bitmapOf(1, 3), sequentialValues), sut.betweenCardinality(0, Long.MAX_VALUE));
    assertEquals(new RoaringBitmap(), sut.between(-42, 0xFFFFFFFFFFFFFFFFL));
    assertEquals(0, sut.betweenCardinality(-42, 0xFFFFFFFFFFFFFFFFL));
  }

  @Test
  public void testBetween4() {
    long[] values = {-4616189618054758400L, 4601552919265804287L, -4586634745500139520L, 4571364728013586431L,
        -4556648864387432448L, 4541763675970600959L, -4526534890170089472L, 4511741717132607487L,
        -4496888740970496000L, 4481700220488384511L, -4466831549978902528L, 4452010031096791039L,
        -4436860832214679552L, 4421918433705197567L, -4407127634823086080L, 4392016835940974591L,
        -4377002437431492608L, 4362241638549381119L, -4347168339667269632L, 4332083628657787647L,
        -4317352126650676160L, 4302315448862314671L, -4287162073302051438L, 4272459181524432137L,
        -4257458266522935884L, 4242237835737300334L, -4227562883636919499L, 4212596893231971325L,
        -4197310978827808127L, 4182663311568480478L, -4167731427214848790L, 4152444271493337051L,
        -4137760542057730537L, 4122861964394837603L, -4107616442036749309L, 4092854650044723837L,
        -4077988598447005469L, 4062783733670385380L, -4047945708713107023L, 4033111420850910690L,
        -4017946260743693147L, 4003033789531285016L, -3988230520942059423L, 3973104134926055302L,
        -3958118962292622004L, 3943345985962156897L, -3928257465269603386L, 3913201295154700195L,
        -3898457901108180877L, 3883406358270559600L, -3868280854677658470L, 3853566349580304959L,
        -3838550917929140944L, 3823357705861632449L, -3808671412628698674L, 3793691245808059326L,
        -3778431912183317079L, 3763773169599230700L, -3748827441089650601L, 3733522980173203346L,
        -3718871697978100924L, 3703959600631664618L, -3688697178669147109L, 3673967073435426414L,
        -3659087819021747723L, 3643866450725177229L, -3629059369867805881L, 3614212188630648294L,
        -3599030911804729185L, 3584148659439886491L, -3569332799664175299L, 3554190674665064179L,
        -3539235012624956505L, 3524449740213939054L, -3509345849420695115L, 3494318498244586478L,
        -3479563096306902762L, 3464496543605325988L, -3449399183507341415L, 3434672951953772672L,
        -3419642862232339617L, 3404477134046585572L, -3389779389196254110L, 3374784907853867652L,
        -3359552413957401235L, 3344882488153199927L, -3329922780618476166L, 3314625085832642195L,
        -3299982327065677369L, 3285056578327499206L, -3269777092125304377L, 3255078982340978658L,
        -3240186396490052060L, 3224948363896921682L, -3210172528595600116L, 3195312328376755120L,
        -3180114777823726749L, 3165263038697213921L, -3150434465072198619L, 3135276447761457361L,
        -3120350583805656195L, 3105552895526177699L, -3090433484897357453L, 3075435233412954391L,
        -3060667706603726686L, 3045585997812719925L, -3030517055382416577L, 3015778983133980657L,
        -3000734092543963630L, 2985596115986804532L, -2970886807957891842L, 2955877872642278850L,
        -2940672479945612186L, 2925991261974827647L, -2911017439231874848L, 2895746210461470323L,
        -2881092424188076561L, 2866152891066862228L, -2850856174385872187L, 2836190371749287492L,
        -2821284324586802134L, 2806029465354223306L, -2791285180001867583L, 2776411833970953486L,
        -2761197851152838739L, 2746376922523362867L, -2731535511191248833L, 2716361446746634160L,
        -2701465671166845646L, 2686655446064028545L, -2671520364406035042L, 2656551496101331837L,
        -2641771726300562525L, 2626674713770128755L, -2611634465851251049L, 2596884437556387778L,
        -2581824601908336563L, 2566714647334991567L, -2551993663479489668L, 2536970133380640164L,
        -2521792105902541959L, 2507099485757353896L, -2492111410296396691L, 2476866905372250427L,
        -2462201984162915723L, 2447248532371775213L, -2431939108066722645L, 2417301236599432233L,
        -2402381596985847092L, 2387109747189393536L, -2372397319144302929L, 2357510699235361700L,
        -2342280124069713038L, 2327490306091863256L, -2312635931988238351L, 2297445664079235090L,
        -2282580269995175180L, 2267757385935804494L, -2252606480582119011L, 2237667281706838269L,
        -2222875149643809597L, 2207762684285551627L, -2192751410418844296L, 2177989309602243369L,
        -2162914383302020084L, 2147832723701497720L, -2133099950273986391L, 2118061683210125099L,
        -2102911287541423996L, 2088207154142320474L, -2073204687113968945L, 2057987166378687038L,
        -2043311001757325518L, 2028343495701151489L, -2013060423143036769L, 1998411571781188921L,
        -1983478207299406984L, 1968189201417707433L, -1953508941032453066L, 1938608917931913404L,
        -1923361588776766700L, 1908603184529225747L, -1893735721371305496L, 1878529092144433054L,
        -1863694375531377859L, 1848858709192421875L, -1833691825989254769L, 1818782585581752070L,
        -1803977970823815880L, 1788849902096923513L, -1773867884546405676L, 1759093593598059126L,
        -1744003429633153813L, 1728950340653910252L, -1714205662800866089L, 1699152515205088754L,
        -1684030020533730232L, 1669314261719067301L, -1654297262921266511L, 1639106989253701968L,
    };
    RangeBitmap.Appender appender = RangeBitmap.appender(0xFFFFFFFFFFFFFFFFL);
    Arrays.stream(values).forEach(appender::add);
    RangeBitmap sut = appender.build();
    assertEquals(RoaringBitmap.bitmapOf(0), sut.between(-4620693217682128896L, -4616189618054758400L));
    assertEquals(1, sut.betweenCardinality(-4620693217682128896L, -4616189618054758400L));
  }

  @ParameterizedTest
  @MethodSource("distributions")
  public void testContextualBetweenCardinality(LongSupplier dist) {
    long maxValue = 10_000_000;
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    long[] thresholds = new long[256];
    LongStream.range(0, 1_000_000)
        .forEach(i -> {
          long v = Math.min(dist.getAsLong(), maxValue);
          thresholds[(int)i & 255] = v;
          appender.add(v);
        });
    RangeBitmap sut = appender.build();
    long numRows = sut.gteCardinality(0L);
    RoaringBitmap context = new RoaringBitmap();
    for (int i = 0; i < numRows; i += 4) {
      context.add(i);
    }
    Arrays.sort(thresholds);
    for (int i = 0; i < thresholds.length; i += 2) {
      long min = thresholds[i];
      long max = thresholds[i+1];
      long contextualCardinality = sut.betweenCardinality(min, max, context);
      RoaringBitmap bitmap = sut.between(min, max);
      bitmap.and(context);
      assertEquals(bitmap.getLongCardinality(), contextualCardinality);
    }
  }

  @Test
  public void testContextualEvaluationOnEmptyRange() {
    RangeBitmap empty = RangeBitmap.appender(10_000_000).build();
    RoaringBitmap nonEmpty = RoaringBitmap.bitmapOfRange(0, 100_000);
    assertEquals(new RoaringBitmap(), empty.lte(10, nonEmpty));
    assertEquals(0, empty.lteCardinality(10, nonEmpty));
    assertEquals(new RoaringBitmap(), empty.lt(10, nonEmpty));
    assertEquals(0, empty.ltCardinality(10, nonEmpty));
    assertEquals(new RoaringBitmap(), empty.gt(10, nonEmpty));
    assertEquals(0, empty.gtCardinality(10, nonEmpty));
    assertEquals(new RoaringBitmap(), empty.gte(10, nonEmpty));
    assertEquals(0, empty.gteCardinality(10, nonEmpty));
  }

  @Test
  public void testExtremeValues() {
    RangeBitmap.Appender appender = RangeBitmap.appender(-1L);
    appender.add(0L);
    appender.add(Long.MIN_VALUE);
    appender.add(-1L);
    RangeBitmap bitmap = appender.build();
    assertEquals(RoaringBitmap.bitmapOf(), bitmap.gt(-1L));
    assertEquals(0, bitmap.gtCardinality(-1L));
    assertEquals(RoaringBitmap.bitmapOf(2), bitmap.gte(-1L));
    assertEquals(1, bitmap.gteCardinality(-1L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1, 2), bitmap.lte(-1L));
    assertEquals(3, bitmap.lteCardinality(-1L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), bitmap.lte(-2L));
    assertEquals(2, bitmap.lteCardinality(-2L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), bitmap.lt(-1L));
    assertEquals(2, bitmap.ltCardinality(-1L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), bitmap.lt(-2L));
    assertEquals(2, bitmap.ltCardinality(-2L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), bitmap.lte(Long.MIN_VALUE));
    assertEquals(2, bitmap.lteCardinality(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(0), bitmap.lt(Long.MIN_VALUE));
    assertEquals(1, bitmap.ltCardinality(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(2), bitmap.gt(Long.MIN_VALUE));
    assertEquals(1, bitmap.gtCardinality(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(1, 2), bitmap.gte(Long.MIN_VALUE));
    assertEquals(2, bitmap.gteCardinality(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(0), bitmap.lte(0));
    assertEquals(1, bitmap.lteCardinality(0));
    assertEquals(RoaringBitmap.bitmapOf(), bitmap.lt(0));
    assertEquals(0, bitmap.ltCardinality(0));
    assertEquals(RoaringBitmap.bitmapOf(0, 1, 2), bitmap.gte(0));
    assertEquals(3, bitmap.gteCardinality(0));
    assertEquals(RoaringBitmap.bitmapOf(1, 2), bitmap.gt(0));
    assertEquals(2, bitmap.gtCardinality(0));
    assertEquals(RoaringBitmap.bitmapOf(), bitmap.eq(2L));
    assertEquals(RoaringBitmap.bitmapOf(0, 1, 2), bitmap.neq(2L));
    assertEquals(RoaringBitmap.bitmapOf(0), bitmap.eq(0L));
    assertEquals(RoaringBitmap.bitmapOf(1), bitmap.eq(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(2), bitmap.eq(-1L));
    assertEquals(RoaringBitmap.bitmapOf(1, 2), bitmap.neq(0L));
    assertEquals(RoaringBitmap.bitmapOf(0, 2), bitmap.neq(Long.MIN_VALUE));
    assertEquals(RoaringBitmap.bitmapOf(0, 1), bitmap.neq(-1L));
  }

  // creates very large integer values so stresses edge cases in the top slice
  private static final DoubleToLongFunction DOUBLE_ENCODER = value -> {
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

  @Test
  public void testIndexDoubleValues() {
    RangeBitmap.Appender appender = RangeBitmap.appender(-1L);
    double[] doubles = IntStream.range(0, 200).mapToDouble(i -> Math.pow(-1, i) * Math.pow(10, i)).toArray();
    Arrays.stream(doubles).mapToLong(DOUBLE_ENCODER).forEach(appender::add);
    RangeBitmap bitmap = appender.build();
    for (double value : doubles) {
      RoaringBitmap expected = new RoaringBitmap();
      for (int j = 0; j < doubles.length; j++) {
        if (doubles[j] <= value) {
          expected.add(j);
        }
      }
      long threshold = DOUBLE_ENCODER.applyAsLong(value);
      RoaringBitmap answer = bitmap.lte(threshold);
      assertEquals(expected, answer);
      assertEquals(expected.getCardinality(), bitmap.lteCardinality(threshold));
    }
  }

  @Test
  public void testBetweenDoubleValues() {
    RangeBitmap.Appender appender = RangeBitmap.appender(-1L);
    double[] doubles = IntStream.range(0, 200).mapToDouble(i -> Math.pow(-1, i) * Math.pow(10, i)).toArray();
    Arrays.stream(doubles).mapToLong(DOUBLE_ENCODER).forEach(appender::add);
    RangeBitmap bitmap = appender.build();
    for (double value : doubles) {
      RoaringBitmap expected = new RoaringBitmap();
      for (int j = 0; j < doubles.length; j++) {
        if (doubles[j] <= value && doubles[j] >= value / 2) {
          expected.add(j);
        }
      }
      long min = DOUBLE_ENCODER.applyAsLong(value / 2);
      long max = DOUBLE_ENCODER.applyAsLong(value);
      RoaringBitmap answer = bitmap.between(min, max);
      assertEquals(expected, answer);
      assertEquals(expected.getCardinality(), bitmap.betweenCardinality(min, max));
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 2, 3, 4, 7, 8, 15, 16,  31, 32, 63, 64})
  public void extremelySmallBitmapTest(long value) {
    RangeBitmap.Appender accumulator = RangeBitmap.appender(value);
    accumulator.add(value);
    assertEquals(accumulator.build().gte(value).getCardinality(), 1);
    assertEquals(accumulator.build().gteCardinality(value), 1);
    assertEquals(accumulator.build().lte(value).getCardinality(), 1);
    assertEquals(accumulator.build().lteCardinality(value), 1);
    assertEquals(accumulator.build().between(value, value).getCardinality(), 1);
    assertEquals(accumulator.build().betweenCardinality(value, value), 1);
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 2, 3, 4, 7, 8, 15, 16,  31, 32, 63, 64})
  public void testModulo65536(long value) {
    int count = 65537;
    RangeBitmap.Appender accumulator = RangeBitmap.appender(value);
    for (int i = 0; i < count; i++) {
      accumulator.add(value);
    }
    assertEquals(accumulator.build().gte(value).getCardinality(), count);
    assertEquals(accumulator.build().gteCardinality(value), count);
    assertEquals(accumulator.build().lte(value).getCardinality(), count);
    assertEquals(accumulator.build().lteCardinality(value), count);
    assertEquals(accumulator.build().between(value, value).getCardinality(), count);
    assertEquals(accumulator.build().betweenCardinality(value, value), count);
  }
  @Test
  public void regressionTestIssue586() {
    // see https://github.com/RoaringBitmap/RoaringBitmap/issues/586
    assertAll(
            () -> regresssionTestIssue586(0x0FFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFF0L, 0xFFFFFFFFFFFFFF0L),
            () -> regresssionTestIssue586(0x0FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFF0L, 0xFFFFFFFFFFFFFFF0L),
            () -> regresssionTestIssue586(0x0FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFF1L, 0xFFFFFFFFFFFFFFF0L),
            () -> regresssionTestIssue586(0, 10_000_000_000L, 10_000_000L)
    );
  }

  @Test
  public void regressionTestIssue588() {
    // see https://github.com/RoaringBitmap/RoaringBitmap/issues/588
    int valueInBitmap = 27470832;
    int baseValue = 27597418;
    int minValueThatWorks = 27459584;

    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(valueInBitmap);
    assertTrue(bitmap.intersects(minValueThatWorks, baseValue));
    assertTrue(bitmap.intersects(minValueThatWorks-1, baseValue));
  }

  private static void regresssionTestIssue586(long low, long high, long value) {
    RangeBitmap.Appender appender = RangeBitmap.appender(0xFFFFFFFFFFFFFFFFL);
    appender.add(value);
    RangeBitmap rangeBitmap = appender.build();
    assertEquals(rangeBitmap.gte(low, rangeBitmap.lte(high)), rangeBitmap.between(low, high));
    assertEquals(1, rangeBitmap.between(low, high).getCardinality());
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 10, 100, 1000})
  public void testEq(int max) {
    RangeBitmap.Appender appender = RangeBitmap.appender(max);
    RoaringBitmap[] expected = new RoaringBitmap[max];
    Arrays.setAll(expected, i -> new RoaringBitmap());
    for (int i = 0; i < 100_000; i++) {
      appender.add(i % max);
      expected[i % max].add(i);
    }
    RangeBitmap bitmap = appender.build();
    for (int offset = 0; offset < max; offset++) {
      assertEquals(expected[offset], bitmap.eq(offset));
      assertEquals(expected[offset].getLongCardinality(), bitmap.eqCardinality(offset));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 10, 100, 1000})
  public void testEqWithContext(int max) {
    RangeBitmap.Appender appender = RangeBitmap.appender(max);
    RoaringBitmap[] expected = new RoaringBitmap[max];
    Arrays.setAll(expected, i -> new RoaringBitmap());
    int maxRow = 100_000;
    for (int i = 0; i < maxRow; i++) {
      appender.add(i % max);
      expected[i % max].add(i);
    }
    RangeBitmap bitmap = appender.build();
    for (int offset = 0; offset < max; offset++) {
      assertEquals(expected[offset], bitmap.eq(offset, expected[offset]));
      assertEquals(expected[offset].getLongCardinality(), bitmap.eqCardinality(offset, expected[offset]));
      assertTrue(bitmap.eq(offset, expected[(offset + 1) % max]).isEmpty());
      assertEquals(0L, bitmap.eqCardinality(offset, expected[(offset + 1) % max]));
      assertTrue(bitmap.eq(offset, new RoaringBitmap()).isEmpty());
      assertEquals(0L, bitmap.eqCardinality(offset, new RoaringBitmap()));
      assertTrue(bitmap.eq(offset, RoaringBitmap.bitmapOf(maxRow + 1)).isEmpty());
      assertEquals(0L, bitmap.eqCardinality(offset, RoaringBitmap.bitmapOf(maxRow + 1)));
      RoaringBitmap overlapOnlyWithLast = RoaringBitmap.bitmapOfRange(expected[offset].last(), 2 * maxRow);
      assertEquals(RoaringBitmap.bitmapOf(expected[offset].last()), bitmap.eq(offset, overlapOnlyWithLast));
      assertEquals(1L, bitmap.eqCardinality(offset, overlapOnlyWithLast));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 10, 100, 1000})
  public void testNeq(int max) {
    RangeBitmap.Appender appender = RangeBitmap.appender(max);
    RoaringBitmap[] expected = new RoaringBitmap[max];
    Arrays.setAll(expected, i -> new RoaringBitmap());
    for (int i = 0; i < 100_000; i++) {
      appender.add(i % max);
      expected[i % max].add(i);
    }
    for (RoaringBitmap bitmap : expected) {
      bitmap.flip(0, 100_000L);
    }
    RangeBitmap bitmap = appender.build();
    for (int offset = 0; offset < max; offset++) {
      assertEquals(expected[offset], bitmap.neq(offset));
      assertEquals(expected[offset].getLongCardinality(), bitmap.neqCardinality(offset));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 10, 100, 1000})
  public void testNeqWithContext(int max) {
    RangeBitmap.Appender appender = RangeBitmap.appender(max);
    RoaringBitmap[] expected = new RoaringBitmap[max];
    Arrays.setAll(expected, i -> new RoaringBitmap());
    int maxRow = 100_000;
    for (int i = 0; i < maxRow; i++) {
      appender.add(i % max);
      expected[i % max].add(i);
    }
    for (RoaringBitmap bitmap : expected) {
      bitmap.flip(0L, maxRow);
    }
    RangeBitmap bitmap = appender.build();
    for (int offset = 0; offset < max; offset++) {
      assertEquals(expected[offset], bitmap.neq(offset, expected[offset]));
      assertEquals(expected[offset].getLongCardinality(), bitmap.neqCardinality(offset, expected[offset]));
      assertTrue(bitmap.neq(offset, new RoaringBitmap()).isEmpty());
      assertEquals(0L, bitmap.neqCardinality(offset, new RoaringBitmap()));
      assertTrue(bitmap.neq(offset, RoaringBitmap.bitmapOf(maxRow + 1)).isEmpty());
      assertEquals(0L, bitmap.neqCardinality(offset, RoaringBitmap.bitmapOf(maxRow + 1)));
    }
  }

  @ParameterizedTest
  @MethodSource("distributions")
  @Disabled("expensive - run on an ad hoc basis")
  public void testEqualsNotEqualsRandom(LongSupplier dist) {
    RangeBitmap.Appender appender = RangeBitmap.appender(Long.MAX_VALUE);
    Map<Long, RoaringBitmap> valuesToTest = new HashMap<>();
    int max = 100_000;
    for (int i = 0; i < max; i++) {
      long value = dist.getAsLong();
      appender.add(value);
      valuesToTest.computeIfAbsent(value, l -> new RoaringBitmap()).add(i);
    }
    RangeBitmap bitmap = appender.build();
    for (Map.Entry<Long, RoaringBitmap> entry : valuesToTest.entrySet()) {
      assertEquals(entry.getValue(), bitmap.eq(entry.getKey()));
      entry.getValue().flip(0L, max);
      assertEquals(entry.getValue(), bitmap.neq(entry.getKey()));
    }
  }

  @ParameterizedTest
  @MethodSource("distributions")
  @Disabled("expensive - run on an ad hoc basis")
  public void testEqualsNotEqualsRandomWithContext(LongSupplier dist) {
    RangeBitmap.Appender appender = RangeBitmap.appender(Long.MAX_VALUE);
    Map<Long, RoaringBitmap> valuesToTest = new HashMap<>();
    int max = 100_000;
    for (int i = 0; i < max; i++) {
      long value = dist.getAsLong();
      appender.add(value);
      valuesToTest.computeIfAbsent(value, l -> new RoaringBitmap()).add(i);
    }
    RangeBitmap bitmap = appender.build();
    for (Map.Entry<Long, RoaringBitmap> entry : valuesToTest.entrySet()) {
      assertEquals(entry.getValue(), bitmap.eq(entry.getKey(), entry.getValue()));
      assertTrue(bitmap.neq(entry.getKey(), entry.getValue()).isEmpty());
      entry.getValue().flip(0L, max);
      assertEquals(entry.getValue(), bitmap.neq(entry.getKey(), entry.getValue()));
      assertTrue(bitmap.eq(entry.getKey(), entry.getValue()).isEmpty());
    }
  }

  @Test
  @Disabled("expensive - run on an ad hoc basis")
  public void triggerBufferOverflow() {
    RangeBitmap.Appender appender = RangeBitmap.appender(1);
    while (appender.serializedSizeInBytes() < (128 << 20)) {
      for (int i = 0; i < 0x10000; i++) {
        appender.add(i & 1);
      }
    }
    RangeBitmap bitmap = appender.build();
    System.err.println(bitmap.eqCardinality(0));
    System.err.println(bitmap.eqCardinality(1));
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
      if (63 - Long.numberOfLeadingZeros(threshold) >= bitmaps.length) {
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

  private static long rangeMaskOriginal(long maxValue) {
    int lz = Long.numberOfLeadingZeros(maxValue | 1);
    return lz == 0 ? -1L : (1L << (64 - lz)) - 1;
  }

  private static long rangeMaskOptimized(long maxValue) {
    int lz = Long.numberOfLeadingZeros(maxValue | 1);
    return -1L >>> lz;
  }

  @Test
  public void rangeMaskRandom() {
    Random r = new Random(0);
    for (int i = 0; i < 10_000; i++) {
      long value = r.nextLong();
      assertEquals(rangeMaskOriginal(value), rangeMaskOptimized(value));
    }
  }
  @Test
  public void rangeMaskExpressionSimplification() {
    for (int lz = 0; lz < 64; lz++) {
      long original = lz == 0 ? -1L : (1L << (64 - lz)) - 1;
      long simplified = -1L >>> lz;
      assertEquals(Long.toBinaryString(original), Long.toBinaryString(simplified), "lz=" + lz);
    }
  }

  private static long rangeMaskOriginal2(long sliceCount) {
    return sliceCount == 64 ? -1L : (1L << sliceCount) - 1;
  }

  private static long rangeMaskSimplified2(long sliceCount) {
    return -1L >>> (64 - sliceCount);
  }

  @Test
  public void rangeMaskRandom2() {
    Random r = new Random(0);
    for (int i = 0; i < 10_000; i++) {
      long value = (r.nextLong() & 63) + 1; // 1-64 are valid only
      assertEquals(rangeMaskOriginal2(value), rangeMaskSimplified2(value), "" + value);
    }
  }
  @Test
  public void rangeMaskExpressionSimplification2() {
    for (int sliceCount = 1; sliceCount <= 64; sliceCount++) {
      long original = sliceCount == 64 ? -1L : (1L << sliceCount) - 1;
      long simplified = -1L >>> (64 - sliceCount);
      assertEquals(Long.toBinaryString(original), Long.toBinaryString(simplified), "sliceCount=" + sliceCount);
    }
  }
}
