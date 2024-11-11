package org.roaringbitmap;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.Integer.toUnsignedLong;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.RoaringBitmapWriter.bufferWriter;
import static org.roaringbitmap.RoaringBitmapWriter.writer;

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Supplier;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class TestRoaringBitmapWriter {

  public static Stream<Arguments> params() {
    return Stream.of(
        Arguments.of(writer().optimiseForArrays()),
        Arguments.of(writer().optimiseForRuns()),
        Arguments.of(writer().constantMemory()),
        Arguments.of(writer().optimiseForArrays().fastRank()),
        Arguments.of(writer().optimiseForRuns().fastRank()),
        Arguments.of(writer().constantMemory().fastRank()),
        Arguments.of(writer().expectedDensity(0.001)),
        Arguments.of(writer().expectedDensity(0.01)),
        Arguments.of(writer().expectedDensity(0.1)),
        Arguments.of(writer().expectedDensity(0.6)),
        Arguments.of(writer().expectedDensity(0.001).fastRank()),
        Arguments.of(writer().expectedDensity(0.01).fastRank()),
        Arguments.of(writer().expectedDensity(0.1).fastRank()),
        Arguments.of(writer().expectedDensity(0.6).fastRank()),
        Arguments.of(writer().initialCapacity(1)),
        Arguments.of(writer().initialCapacity(8)),
        Arguments.of(writer().initialCapacity(8192)),
        Arguments.of(writer().initialCapacity(1).fastRank()),
        Arguments.of(writer().initialCapacity(8).fastRank()),
        Arguments.of(writer().initialCapacity(8192).fastRank()),
        Arguments.of(writer().optimiseForArrays().expectedRange(0, toUnsignedLong(MIN_VALUE))),
        Arguments.of(writer().optimiseForRuns().expectedRange(0, toUnsignedLong(MIN_VALUE))),
        Arguments.of(writer().constantMemory().expectedRange(0, toUnsignedLong(MIN_VALUE))),
        Arguments.of(
            writer().optimiseForArrays().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()),
        Arguments.of(
            writer().optimiseForRuns().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()),
        Arguments.of(
            writer().constantMemory().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()),
        Arguments.of(
            writer()
                .optimiseForArrays()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))),
        Arguments.of(
            writer()
                .optimiseForRuns()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))),
        Arguments.of(
            writer()
                .constantMemory()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))),
        Arguments.of(
            writer()
                .optimiseForArrays()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .fastRank()),
        Arguments.of(
            writer()
                .optimiseForRuns()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .fastRank()),
        Arguments.of(
            writer()
                .constantMemory()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .fastRank()),
        Arguments.of(bufferWriter().optimiseForArrays()),
        Arguments.of(bufferWriter().optimiseForRuns()),
        Arguments.of(bufferWriter().constantMemory()),
        Arguments.of(bufferWriter().expectedDensity(0.001)),
        Arguments.of(bufferWriter().expectedDensity(0.01)),
        Arguments.of(bufferWriter().expectedDensity(0.1)),
        Arguments.of(bufferWriter().expectedDensity(0.6)),
        Arguments.of(bufferWriter().initialCapacity(1)),
        Arguments.of(bufferWriter().initialCapacity(8)),
        Arguments.of(bufferWriter().initialCapacity(8192)),
        Arguments.of(
            bufferWriter().optimiseForArrays().expectedRange(0, toUnsignedLong(MIN_VALUE))),
        Arguments.of(bufferWriter().optimiseForRuns().expectedRange(0, toUnsignedLong(MIN_VALUE))),
        Arguments.of(bufferWriter().constantMemory().expectedRange(0, toUnsignedLong(MIN_VALUE))),
        Arguments.of(
            bufferWriter()
                .optimiseForArrays()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))),
        Arguments.of(
            bufferWriter()
                .optimiseForRuns()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))),
        Arguments.of(
            bufferWriter()
                .constantMemory()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))),
        Arguments.of(writer().optimiseForArrays().runCompress(false)),
        Arguments.of(writer().optimiseForRuns().runCompress(false)),
        Arguments.of(writer().constantMemory().runCompress(false)),
        Arguments.of(writer().optimiseForArrays().fastRank().runCompress(false)),
        Arguments.of(writer().optimiseForRuns().fastRank().runCompress(false)),
        Arguments.of(writer().constantMemory().fastRank().runCompress(false)),
        Arguments.of(writer().expectedDensity(0.001).runCompress(false)),
        Arguments.of(writer().expectedDensity(0.01).runCompress(false)),
        Arguments.of(writer().expectedDensity(0.1).runCompress(false)),
        Arguments.of(writer().expectedDensity(0.6).runCompress(false)),
        Arguments.of(writer().expectedDensity(0.001).fastRank().runCompress(false)),
        Arguments.of(writer().expectedDensity(0.01).fastRank().runCompress(false)),
        Arguments.of(writer().expectedDensity(0.1).fastRank().runCompress(false)),
        Arguments.of(writer().expectedDensity(0.6).fastRank().runCompress(false)),
        Arguments.of(writer().initialCapacity(1).runCompress(false)),
        Arguments.of(writer().initialCapacity(8).runCompress(false)),
        Arguments.of(writer().initialCapacity(8192).runCompress(false)),
        Arguments.of(writer().initialCapacity(1).fastRank().runCompress(false)),
        Arguments.of(writer().initialCapacity(8).fastRank().runCompress(false)),
        Arguments.of(writer().initialCapacity(8192).fastRank().runCompress(false)),
        Arguments.of(
            writer()
                .optimiseForArrays()
                .expectedRange(0, toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            writer()
                .optimiseForRuns()
                .expectedRange(0, toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            writer()
                .constantMemory()
                .expectedRange(0, toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            writer()
                .optimiseForArrays()
                .expectedRange(0, toUnsignedLong(MIN_VALUE))
                .fastRank()
                .runCompress(false)),
        Arguments.of(
            writer()
                .optimiseForRuns()
                .expectedRange(0, toUnsignedLong(MIN_VALUE))
                .fastRank()
                .runCompress(false)),
        Arguments.of(
            writer()
                .constantMemory()
                .expectedRange(0, toUnsignedLong(MIN_VALUE))
                .fastRank()
                .runCompress(false)),
        Arguments.of(
            writer()
                .optimiseForArrays()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            writer()
                .optimiseForRuns()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            writer()
                .constantMemory()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            writer()
                .optimiseForArrays()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .fastRank()
                .runCompress(false)),
        Arguments.of(
            writer()
                .optimiseForRuns()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .fastRank()
                .runCompress(false)),
        Arguments.of(
            writer()
                .constantMemory()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .fastRank()
                .runCompress(false)),
        Arguments.of(bufferWriter().optimiseForArrays().runCompress(false)),
        Arguments.of(bufferWriter().optimiseForRuns().runCompress(false)),
        Arguments.of(bufferWriter().constantMemory().runCompress(false)),
        Arguments.of(bufferWriter().expectedDensity(0.001).runCompress(false)),
        Arguments.of(bufferWriter().expectedDensity(0.01).runCompress(false)),
        Arguments.of(bufferWriter().expectedDensity(0.1).runCompress(false)),
        Arguments.of(bufferWriter().expectedDensity(0.6).runCompress(false)),
        Arguments.of(bufferWriter().initialCapacity(1).runCompress(false)),
        Arguments.of(bufferWriter().initialCapacity(8).runCompress(false)),
        Arguments.of(bufferWriter().initialCapacity(8192).runCompress(false)),
        Arguments.of(
            bufferWriter()
                .optimiseForArrays()
                .expectedRange(0, toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            bufferWriter()
                .optimiseForRuns()
                .expectedRange(0, toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            bufferWriter()
                .constantMemory()
                .expectedRange(0, toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            bufferWriter()
                .optimiseForArrays()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            bufferWriter()
                .optimiseForRuns()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .runCompress(false)),
        Arguments.of(
            bufferWriter()
                .constantMemory()
                .expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))
                .runCompress(false)));
  }

  @ParameterizedTest
  @MethodSource("params")
  public void addInReverseOrder(
      Supplier<RoaringBitmapWriter<? extends BitmapDataProvider>> supplier) {
    RoaringBitmapWriter<? extends BitmapDataProvider> writer = supplier.get();
    writer.add(1 << 17);
    writer.add(0);
    writer.flush();
    assertArrayEquals(
        RoaringBitmap.bitmapOf(0, 1 << 17).toArray(), writer.getUnderlying().toArray());
  }

  @ParameterizedTest
  @MethodSource("params")
  public void bitmapShouldContainAllValuesAfterFlush(
      Supplier<RoaringBitmapWriter<? extends BitmapDataProvider>> supplier) {
    RoaringBitmapWriter<? extends BitmapDataProvider> writer = supplier.get();
    writer.add(0);
    writer.add(1 << 17);
    writer.flush();
    assertTrue(writer.getUnderlying().contains(0));
    assertTrue(writer.getUnderlying().contains(1 << 17));
  }

  @ParameterizedTest
  @MethodSource("params")
  public void newKeyShouldTriggerFlush(
      Supplier<RoaringBitmapWriter<? extends BitmapDataProvider>> supplier) {
    RoaringBitmapWriter<? extends BitmapDataProvider> writer = supplier.get();
    writer.add(0);
    writer.add(1 << 17);
    assertTrue(writer.getUnderlying().contains(0));
    writer.add(1 << 18);
    assertTrue(writer.getUnderlying().contains(1 << 17));
  }

  @ParameterizedTest
  @MethodSource("params")
  public void writeSameKeyAfterManualFlush(
      Supplier<RoaringBitmapWriter<? extends BitmapDataProvider>> supplier) {
    RoaringBitmapWriter<? extends BitmapDataProvider> writer = supplier.get();
    writer.add(0);
    writer.flush();
    writer.add(1);
    writer.flush();
    assertArrayEquals(RoaringBitmap.bitmapOf(0, 1).toArray(), writer.getUnderlying().toArray());
  }

  @ParameterizedTest
  @MethodSource("params")
  public void writeRange(Supplier<RoaringBitmapWriter<? extends BitmapDataProvider>> supplier) {
    RoaringBitmapWriter<? extends BitmapDataProvider> writer = supplier.get();
    writer.add(0);
    writer.add(65500L, 65600L);
    writer.add(1);
    writer.add(65610);
    writer.flush();
    RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 65610);
    expected.add(65500L, 65600L);
    assertArrayEquals(expected.toArray(), writer.getUnderlying().toArray());
  }

  @ParameterizedTest
  @MethodSource("params")
  public void testWriteToMaxKeyAfterFlush(
      Supplier<RoaringBitmapWriter<? extends BitmapDataProvider>> supplier) {
    RoaringBitmapWriter writer = supplier.get();
    writer.add(0);
    writer.add(-2);
    writer.flush();
    assertArrayEquals(RoaringBitmap.bitmapOf(0, -2).toArray(), writer.get().toArray());
    writer.add(-1);
    assertArrayEquals(RoaringBitmap.bitmapOf(0, -2, -1).toArray(), writer.get().toArray());
  }

  @ParameterizedTest
  @MethodSource("params")
  public void testWriteBitmapAfterReset(
      Supplier<RoaringBitmapWriter<? extends BitmapDataProvider>> supplier) {
    RoaringBitmapWriter writer = supplier.get();
    writer.add(0);
    writer.add(-2);
    assertArrayEquals(new int[] {0, -2}, writer.get().toArray());
    writer.reset();
    writer.add(100);
    writer.addMany(4, 5, 6);
    assertArrayEquals(new int[] {4, 5, 6, 100}, writer.get().toArray());
  }
}
