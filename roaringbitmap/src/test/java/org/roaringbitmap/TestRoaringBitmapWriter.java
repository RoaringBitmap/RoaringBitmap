package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.function.Supplier;

import static java.lang.Integer.*;
import static org.junit.Assert.assertArrayEquals;
import static org.roaringbitmap.RoaringBitmapWriter.bufferWriter;
import static org.roaringbitmap.RoaringBitmapWriter.writer;

@RunWith(Parameterized.class)
public class TestRoaringBitmapWriter {


  @Parameterized.Parameters
  public static Object[][] params() {
    return new Object[][]
            {
                    {writer().optimiseForArrays()},
                    {writer().optimiseForRuns()},
                    {writer().constantMemory()},
                    {writer().optimiseForArrays().fastRank()},
                    {writer().optimiseForRuns().fastRank()},
                    {writer().constantMemory().fastRank()},
                    {writer().expectedDensity(0.001)},
                    {writer().expectedDensity(0.01)},
                    {writer().expectedDensity(0.1)},
                    {writer().expectedDensity(0.6)},
                    {writer().expectedDensity(0.001).fastRank()},
                    {writer().expectedDensity(0.01).fastRank()},
                    {writer().expectedDensity(0.1).fastRank()},
                    {writer().expectedDensity(0.6).fastRank()},
                    {writer().initialCapacity(1)},
                    {writer().initialCapacity(8)},
                    {writer().initialCapacity(8192)},
                    {writer().initialCapacity(1).fastRank()},
                    {writer().initialCapacity(8).fastRank()},
                    {writer().initialCapacity(8192).fastRank()},
                    {writer().optimiseForArrays().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForRuns().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {writer().constantMemory().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForArrays().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().optimiseForRuns().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().constantMemory().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().optimiseForArrays().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForRuns().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))},
                    {writer().constantMemory().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForArrays().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().optimiseForRuns().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().constantMemory().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE)).fastRank()},
                    {bufferWriter().optimiseForArrays()},
                    {bufferWriter().optimiseForRuns()},
                    {bufferWriter().constantMemory()},
                    {bufferWriter().expectedDensity(0.001)},
                    {bufferWriter().expectedDensity(0.01)},
                    {bufferWriter().expectedDensity(0.1)},
                    {bufferWriter().expectedDensity(0.6)},
                    {bufferWriter().initialCapacity(1)},
                    {bufferWriter().initialCapacity(8)},
                    {bufferWriter().initialCapacity(8192)},
                    {bufferWriter().optimiseForArrays().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {bufferWriter().optimiseForRuns().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {bufferWriter().constantMemory().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {bufferWriter().optimiseForArrays().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))},
                    {bufferWriter().optimiseForRuns().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))},
                    {bufferWriter().constantMemory().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))}
            };
  }

  private final Supplier<RoaringBitmapWriter<? extends BitmapDataProvider>> supplier;

  public TestRoaringBitmapWriter(Supplier<RoaringBitmapWriter<? extends BitmapDataProvider>> supplier) {
    this.supplier = supplier;
  }

  @Test
  public void addInReverseOrder() {
    RoaringBitmapWriter<? extends BitmapDataProvider> writer = supplier.get();
    writer.add(1 << 17);
    writer.add(0);
    writer.flush();
    assertArrayEquals(RoaringBitmap.bitmapOf(0, 1 << 17).toArray(), writer.getUnderlying().toArray());
  }

  @Test
  public void bitmapShouldContainAllValuesAfterFlush() {
    RoaringBitmapWriter<? extends BitmapDataProvider> writer = supplier.get();
    writer.add(0);
    writer.add(1 << 17);
    writer.flush();
    Assert.assertTrue(writer.getUnderlying().contains(0));
    Assert.assertTrue(writer.getUnderlying().contains(1 << 17));
  }


  @Test
  public void newKeyShouldTriggerFlush() {
    RoaringBitmapWriter<? extends BitmapDataProvider> writer = supplier.get();
    writer.add(0);
    writer.add(1 << 17);
    Assert.assertTrue(writer.getUnderlying().contains(0));
    writer.add(1 << 18);
    Assert.assertTrue(writer.getUnderlying().contains(1 << 17));
  }

  @Test
  public void writeSameKeyAfterManualFlush() {
    RoaringBitmapWriter<? extends BitmapDataProvider> writer = supplier.get();
    writer.add(0);
    writer.flush();
    writer.add(1);
    writer.flush();
    assertArrayEquals(RoaringBitmap.bitmapOf(0, 1).toArray(), writer.getUnderlying().toArray());
  }


  @Test
  public void writeRange() {
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

  @Test
  public void testWriteToMaxKeyAfterFlush() {
    RoaringBitmapWriter writer = supplier.get();
    writer.add(0);
    writer.add(-2);
    writer.flush();
    assertArrayEquals(RoaringBitmap.bitmapOf(0, -2).toArray(), writer.get().toArray());
    writer.add(-1);
    assertArrayEquals(RoaringBitmap.bitmapOf(0, -2, -1).toArray(), writer.get().toArray());
  }


  @Test
  public void testWriteBitmapAfterReset() {
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
