package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.function.Supplier;

import static java.lang.Integer.*;
import static org.junit.Assert.assertEquals;
import static org.roaringbitmap.RoaringBitmapWriter.writer;

@RunWith(Parameterized.class)
public class TestRoaringBitmapWriter {


  @Parameterized.Parameters
  public static Object[][] params() {
    return new Object[][]
            {
                    {writer().optimiseForBitmaps()},
                    {writer().optimiseForArrays()},
                    {writer().optimiseForRuns()},
                    {writer().constantMemory()},
                    {writer().optimiseForBitmaps().fastRank()},
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
                    {writer().optimiseForBitmaps().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForArrays().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForRuns().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {writer().constantMemory().expectedRange(0, toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForBitmaps().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().optimiseForArrays().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().optimiseForRuns().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().constantMemory().expectedRange(0, toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().optimiseForBitmaps().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForArrays().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForRuns().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))},
                    {writer().constantMemory().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE))},
                    {writer().optimiseForBitmaps().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().optimiseForArrays().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().optimiseForRuns().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE)).fastRank()},
                    {writer().constantMemory().expectedRange(toUnsignedLong(MAX_VALUE), toUnsignedLong(MIN_VALUE)).fastRank()},
            };
  }

  private final Supplier<RoaringBitmapWriter<? extends RoaringBitmap>> supplier;

  public TestRoaringBitmapWriter(Supplier<RoaringBitmapWriter<? extends RoaringBitmap>> supplier) {
    this.supplier = supplier;
  }

  @Test
  public void addInReverseOrder() {
    RoaringBitmapWriter writer = supplier.get();
    writer.add(1 << 17);
    writer.add(0);
    writer.flush();
    assertEquals(RoaringBitmap.bitmapOf(0, 1 << 17), writer.getUnderlying());
  }

  @Test
  public void bitmapShouldContainAllValuesAfterFlush() {
    RoaringBitmapWriter writer = supplier.get();
    writer.add(0);
    writer.add(1 << 17);
    writer.flush();
    Assert.assertTrue(writer.getUnderlying().contains(0));
    Assert.assertTrue(writer.getUnderlying().contains(1 << 17));
  }


  @Test
  public void newKeyShouldTriggerFlush() {
    RoaringBitmapWriter writer = supplier.get();
    writer.add(0);
    writer.add(1 << 17);
    Assert.assertTrue(writer.getUnderlying().contains(0));
    writer.add(1 << 18);
    Assert.assertTrue(writer.getUnderlying().contains(1 << 17));
  }

  @Test
  public void writeSameKeyAfterManualFlush() {
    RoaringBitmapWriter writer = supplier.get();
    writer.add(0);
    writer.flush();
    writer.add(1);
    writer.flush();
    assertEquals(RoaringBitmap.bitmapOf(0, 1), writer.getUnderlying());
  }


  @Test
  public void writeRange() {
    RoaringBitmapWriter writer = supplier.get();
    writer.add(0);
    writer.add(65500L, 65600L);
    writer.add(1);
    writer.add(65610);
    writer.flush();
    RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 65610);
    expected.add(65500L, 65600L);
    assertEquals(expected, writer.getUnderlying());
  }

  @Test
  public void testWriteToMaxKeyAfterFlush() {
    RoaringBitmapWriter writer = supplier.get();
    writer.add(0);
    writer.add(-2);
    writer.flush();
    assertEquals(RoaringBitmap.bitmapOf(0, -2), writer.get());
    writer.add(-1);
    assertEquals(RoaringBitmap.bitmapOf(0, -2, -1), writer.get());
  }
}
