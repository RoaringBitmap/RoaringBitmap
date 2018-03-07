package org.roaringbitmap.buffer;

import org.junit.Assert;
import org.junit.Test;

import static org.roaringbitmap.RandomisedTestData.TestDataSet.testCase;

public class BufferParallelAggregationTest {

  @Test
  public void singleContainerOR() {
    ImmutableRoaringBitmap one = testCase().withRunAt(0).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap two = testCase().withBitmapAt(0).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap three = testCase().withArrayAt(0).build().toMutableRoaringBitmap();
    Assert.assertEquals(BufferFastAggregation.or(one, two, three), BufferParallelAggregation.or(one, two, three));
  }

  @Test
  public void twoContainerOR() {
    ImmutableRoaringBitmap one = testCase().withRunAt(0).withArrayAt(1).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap two = testCase().withBitmapAt(1).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap three = testCase().withArrayAt(1).build().toMutableRoaringBitmap();
    Assert.assertEquals(BufferFastAggregation.or(one, two, three), BufferParallelAggregation.or(one, two, three));
  }

  @Test
  public void disjointOR() {
    ImmutableRoaringBitmap one = testCase().withRunAt(0).withArrayAt(2).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap two = testCase().withBitmapAt(1).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap three = testCase().withArrayAt(3).build().toMutableRoaringBitmap();
    Assert.assertEquals(BufferFastAggregation.or(one, two, three), BufferParallelAggregation.or(one, two, three));
  }

  @Test
  public void disjointBigKeysOR() {
    ImmutableRoaringBitmap one = testCase().withRunAt(0).withArrayAt(2).withBitmapAt((1 << 15) | 1).build()
            .toMutableRoaringBitmap();
    ImmutableRoaringBitmap two = testCase().withBitmapAt(1).withRunAt((1 << 15) | 2).build()
            .toMutableRoaringBitmap();
    ImmutableRoaringBitmap three = testCase().withArrayAt(3).withRunAt((1 << 15) | 3).build()
            .toMutableRoaringBitmap();
    Assert.assertEquals(BufferFastAggregation.or(one, two, three), BufferParallelAggregation.or(one, two, three));
  }

  @Test
  public void singleContainerXOR() {
    ImmutableRoaringBitmap one = testCase().withRunAt(0).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap two = testCase().withBitmapAt(0).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap three = testCase().withArrayAt(0).build().toMutableRoaringBitmap();
    Assert.assertEquals(BufferFastAggregation.xor(one, two, three), BufferParallelAggregation.xor(one, two, three));
  }


  @Test
  public void missingMiddleContainerXOR() {
    ImmutableRoaringBitmap one = testCase().withRunAt(0).withBitmapAt(1).withArrayAt(2).build()
            .toMutableRoaringBitmap();
    ImmutableRoaringBitmap two = testCase().withBitmapAt(0).withArrayAt(2).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap three = testCase().withArrayAt(0).withBitmapAt(1).withArrayAt(2).build()
            .toMutableRoaringBitmap();
    Assert.assertEquals(BufferFastAggregation.xor(one, two, three), BufferParallelAggregation.xor(one, two, three));
  }

  @Test
  public void twoContainerXOR() {
    ImmutableRoaringBitmap one = testCase().withRunAt(0).withArrayAt(1).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap two = testCase().withBitmapAt(1).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap three = testCase().withArrayAt(1).build().toMutableRoaringBitmap();
    Assert.assertEquals(BufferFastAggregation.xor(one, two, three), BufferParallelAggregation.xor(one, two, three));
  }

  @Test
  public void disjointXOR() {
    ImmutableRoaringBitmap one = testCase().withRunAt(0).withArrayAt(2).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap two = testCase().withBitmapAt(1).build().toMutableRoaringBitmap();
    ImmutableRoaringBitmap three = testCase().withArrayAt(3).build().toMutableRoaringBitmap();
    Assert.assertEquals(BufferFastAggregation.xor(one, two, three), BufferParallelAggregation.xor(one, two, three));
  }

  @Test
  public void disjointBigKeysXOR() {
    ImmutableRoaringBitmap one = testCase().withRunAt(0).withArrayAt(2).withBitmapAt((1 << 15) | 1).build()
            .toMutableRoaringBitmap();
    ImmutableRoaringBitmap two = testCase().withBitmapAt(1).withRunAt((1 << 15) | 2).build()
            .toMutableRoaringBitmap();
    ImmutableRoaringBitmap three = testCase().withArrayAt(3).withRunAt((1 << 15) | 3).build()
            .toMutableRoaringBitmap();
    Assert.assertEquals(BufferFastAggregation.xor(one, two, three), BufferParallelAggregation.xor(one, two, three));
  }
}



