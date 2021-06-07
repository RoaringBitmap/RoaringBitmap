package org.roaringbitmap;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.TestRangeConsumer.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.roaringbitmap.TestRangeConsumer.Value.ABSENT;
import static org.roaringbitmap.TestRangeConsumer.Value.PRESENT;


public class TestForAllInRange {

  @Test
  public void testContinuous() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(100L, 10000L);

    TestRangeConsumer consumer = TestRangeConsumer.validateContinuous(9900, PRESENT);
    bitmap.forAllInRange(100, 9900, consumer);
    assertEquals(9900, consumer.getNumberOfValuesConsumed());

    TestRangeConsumer consumer2 = TestRangeConsumer.validateContinuous(1000, ABSENT);
    bitmap.forAllInRange(10001, 1000, consumer2);
    assertEquals(1000, consumer2.getNumberOfValuesConsumed());

    TestRangeConsumer consumer3 = TestRangeConsumer.validate(new Value[]{
        ABSENT, ABSENT, PRESENT, PRESENT, PRESENT
    });
    bitmap.forAllInRange(98, 5, consumer3);
    assertEquals(5, consumer3.getNumberOfValuesConsumed());

    TestRangeConsumer consumer4 = TestRangeConsumer.validate(new Value[]{
        PRESENT, PRESENT, ABSENT, ABSENT, ABSENT
    });
    bitmap.forAllInRange(9998, 5, consumer4);
    assertEquals(5, consumer4.getNumberOfValuesConsumed());
  }

  @Test
  public void testDense() {
    RoaringBitmap bitmap = new RoaringBitmap();
    Value[] expected = new Value[100000];
    Arrays.fill(expected, ABSENT);
    for (int k = 0; k < 100000; k += 3) {
      bitmap.add(k);
      expected[k] = PRESENT;
    }

    TestRangeConsumer consumer = TestRangeConsumer.validate(expected);
    bitmap.forAllInRange(0, 100000, consumer);
    assertEquals(100000, consumer.getNumberOfValuesConsumed());

    Value[] expectedSubRange = Arrays.copyOfRange(expected,2500, 6000);
    TestRangeConsumer consumer2 = TestRangeConsumer.validate(expectedSubRange);
    bitmap.forAllInRange(2500, 3500, consumer2);
    assertEquals(1000, consumer2.getNumberOfValuesConsumed());

    TestRangeConsumer consumer3 = TestRangeConsumer.validate(new Value[]{
        expected[99997], expected[99998], expected[99999], ABSENT, ABSENT, ABSENT
    });
    bitmap.forAllInRange(99997, 6, consumer3);
    assertEquals(6, consumer3.getNumberOfValuesConsumed());
  }


  @Test
  public void testSparse() {
    RoaringBitmap bitmap = new RoaringBitmap();
    Value[] expected = new Value[100000];
    Arrays.fill(expected, ABSENT);
    for (int k = 0; k < 100000; k += 3000) {
      bitmap.add(k);
      expected[k] = PRESENT;
    }

    TestRangeConsumer consumer = TestRangeConsumer.validate(expected);
    bitmap.forAllInRange(0, 100000, consumer);
    assertEquals(100000, consumer.getNumberOfValuesConsumed());

    Value[] expectedSubRange = Arrays.copyOfRange(expected,2500, 6001);
    TestRangeConsumer consumer2 = TestRangeConsumer.validate(expectedSubRange);
    bitmap.forAllInRange(2500, 3500, consumer2);
    assertEquals(1000, consumer2.getNumberOfValuesConsumed());

    TestRangeConsumer consumer3 = TestRangeConsumer.ofSize(1000);
    bitmap.forAllInRange(2500, 1000, consumer3);
    consumer3.assertAllAbsentExcept(new int[] {3000 - 2500});
    assertEquals(1000, consumer3.getNumberOfValuesConsumed());
  }
}
