package org.roaringbitmap;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.ValidationRangeConsumer.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.roaringbitmap.ValidationRangeConsumer.Value.ABSENT;
import static org.roaringbitmap.ValidationRangeConsumer.Value.PRESENT;


public class TestForAllInRange {

  @Test
  public void testContinuous() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(100L, 10000L);

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validateContinuous(9900, PRESENT);
    bitmap.forAllInRange(100, 9900, consumer);
    assertEquals(9900, consumer.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validateContinuous(1000, ABSENT);
    bitmap.forAllInRange(10001, 1000, consumer2);
    assertEquals(1000, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 = ValidationRangeConsumer.validate(new Value[]{
        ABSENT, ABSENT, PRESENT, PRESENT, PRESENT
    });
    bitmap.forAllInRange(98, 5, consumer3);
    assertEquals(5, consumer3.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer4 = ValidationRangeConsumer.validate(new Value[]{
        PRESENT, PRESENT, ABSENT, ABSENT, ABSENT
    });
    bitmap.forAllInRange(9998, 5, consumer4);
    assertEquals(5, consumer4.getNumberOfValuesConsumed());

    bitmap = new RoaringBitmap();
    bitmap.add(0L, 1000000L);
    ValidationRangeConsumer consumer5 = ValidationRangeConsumer.ofSize(1000000);
    bitmap.forAllInRange(0, 1000000, consumer5);
    consumer5.assertAllPresent();
    bitmap.runOptimize();
    ValidationRangeConsumer consumer6 = ValidationRangeConsumer.ofSize(1000000);
    bitmap.forAllInRange(0, 1000000, consumer6);
    consumer6.assertAllPresent();

    bitmap = new RoaringBitmap();
    bitmap.add(100L, 10000L);
    ValidationRangeConsumer consumer7 = ValidationRangeConsumer.ofSize(1000000);
    bitmap.forAllInRange(10000, 1000000, consumer7);
    consumer7.assertAllAbsent();
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

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
    bitmap.forAllInRange(0, 100000, consumer);
    assertEquals(100000, consumer.getNumberOfValuesConsumed());

    Value[] expectedSubRange = Arrays.copyOfRange(expected,2500, 6000);
    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validate(expectedSubRange);
    bitmap.forAllInRange(2500, 3500, consumer2);
    assertEquals(3500, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 = ValidationRangeConsumer.validate(new Value[]{
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

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
    bitmap.forAllInRange(0, 100000, consumer);
    assertEquals(100000, consumer.getNumberOfValuesConsumed());

    Value[] expectedSubRange = Arrays.copyOfRange(expected,2500, 6001);
    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validate(expectedSubRange);
    bitmap.forAllInRange(2500, 3500, consumer2);
    assertEquals(3500, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 = ValidationRangeConsumer.ofSize(1000);
    bitmap.forAllInRange(2500, 1000, consumer3);
    consumer3.assertAllAbsentExcept(new int[] {3000 - 2500});
    assertEquals(1000, consumer3.getNumberOfValuesConsumed());
  }
}
