package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.roaringbitmap.ValidationRangeConsumer.Value.ABSENT;
import static org.roaringbitmap.ValidationRangeConsumer.Value.PRESENT;

import org.roaringbitmap.ValidationRangeConsumer.Value;

import com.google.common.primitives.UnsignedInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

public class TestForAllInRange {

  private int uAdd(int l, int r) {
    UnsignedInteger result = UnsignedInteger.fromIntBits(l).plus(UnsignedInteger.fromIntBits(r));
    return result.intValue();
  }

  private long uAddL(int l, int r) {
    UnsignedInteger result = UnsignedInteger.fromIntBits(l).plus(UnsignedInteger.fromIntBits(r));
    return result.longValue();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 65531, 65536, 2147483642, 2147483647, -2147483648, -2146958415})
  public void testContinuous(int offset) {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(uAddL(offset, 100), uAddL(offset, 10000));

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validateContinuous(9900, PRESENT);
    bitmap.forAllInRange(uAdd(offset, 100), 9900, consumer);
    assertEquals(9900, consumer.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validateContinuous(1000, ABSENT);
    bitmap.forAllInRange(uAdd(offset, 10001), 1000, consumer2);
    assertEquals(1000, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 =
        ValidationRangeConsumer.validate(new Value[] {ABSENT, ABSENT, PRESENT, PRESENT, PRESENT});
    bitmap.forAllInRange(uAdd(offset, 98), 5, consumer3);
    assertEquals(5, consumer3.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer4 =
        ValidationRangeConsumer.validate(new Value[] {PRESENT, PRESENT, ABSENT, ABSENT, ABSENT});
    bitmap.forAllInRange(uAdd(offset, 9998), 5, consumer4);
    assertEquals(5, consumer4.getNumberOfValuesConsumed());

    bitmap = new RoaringBitmap();
    bitmap.add(uAddL(offset, 0), uAddL(offset, 1000000));
    ValidationRangeConsumer consumer5 = ValidationRangeConsumer.ofSize(1000000);
    bitmap.forAllInRange(uAdd(offset, 0), 1000000, consumer5);
    consumer5.assertAllPresent();
    bitmap.runOptimize();
    ValidationRangeConsumer consumer6 = ValidationRangeConsumer.ofSize(1000000);
    bitmap.forAllInRange(uAdd(offset, 0), 1000000, consumer6);
    consumer6.assertAllPresent();

    bitmap = new RoaringBitmap();
    bitmap.add(uAddL(offset, 100), uAddL(offset, 10000));
    ValidationRangeConsumer consumer7 = ValidationRangeConsumer.ofSize(1000000);
    bitmap.forAllInRange(uAdd(offset, 10000), 1000000, consumer7);
    consumer7.assertAllAbsent();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 65531, 65536, 2147483642, 2147483647, -2147483648, -2146958415})
  public void testDense(int offset) {
    RoaringBitmap bitmap = new RoaringBitmap();
    Value[] expected = new Value[100000];
    Arrays.fill(expected, ABSENT);
    for (int k = 0; k < 100000; k += 3) {
      bitmap.add(uAdd(offset, k));
      expected[k] = PRESENT;
    }

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
    bitmap.forAllInRange(uAdd(offset, 0), 100000, consumer);
    assertEquals(100000, consumer.getNumberOfValuesConsumed());

    Value[] expectedSubRange = Arrays.copyOfRange(expected, 2500, 6000);
    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validate(expectedSubRange);
    bitmap.forAllInRange(uAdd(offset, 2500), 3500, consumer2);
    assertEquals(3500, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 =
        ValidationRangeConsumer.validate(
            new Value[] {
              expected[99997], expected[99998], expected[99999], ABSENT, ABSENT, ABSENT
            });
    bitmap.forAllInRange(uAdd(offset, 99997), 6, consumer3);
    assertEquals(6, consumer3.getNumberOfValuesConsumed());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 65531, 65536, 2147483642, 2147483647, -2147483648, -2146958415})
  public void testSparse(int offset) {
    RoaringBitmap bitmap = new RoaringBitmap();
    Value[] expected = new Value[100000];
    Arrays.fill(expected, ABSENT);
    for (int k = 0; k < 100000; k += 3000) {
      bitmap.add(uAdd(offset, k));
      expected[k] = PRESENT;
    }

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
    bitmap.forAllInRange(uAdd(offset, 0), 100000, consumer);
    assertEquals(100000, consumer.getNumberOfValuesConsumed());

    Value[] expectedSubRange = Arrays.copyOfRange(expected, 2500, 6001);
    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validate(expectedSubRange);
    bitmap.forAllInRange(uAdd(offset, 2500), 3500, consumer2);
    assertEquals(3500, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 = ValidationRangeConsumer.ofSize(1000);
    bitmap.forAllInRange(uAdd(offset, 2500), 1000, consumer3);
    consumer3.assertAllAbsentExcept(new int[] {3000 - 2500});
    assertEquals(1000, consumer3.getNumberOfValuesConsumed());
  }

  @Test
  public void readToEnd() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0xFFFFFFFE);
    bitmap.add(0xFFFFFFFF);

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validateContinuous(2, PRESENT);
    bitmap.forAllInRange(0xFFFFFFFE, 2, consumer);
    assertEquals(2, consumer.getNumberOfValuesConsumed());

    RoaringBitmap emptyBitmap = new RoaringBitmap();

    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validateContinuous(2, ABSENT);
    emptyBitmap.forAllInRange(0xFFFFFFFE, 2, consumer2);
    assertEquals(2, consumer2.getNumberOfValuesConsumed());

    RoaringBitmap emptyLastContainerBitmap = new RoaringBitmap();
    bitmap.add(0xFFFFFEFE);
    bitmap.add(0xFFFFFEFF);

    ValidationRangeConsumer consumer3 = ValidationRangeConsumer.validateContinuous(2, ABSENT);
    emptyLastContainerBitmap.forAllInRange(0xFFFFFFFE, 2, consumer3);
    assertEquals(2, consumer3.getNumberOfValuesConsumed());
  }

  @Test
  public void readPastEnd() {
    final RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0xFFFFFFFE);
    bitmap.add(0xFFFFFFFF);

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          bitmap.forAllInRange(0xFFFFFFFE, 3, ValidationRangeConsumer.ofSize(3));
        });
  }

  @Test
  public void readToJustBeforeEndEnd() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0xFFFFFFFD);
    bitmap.add(0xFFFFFFFE);
    bitmap.add(0xFFFFFFFF);

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validateContinuous(2, PRESENT);
    bitmap.forAllInRange(0xFFFFFFFD, 2, consumer);
    assertEquals(2, consumer.getNumberOfValuesConsumed());

    RoaringBitmap emptyBitmap = new RoaringBitmap();

    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validateContinuous(2, ABSENT);
    emptyBitmap.forAllInRange(0xFFFFFFFD, 2, consumer2);
    assertEquals(2, consumer2.getNumberOfValuesConsumed());

    RoaringBitmap emptyLastContainerBitmap = new RoaringBitmap();
    bitmap.add(0xFFFFFEFD);
    bitmap.add(0xFFFFFEFE);
    bitmap.add(0xFFFFFEFF);

    ValidationRangeConsumer consumer3 = ValidationRangeConsumer.validateContinuous(2, ABSENT);
    emptyLastContainerBitmap.forAllInRange(0xFFFFFFFD, 2, consumer3);
    assertEquals(2, consumer3.getNumberOfValuesConsumed());
  }
}
