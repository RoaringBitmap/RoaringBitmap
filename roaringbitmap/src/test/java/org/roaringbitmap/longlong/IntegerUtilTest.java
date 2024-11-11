package org.roaringbitmap.longlong;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class IntegerUtilTest {

  @Test
  public void testConvertIntToBytes() {
    int v = 1;
    byte[] bytes = IntegerUtil.toBDBytes(v);
    int revertVal = IntegerUtil.fromBDBytes(bytes);
    assertEquals(revertVal, v);
    v = -1;
    bytes = IntegerUtil.toBDBytes(v);
    revertVal = IntegerUtil.fromBDBytes(bytes);
    assertEquals(revertVal, v);
    v = -125;
    bytes = IntegerUtil.toBDBytes(v);
    revertVal = IntegerUtil.fromBDBytes(bytes);
    assertEquals(revertVal, v);
  }

  @Test
  public void testSetByte() {
    for (int i = 0; i < 4; ++i) {
      int value = IntegerUtil.setByte(0x55555555, (byte) 0xAA, i);
      byte[] bytes = IntegerUtil.toBDBytes(value);
      for (int j = 0; j < 4; ++j) {
        byte expected = i == j ? (byte) 0xAA : (byte) 0x55;
        assertEquals(expected, bytes[j]);
      }
    }
  }

  @Test
  public void testShiftLeftFromSpecifiedPosition() {
    assertEquals(0xBBCCDDDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 0, 3));
    assertEquals(0xBBCCCCDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 0, 2));
    assertEquals(0xBBBBCCDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 0, 1));
    assertEquals(0xAABBCCDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 0, 0));
    assertEquals(0xAACCDDDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 1, 2));
    assertEquals(0xAACCCCDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 1, 1));
    assertEquals(0xAABBCCDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 1, 0));
    assertEquals(0xAABBDDDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 2, 1));
    assertEquals(0xAABBCCDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 2, 0));
    assertEquals(0xAABBCCDD, IntegerUtil.shiftLeftFromSpecifiedPosition(0xAABBCCDD, 3, 0));
  }
}
