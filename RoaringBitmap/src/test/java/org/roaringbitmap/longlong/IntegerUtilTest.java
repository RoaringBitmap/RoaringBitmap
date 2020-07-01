package org.roaringbitmap.longlong;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IntegerUtilTest {

  @Test
  public void test() {
    int v = 1;
    byte[] bytes = IntegerUtil.toBDBytes(v);
    int revertVal = IntegerUtil.fromBDBytes(bytes);
    Assertions.assertTrue(revertVal == v);
    v = -1;
    bytes = IntegerUtil.toBDBytes(v);
    revertVal = IntegerUtil.fromBDBytes(bytes);
    Assertions.assertTrue(revertVal == v);
    v = -125;
    bytes = IntegerUtil.toBDBytes(v);
    revertVal = IntegerUtil.fromBDBytes(bytes);
    Assertions.assertTrue(revertVal == v);
  }
}
