package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class PreviousValueTest {
  @Test
  public void regressionTest() throws IOException {
    int testValue = 1828834057;
    String[] data =
        new String(
                Files.readAllBytes(
                    Paths.get("src/test/resources/testdata/prevvalue-regression.txt")))
            .split(",");
    RoaringBitmap bitmap = new RoaringBitmap();
    Arrays.stream(data).map(Integer::parseInt).forEach(bitmap::add);
    assertEquals(bitmap.last(), bitmap.previousValue(testValue));
    MutableRoaringBitmap mutable = bitmap.toMutableRoaringBitmap();
    assertEquals(mutable.last(), bitmap.previousValue(testValue));
  }
}
