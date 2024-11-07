package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestAdversarialInputs {

  public static Stream<Arguments> badFiles() {
    return IntStream.rangeClosed(1, 7)
        .mapToObj(i -> Arguments.of("/testdata/crashproneinput" + i + ".bin"));
  }

  // open a stream without copying files
  public static InputStream openInputstream(String resourceName) throws IOException {
    InputStream resourceAsStream = TestAdversarialInputs.class.getResourceAsStream(resourceName);
    if (resourceAsStream == null) {
      throw new IOException("Cannot get resource \"" + resourceName + "\".");
    }
    return resourceAsStream;
  }

  @Test
  public void testInputGoodFile1() throws IOException {
    InputStream inputStream = openInputstream("/testdata/bitmapwithruns.bin");
    RoaringBitmap rb = new RoaringBitmap();
    // should not throw an exception
    rb.deserialize(new DataInputStream(inputStream));
    assertEquals(rb.getCardinality(), 200100);
  }

  @Test
  public void testInputGoodFile2() throws IOException {
    InputStream inputStream = openInputstream("/testdata/bitmapwithoutruns.bin");
    RoaringBitmap rb = new RoaringBitmap();
    // should not throw an exception
    rb.deserialize(new DataInputStream(inputStream));
    assertEquals(rb.getCardinality(), 200100);
  }

  @ParameterizedTest
  @MethodSource("badFiles")
  public void testInputBadFile8(String fileName) {
    assertThrows(IOException.class, () -> deserialize(fileName));
  }

  private void deserialize(String fileName) throws IOException {
    InputStream inputStream = openInputstream(fileName);
    RoaringBitmap rb = new RoaringBitmap();
    // should not work
    rb.deserialize(new DataInputStream(inputStream));
  }
}
