package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.roaringbitmap.TestAdversarialInputs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

public class TestBufferAdversarialInputs {

  public static Stream<Arguments> badFiles() {
    return TestAdversarialInputs.badFiles();
  }

  // copy to a temporary file
  protected static File copy(String resourceName) throws IOException {
    File tmpFile = File.createTempFile(TestBufferAdversarialInputs.class.getName(), "bin");
    tmpFile.deleteOnExit();

    try (InputStream input = TestAdversarialInputs.openInputstream(resourceName)) {
      Files.copy(input, tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    return tmpFile;
  }

  public ByteBuffer memoryMap(String resourceName) throws IOException {
    File tmpfile = copy(resourceName);
    long totalcount = tmpfile.length();
    RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
    ByteBuffer bb =
        memoryMappedFile
            .getChannel()
            .map(
                FileChannel.MapMode.READ_ONLY,
                0,
                totalcount); // even though we have two bitmaps, we have one map, maps are
    // expensive!!!
    memoryMappedFile.close(); // we can safely close
    bb.position(0);
    return bb;
  }

  @Test
  public void testInputGoodFile1() throws IOException {
    File file = copy("/testdata/bitmapwithruns.bin");
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    // should not throw an exception
    rb.deserialize(new DataInputStream(new FileInputStream(file)));
    assertTrue(rb.validate());
    assertEquals(rb.getCardinality(), 200100);
    file.delete();
  }

  @Test
  public void testInputGoodFile1Mapped() throws IOException {
    ByteBuffer bb = memoryMap("/testdata/bitmapwithruns.bin");
    ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
    assertEquals(rb.getCardinality(), 200100);
  }

  @Test
  public void testInputGoodFile2() throws IOException {
    File file = copy("/testdata/bitmapwithoutruns.bin");
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    // should not throw an exception
    rb.deserialize(new DataInputStream(new FileInputStream(file)));
    assertEquals(rb.getCardinality(), 200100);
    file.delete();
  }

  @Test
  public void testInputGoodFile2Mapped() throws IOException {
    ByteBuffer bb = memoryMap("/testdata/bitmapwithoutruns.bin");
    ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
    assertEquals(rb.getCardinality(), 200100);
  }

  @ParameterizedTest
  @MethodSource("badFiles")
  public void testInputBadFileDeserialize(String file) {
    assertThrows(IOException.class, () -> deserialize(file));
  }

  @ParameterizedTest
  @MethodSource("badFiles")
  public void testInputBadFileMap(String file) {
    if (file.endsWith("7.bin")) {
      assertThrows(IllegalArgumentException.class, () -> map(file));
    } else {
      assertThrows(IndexOutOfBoundsException.class, () -> map(file));
    }
  }

  private void deserialize(String fileName) throws IOException {
    File file = copy(fileName);
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    // should not work
    rb.deserialize(new DataInputStream(new FileInputStream(file)));
    file.delete();
  }

  private void map(String fileName) throws IOException {
    ByteBuffer bb = memoryMap(fileName);
    ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
    System.out.println(rb.getCardinality()); // won't get here
  }
}
