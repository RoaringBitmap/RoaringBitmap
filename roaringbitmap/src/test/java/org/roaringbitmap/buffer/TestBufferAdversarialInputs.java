package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.roaringbitmap.TestAdversarialInputs.hex;
import static org.roaringbitmap.TestAdversarialInputs.readAllBytes;

import org.roaringbitmap.InvalidRoaringFormat;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.TestAdversarialInputs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Random;
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

  @ParameterizedTest
  @MethodSource("org.roaringbitmap.TestAdversarialInputs#badCookieFiles")
  public void testInputBadCookieFile(String file) throws IOException {
    assertThrows(IOException.class, () -> deserialize(file));
    ByteBuffer bytes = ByteBuffer.wrap(readAllBytes(file));
    if (bytes.capacity() == 0) {
      assertThrows(IndexOutOfBoundsException.class, () -> new ImmutableRoaringBitmap(bytes));
    } else {
      assertThrows(InvalidRoaringFormat.class, () -> new ImmutableRoaringBitmap(bytes));
    }
  }

  @ParameterizedTest
  @MethodSource("org.roaringbitmap.TestAdversarialInputs#goodFiles")
  public void testGoodFilesMappedHaveTheExpectedContent(String file, int runContainers)
      throws IOException {
    ByteBuffer bb = memoryMap(file);
    ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
    assertTrue(rb.validate());
    assertEquals(bb.capacity(), rb.serializedSizeInBytes());
    RoaringBitmap expected = TestAdversarialInputs.expectedContentOfGoodFiles();
    assertEquals(expected, rb.toRoaringBitmap());
    int containers = 0;
    int runs = 0;
    for (MappeableContainerPointer cp = rb.getContainerPointer(); cp.hasContainer(); cp.advance()) {
      containers++;
      runs += cp.isRunContainer() ? 1 : 0;
    }
    assertEquals(11, containers);
    assertEquals(runContainers, runs);
  }

  @ParameterizedTest
  @MethodSource("org.roaringbitmap.TestAdversarialInputs#frozenDataPortableFiles")
  public void testPortableFilesSharedWithGoMapped(
      String name, int cardinality, int bitmaps, int arrays, int runs) throws IOException {
    byte[] bytes = readAllBytes("/testdata/testfrozendata/" + name + ".portable");
    ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes));
    assertTrue(rb.validate());
    assertEquals(cardinality, rb.getLongCardinality());
    assertEquals(bytes.length, rb.serializedSizeInBytes());
    RoaringBitmap heap = new RoaringBitmap();
    heap.deserialize(ByteBuffer.wrap(bytes));
    assertEquals(heap, rb.toRoaringBitmap());
  }

  @ParameterizedTest
  @MethodSource("org.roaringbitmap.TestAdversarialInputs#truncationSources")
  public void everyStrictPrefixIsRejectedMapped(String file) throws IOException {
    byte[] bytes = readAllBytes(file);
    for (int length = 0; length < bytes.length; length++) {
      ByteBuffer prefix = ByteBuffer.wrap(Arrays.copyOf(bytes, length));
      assertThrows(RuntimeException.class, () -> new ImmutableRoaringBitmap(prefix));
    }
  }

  @ParameterizedTest
  @MethodSource("org.roaringbitmap.TestAdversarialInputs#malformedVectors")
  public void malformedInputIsAcceptedButInvalidMapped(String name, String hex, int cardinality) {
    ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(ByteBuffer.wrap(hex(hex)));
    assertFalse(rb.validate(), name);
  }

  @Test
  public void bitmapContainerWithWrongHeaderCardinalityIsInvalidMapped() {
    byte[] bytes = new byte[9 + 8192];
    System.arraycopy(hex("3B30 0000 00 0000 FFFF"), 0, bytes, 0, 9);
    Arrays.fill(bytes, 9, bytes.length, (byte) 0xFF);
    assertTrue(new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes)).validate());
    bytes[bytes.length - 1] = (byte) 0xFE;
    assertFalse(new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes)).validate());
  }

  @Test
  public void negativeAndHugeContainerCountsAreRejectedMapped() {
    // the mapped reader rejects these with unchecked exceptions, as for other malformed input
    assertThrows(
        IllegalArgumentException.class,
        () -> new ImmutableRoaringBitmap(ByteBuffer.wrap(hex("3A300000 00000080"))));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> new ImmutableRoaringBitmap(ByteBuffer.wrap(hex("3A300000 01000100"))));
  }

  @Test
  public void validateRejectsAnOffsetTableThatDisagreesWithTheLayout() throws IOException {
    // two containers {7} and {65536 + 9}: the second offset should be 26 (0x1A), not 24
    byte[] good = hex("3A300000 02000000 0000 0000 0100 0000 18000000 1A000000 0700 0900");
    byte[] bad = hex("3A300000 02000000 0000 0000 0100 0000 18000000 18000000 0700 0900");
    RoaringBitmap expected = RoaringBitmap.bitmapOf(7, 65536 + 9);
    ImmutableRoaringBitmap goodMapped = new ImmutableRoaringBitmap(ByteBuffer.wrap(good));
    assertTrue(goodMapped.validate());
    assertEquals(expected, goodMapped.toRoaringBitmap());
    // the heap reader ignores the offsets
    RoaringBitmap heap = new RoaringBitmap();
    heap.deserialize(ByteBuffer.wrap(bad));
    assertEquals(expected, heap);
    assertTrue(heap.validate());
    // the mapped reader follows them, so the same bytes would mean another set
    ImmutableRoaringBitmap badMapped = new ImmutableRoaringBitmap(ByteBuffer.wrap(bad));
    assertFalse(badMapped.validate());
    // with run containers and at least NO_OFFSET_THRESHOLD containers, offsets are also stored
    MutableRoaringBitmap runs = new MutableRoaringBitmap();
    for (int k = 0; k < 5; k++) {
      runs.add((k << 16) + 0L, (k << 16) + 10L);
    }
    runs.runOptimize();
    ByteBuffer buffer = ByteBuffer.allocate(runs.serializedSizeInBytes());
    runs.serialize(buffer);
    byte[] runBytes = buffer.array();
    assertTrue(new ImmutableRoaringBitmap(ByteBuffer.wrap(runBytes)).validate());
    // the offset table starts after the cookie, the run bitmap and the keys/cardinalities
    int offsetOfSecondOffset = 4 + 1 + 4 * 5 + 4;
    ByteBuffer.wrap(runBytes)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(offsetOfSecondOffset, 45); // instead of 51
    assertFalse(new ImmutableRoaringBitmap(ByteBuffer.wrap(runBytes)).validate());
  }

  @Test
  public void validateOfRandomMappedInputDoesNotThrow() {
    Random random = new Random(1234);
    for (int i = 0; i < 10000; i++) {
      byte[] bytes = new byte[random.nextInt(10000)];
      random.nextBytes(bytes);
      if (bytes.length >= 8) {
        if (i % 2 == 0) {
          bytes[0] = 0x3B;
          bytes[1] = 0x30;
          bytes[2] = (byte) random.nextInt(16);
          bytes[3] = 0;
        } else {
          System.arraycopy(hex("3A300000"), 0, bytes, 0, 4);
          bytes[4] = (byte) random.nextInt(16);
          bytes[5] = 0;
          bytes[6] = 0;
          bytes[7] = 0;
        }
      }
      ImmutableRoaringBitmap rb;
      try {
        rb = new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes));
      } catch (RuntimeException rejected) {
        continue;
      }
      boolean valid;
      try {
        valid = rb.validate();
      } catch (RuntimeException e) {
        fail("validate() threw for input " + i, e);
        return;
      }
      if (valid) {
        RoaringBitmap heap = new RoaringBitmap();
        try {
          heap.deserialize(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
          fail(e);
        }
        assertEquals(heap, rb.toRoaringBitmap());
      }
    }
  }

  @Test
  public void failedDeserializationLeavesAnEmptyValidMutableBitmap() {
    byte[] bytes = new byte[0];
    MutableRoaringBitmap rb = MutableRoaringBitmap.bitmapOf(1, 2, 3);
    assertThrows(
        IOException.class,
        () -> rb.deserialize(new DataInputStream(new ByteArrayInputStream(bytes))));
    assertTrue(rb.isEmpty());
    rb.add(5);
    assertEquals(1, rb.getLongCardinality());
    assertTrue(rb.validate());
  }
}
