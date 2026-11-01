package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.insights.BitmapAnalyser;
import org.roaringbitmap.insights.BitmapStatistics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestAdversarialInputs {

  public static Stream<Arguments> badFiles() {
    return IntStream.rangeClosed(1, 8)
        .mapToObj(i -> Arguments.of("/testdata/crashproneinput" + i + ".bin"));
  }

  // files shared with the Go library: an ASCII "0000" cookie and an empty file
  public static Stream<Arguments> badCookieFiles() {
    return Stream.of(
        Arguments.of("/testdata/crashwithinvalidcookie.bin"),
        Arguments.of("/testdata/crashwithoutcookie.bin"));
  }

  // open a stream without copying files
  public static InputStream openInputstream(String resourceName) throws IOException {
    InputStream resourceAsStream = TestAdversarialInputs.class.getResourceAsStream(resourceName);
    if (resourceAsStream == null) {
      throw new IOException("Cannot get resource \"" + resourceName + "\".");
    }
    return resourceAsStream;
  }

  public static byte[] readAllBytes(String resourceName) throws IOException {
    try (InputStream inputStream = openInputstream(resourceName)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] chunk = new byte[4096];
      for (int n = inputStream.read(chunk); n >= 0; n = inputStream.read(chunk)) {
        out.write(chunk, 0, n);
      }
      return out.toByteArray();
    }
  }

  public static byte[] hex(String hex) {
    String digits = hex.replace(" ", "");
    byte[] bytes = new byte[digits.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) Integer.parseInt(digits.substring(2 * i, 2 * i + 2), 16);
    }
    return bytes;
  }

  /**
   * The content of bitmapwithruns.bin and bitmapwithoutruns.bin, shared with CRoaring and Go.
   */
  public static RoaringBitmap expectedContentOfGoodFiles() {
    RoaringBitmap expected = new RoaringBitmap();
    for (int k = 0; k < 100000; k += 1000) {
      expected.add(k);
    }
    for (int k = 100000; k < 200000; ++k) {
      expected.add(3 * k);
    }
    expected.add(700000L, 800000L);
    return expected;
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
  @MethodSource("goodFiles")
  public void testGoodFilesHaveTheExpectedContent(String fileName, int runContainers)
      throws IOException {
    byte[] bytes = readAllBytes(fileName);
    RoaringBitmap rb = new RoaringBitmap();
    rb.deserialize(ByteBuffer.wrap(bytes));
    assertTrue(rb.validate());
    assertEquals(expectedContentOfGoodFiles(), rb);
    BitmapStatistics stats = BitmapAnalyser.analyse(rb);
    assertEquals(11, stats.containerCount());
    assertEquals(runContainers, stats.getRunContainerCount());
    assertEquals(bytes.length, rb.serializedSizeInBytes());
    ByteBuffer out = ByteBuffer.allocate(rb.serializedSizeInBytes());
    rb.serialize(out);
    assertArrayEquals(bytes, out.array());
    RoaringBitmap fromStream = new RoaringBitmap();
    fromStream.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    assertEquals(rb, fromStream);
  }

  public static Stream<Arguments> goodFiles() {
    return Stream.of(
        Arguments.of("/testdata/bitmapwithruns.bin", 3),
        Arguments.of("/testdata/bitmapwithoutruns.bin", 0));
  }

  @ParameterizedTest
  @MethodSource("frozenDataPortableFiles")
  public void testPortableFilesSharedWithGo(
      String name, int cardinality, int bitmaps, int arrays, int runs) throws IOException {
    // portable twins of the frozen files in Go's testfrozendata directory
    byte[] bytes = readAllBytes("/testdata/testfrozendata/" + name + ".portable");
    RoaringBitmap rb = new RoaringBitmap();
    rb.deserialize(ByteBuffer.wrap(bytes));
    assertTrue(rb.validate());
    assertEquals(cardinality, rb.getLongCardinality());
    BitmapStatistics stats = BitmapAnalyser.analyse(rb);
    assertEquals(bitmaps, stats.getBitmapContainerCount());
    assertEquals(arrays, stats.getArrayContainersStats().getContainersCount());
    assertEquals(runs, stats.getRunContainerCount());
    RoaringBitmap fromStream = new RoaringBitmap();
    fromStream.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    assertEquals(rb, fromStream);
    MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    mutable.deserialize(ByteBuffer.wrap(bytes));
    assertEquals(rb, mutable.toRoaringBitmap());
    ByteBuffer out = ByteBuffer.allocate(rb.serializedSizeInBytes());
    rb.serialize(out);
    assertArrayEquals(bytes, out.array());
  }

  public static Stream<Arguments> frozenDataPortableFiles() {
    return Stream.of(
        Arguments.of("bitmaps_only", 65536, 2, 0, 0),
        Arguments.of("arrays_only", 132, 0, 2, 0),
        Arguments.of("runs_only", 122878, 0, 0, 2),
        Arguments.of("mixed", 188546, 2, 2, 2));
  }

  @ParameterizedTest
  @MethodSource("badFiles")
  public void testInputBadFile8(String fileName) {
    assertThrows(IOException.class, () -> deserialize(fileName));
  }

  @ParameterizedTest
  @MethodSource("badCookieFiles")
  public void testInputBadCookieFile(String fileName) throws IOException {
    assertThrows(IOException.class, () -> deserialize(fileName));
    byte[] bytes = readAllBytes(fileName);
    RoaringBitmap rb = new RoaringBitmap();
    assertThrows(Exception.class, () -> rb.deserialize(ByteBuffer.wrap(bytes)));
    assertTrue(rb.isEmpty());
  }

  private void deserialize(String fileName) throws IOException {
    InputStream inputStream = openInputstream(fileName);
    RoaringBitmap rb = new RoaringBitmap();
    // should not work
    rb.deserialize(new DataInputStream(inputStream));
  }

  private interface Deserializer {
    void deserialize(RoaringBitmap target, byte[] bytes) throws IOException;
  }

  private static final Deserializer[] DESERIALIZERS = {
    (target, bytes) -> target.deserialize(new DataInputStream(new ByteArrayInputStream(bytes))),
    (target, bytes) ->
        target.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)), new byte[16]),
    (target, bytes) ->
        target.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)), new byte[8192]),
    (target, bytes) -> target.deserialize(ByteBuffer.wrap(bytes)),
  };

  private static byte[] serialize(RoaringBitmap rb) {
    ByteBuffer buffer = ByteBuffer.allocate(rb.serializedSizeInBytes());
    rb.serialize(buffer);
    return buffer.array();
  }

  private static void assertUsableAndEmpty(RoaringBitmap rb) {
    assertEquals(0, rb.getLongCardinality());
    assertTrue(rb.validate());
    assertFalse(rb.contains(7));
    rb.add(5);
    assertTrue(rb.contains(5));
    assertEquals(1, rb.getLongCardinality());
  }

  @Test
  public void failedDeserializationLeavesAnEmptyValidBitmap() {
    byte[] bytes = serialize(RoaringBitmap.bitmapOf(7, 70000, 140000));
    assertEquals(38, bytes.length);
    byte[] truncated = Arrays.copyOf(bytes, bytes.length - 1);
    for (Deserializer deserializer : DESERIALIZERS) {
      RoaringBitmap rb = RoaringBitmap.bitmapOf(1, 2, 3);
      assertThrows(Exception.class, () -> deserializer.deserialize(rb, truncated));
      assertUsableAndEmpty(rb);
    }
    MutableRoaringBitmap mutable = MutableRoaringBitmap.bitmapOf(1, 2, 3);
    assertThrows(
        IOException.class,
        () -> mutable.deserialize(new DataInputStream(new ByteArrayInputStream(truncated))));
    assertEquals(0, mutable.getLongCardinality());
    assertTrue(mutable.validate());
    MutableRoaringBitmap mutable2 = MutableRoaringBitmap.bitmapOf(1, 2, 3);
    assertThrows(Exception.class, () -> mutable2.deserialize(ByteBuffer.wrap(truncated)));
    assertEquals(0, mutable2.getLongCardinality());
    assertTrue(mutable2.validate());
  }

  @Test
  public void negativeContainerCountIsRejected() {
    byte[] bytes = hex("3A300000 00000080");
    for (Deserializer deserializer : DESERIALIZERS) {
      RoaringBitmap rb = RoaringBitmap.bitmapOf(1, 2, 3);
      assertThrows(IOException.class, () -> deserializer.deserialize(rb, bytes));
      assertUsableAndEmpty(rb);
    }
    MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    assertThrows(
        IOException.class,
        () -> mutable.deserialize(new DataInputStream(new ByteArrayInputStream(bytes))));
    assertThrows(IOException.class, () -> mutable.deserialize(ByteBuffer.wrap(bytes)));
  }

  @Test
  public void containerCountAbove65536IsRejected() {
    byte[] header = hex("3A300000 01000100"); // 65537 containers
    byte[] bytes = Arrays.copyOf(header, header.length + 65537 * 12);
    for (Deserializer deserializer : DESERIALIZERS) {
      RoaringBitmap rb = new RoaringBitmap();
      assertThrows(IOException.class, () -> deserializer.deserialize(rb, bytes));
      assertUsableAndEmpty(rb);
    }
    MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    assertThrows(IOException.class, () -> mutable.deserialize(ByteBuffer.wrap(bytes)));
  }

  @Test
  public void containerCountOf65536IsAccepted() throws IOException {
    // 65536 containers with key 0 holding the value 0: accepted, but the keys are not increasing
    byte[] header = hex("3A300000 00000100");
    byte[] bytes = Arrays.copyOf(header, header.length + 65536 * 10);
    for (Deserializer deserializer : DESERIALIZERS) {
      RoaringBitmap rb = new RoaringBitmap();
      deserializer.deserialize(rb, bytes);
      assertEquals(65536, rb.getContainerCount());
      assertFalse(rb.validate());
    }
  }

  @ParameterizedTest
  @MethodSource("truncationSources")
  public void everyStrictPrefixIsRejected(String fileName) throws IOException {
    byte[] bytes = readAllBytes(fileName);
    RoaringBitmap complete = new RoaringBitmap();
    complete.deserialize(ByteBuffer.wrap(bytes));
    assertTrue(complete.validate());
    for (int length = 0; length < bytes.length; length++) {
      byte[] prefix = Arrays.copyOf(bytes, length);
      for (Deserializer deserializer : DESERIALIZERS) {
        RoaringBitmap rb = RoaringBitmap.bitmapOf(42);
        assertThrows(Exception.class, () -> deserializer.deserialize(rb, prefix));
        assertEquals(0, rb.getLongCardinality());
        assertTrue(rb.validate());
      }
      MutableRoaringBitmap mutable = MutableRoaringBitmap.bitmapOf(42);
      assertThrows(
          IOException.class,
          () -> mutable.deserialize(new DataInputStream(new ByteArrayInputStream(prefix))));
      assertEquals(0, mutable.getLongCardinality());
    }
  }

  public static Stream<Arguments> truncationSources() {
    return Stream.of(
        Arguments.of("/testdata/testfrozendata/runs_only.portable"),
        Arguments.of("/testdata/testfrozendata/arrays_only.portable"),
        Arguments.of("/testdata/testfrozendata/mixed.portable"));
  }

  @ParameterizedTest
  @MethodSource("malformedVectors")
  public void malformedInputIsAcceptedButInvalid(String name, String hex, int cardinality)
      throws IOException {
    byte[] bytes = hex(hex);
    for (Deserializer deserializer : DESERIALIZERS) {
      RoaringBitmap rb = new RoaringBitmap();
      deserializer.deserialize(rb, bytes);
      assertEquals(cardinality, rb.getLongCardinality(), name);
      assertFalse(rb.validate(), name);
    }
    MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    mutable.deserialize(ByteBuffer.wrap(bytes));
    assertFalse(mutable.validate(), name);
    // dropping the last byte makes the input too short
    byte[] truncated = Arrays.copyOf(bytes, bytes.length - 1);
    for (Deserializer deserializer : DESERIALIZERS) {
      assertThrows(
          Exception.class, () -> deserializer.deserialize(new RoaringBitmap(), truncated), name);
    }
  }

  // byte vectors from CRoaring's robust_deserialization_unit.c
  public static Stream<Arguments> malformedVectors() {
    return Stream.of(
        Arguments.of("duplicate keys", "3B30 0100 00 0000 0000 0000 0000 0000 0000", 2),
        Arguments.of("unsorted keys", "3B30 0100 00 0100 0000 0000 0000 0000 0000", 2),
        Arguments.of("descending array", "3B30 0000 00 0000 0100 0100 0000", 2),
        Arguments.of("array duplicates", "3B30 0000 00 0000 0100 0000 0000", 2),
        Arguments.of("zero runs", "3B30 0000 01 0000 0000 0000", 0),
        Arguments.of("contiguous runs", "3B30 0000 01 0000 0100 0200 0000 0000 0100 0000", 2),
        Arguments.of("overlapping runs", "3B30 0000 01 0000 0400 0200 0000 0400 0100 0000", 6),
        Arguments.of("run overflow", "3B30 0000 01 0000 0400 0100 FEFF 0400", 5),
        Arguments.of("run crossing the end", "3b300000 01 0000 fa2e 0100 0002 ffff", 65536));
  }

  @Test
  public void bitmapContainerWithWrongHeaderCardinalityIsInvalid() throws IOException {
    // one full bitmap container (65536 values), from robust_deserialization_unit.c
    byte[] bytes = new byte[9 + 8192];
    System.arraycopy(hex("3B30 0000 00 0000 FFFF"), 0, bytes, 0, 9);
    Arrays.fill(bytes, 9, bytes.length, (byte) 0xFF);
    for (Deserializer deserializer : DESERIALIZERS) {
      RoaringBitmap rb = new RoaringBitmap();
      deserializer.deserialize(rb, bytes);
      assertTrue(rb.validate());
      assertEquals(65536, rb.getLongCardinality());
    }
    bytes[bytes.length - 1] = (byte) 0xFE;
    for (Deserializer deserializer : DESERIALIZERS) {
      RoaringBitmap rb = new RoaringBitmap();
      deserializer.deserialize(rb, bytes);
      assertFalse(rb.validate());
    }
  }

  @Test
  public void runHeaderCardinalityIsIgnoredOnTheHeap() throws IOException {
    // the header says 1 value, the run holds 9: the heap readers ignore the header
    // (as CRoaring and Go do), while ImmutableRoaringBitmap trusts it and fails validation
    byte[] bytes = hex("3B30 0000 01 0000 0000 0100 0000 0800");
    for (Deserializer deserializer : DESERIALIZERS) {
      RoaringBitmap rb = new RoaringBitmap();
      deserializer.deserialize(rb, bytes);
      assertEquals(9, rb.getLongCardinality());
      assertTrue(rb.validate());
    }
  }

  @Test
  public void randomInputOnlyThrowsIOExceptions() throws IOException {
    // port of CRoaring's fuzz_deserializer
    Random random = new Random(1234);
    for (int i = 0; i < 10000; i++) {
      byte[] bytes = new byte[random.nextInt(10000)];
      random.nextBytes(bytes);
      if (bytes.length >= 8) {
        // most random cookies are rejected immediately, so we mostly use valid cookies
        switch (i % 3) {
          case 0:
            bytes[0] = 0x3B;
            bytes[1] = 0x30;
            bytes[2] = (byte) random.nextInt(16);
            bytes[3] = 0;
            break;
          case 1:
            System.arraycopy(hex("3A300000"), 0, bytes, 0, 4);
            bytes[4] = (byte) random.nextInt(16);
            bytes[5] = 0;
            bytes[6] = 0;
            bytes[7] = 0;
            break;
          default:
            break;
        }
      }
      RoaringBitmap rb = new RoaringBitmap();
      try {
        rb.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
      } catch (IOException expected) {
        assertTrue(rb.isEmpty());
        continue;
      } catch (RuntimeException e) {
        fail("unexpected exception for input " + i, e);
      }
      if (rb.validate()) {
        long cardinality = rb.getLongCardinality();
        long absent = IntStream.range(100, 1000).filter(v -> !rb.contains(v)).count();
        rb.add(100L, 1000L);
        assertEquals(cardinality + absent, rb.getLongCardinality());
        assertTrue(rb.validate());
      }
    }
  }
}
