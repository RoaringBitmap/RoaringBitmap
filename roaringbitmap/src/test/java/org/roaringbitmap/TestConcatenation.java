package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class TestConcatenation {

  private static Arguments[] DATA;

  @BeforeAll
  public static void setup() {
    DATA =
        new Arguments[] {
          // the data set reported in issue #260
          Arguments.of(read("src/test/resources/testdata/testIssue260.txt"), 5950),
          Arguments.of(read("src/test/resources/testdata/offset_failure_case_1.txt"), 20),
          Arguments.of(read("src/test/resources/testdata/offset_failure_case_2.txt"), 20),
          Arguments.of(read("src/test/resources/testdata/offset_failure_case_3.txt"), 20),
          // a range of test cases with offsets being divisors of 65536
          Arguments.of(testCase().withBitmapAt(0).withRunAt(1).withArrayAt(2).build(), 1 << 16),
          Arguments.of(testCase().withRunAt(0).withBitmapAt(1).withBitmapAt(2).build(), 1 << 16),
          Arguments.of(testCase().withBitmapAt(0).withBitmapAt(1).withRunAt(2).build(), 1 << 16),
          Arguments.of(testCase().withBitmapAt(0).withRunAt(2).withArrayAt(4).build(), 1 << 16),
          Arguments.of(testCase().withRunAt(0).withBitmapAt(2).withBitmapAt(4).build(), 1 << 16),
          Arguments.of(testCase().withArrayAt(0).withBitmapAt(2).withRunAt(4).build(), 1 << 16),
          // awkward offsets
          Arguments.of(testCase().withBitmapAt(0).build(), 20),
          Arguments.of(testCase().withRunAt(0).build(), 20),
          Arguments.of(testCase().withArrayAt(0).build(), 20),
          Arguments.of(testCase().withBitmapAt(0).withRunAt(1).build(), 20),
          Arguments.of(testCase().withRunAt(0).withBitmapAt(1).build(), 20),
          Arguments.of(testCase().withArrayAt(0).withBitmapAt(1).build(), 20),
          Arguments.of(testCase().withBitmapAt(0).withRunAt(2).build(), 20),
          Arguments.of(testCase().withRunAt(0).withBitmapAt(2).build(), 20),
          Arguments.of(testCase().withArrayAt(0).withBitmapAt(2).build(), 20),
          Arguments.of(testCase().withBitmapAt(0).withRunAt(1).withArrayAt(2).build(), 20),
          Arguments.of(testCase().withRunAt(0).withBitmapAt(1).withBitmapAt(2).build(), 20),
          Arguments.of(testCase().withArrayAt(0).withBitmapAt(1).withRunAt(2).build(), 20),
          Arguments.of(testCase().withBitmapAt(0).withRunAt(2).withArrayAt(4).build(), 20),
          Arguments.of(testCase().withRunAt(0).withBitmapAt(2).withBitmapAt(4).build(), 20),
          Arguments.of(testCase().withArrayAt(0).withBitmapAt(2).withRunAt(4).build(), 20),
          Arguments.of(testCase().withRange(0, 1 << 16).build(), 20)
        };
  }

  @AfterAll
  public static void clear() {
    DATA = null;
  }

  public static Stream<Arguments> params() {
    return Stream.of(DATA);
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("params")
  public void testElementwiseOffsetAppliedCorrectly(RoaringBitmap bitmap, int offset) {
    int[] array1 = bitmap.toArray();
    for (int i = 0; i < array1.length; ++i) {
      array1[i] += offset;
    }
    RoaringBitmap shifted = RoaringBitmap.addOffset(bitmap, offset);
    assertArrayEquals(array1, shifted.toArray(), failureMessage(bitmap));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("params")
  public void testElementwiseOffsetAppliedCorrectlyBuffer(RoaringBitmap bitmap, int offset) {
    int[] array1 = bitmap.toArray();
    for (int i = 0; i < array1.length; ++i) {
      array1[i] += offset;
    }
    MutableRoaringBitmap shifted =
        MutableRoaringBitmap.addOffset(bitmap.toMutableRoaringBitmap(), offset);
    assertArrayEquals(array1, shifted.toArray(), failureMessage(bitmap));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("params")
  public void testCardinalityPreserved(RoaringBitmap bitmap, int offset) {
    RoaringBitmap shifted = RoaringBitmap.addOffset(bitmap, offset);
    assertEquals(bitmap.getCardinality(), shifted.getCardinality(), failureMessage(bitmap));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("params")
  public void testCardinalityPreservedBuffer(RoaringBitmap bitmap, int offset) {
    MutableRoaringBitmap shifted =
        MutableRoaringBitmap.addOffset(bitmap.toMutableRoaringBitmap(), offset);
    assertEquals(bitmap.getCardinality(), shifted.getCardinality(), failureMessage(bitmap));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("params")
  public void canSerializeAndDeserialize(RoaringBitmap bitmap, int offset) throws IOException {
    RoaringBitmap shifted = RoaringBitmap.addOffset(bitmap, offset);
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    shifted.serialize(out);
    RoaringBitmap deserialized = new RoaringBitmap();
    deserialized.deserialize(ByteStreams.newDataInput(out.toByteArray()));
    assertEquals(shifted, deserialized, failureMessage(bitmap));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("params")
  public void canSerializeAndDeserializeBuffer(RoaringBitmap bitmap, int offset)
      throws IOException {
    MutableRoaringBitmap shifted =
        MutableRoaringBitmap.addOffset(bitmap.toMutableRoaringBitmap(), offset);
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    shifted.serialize(out);
    MutableRoaringBitmap deserialized = new MutableRoaringBitmap();
    deserialized.deserialize(ByteStreams.newDataInput(out.toByteArray()));
    assertEquals(shifted, deserialized, failureMessage(bitmap));
  }

  private static RoaringBitmap read(String classPathResource) {
    try {
      RoaringBitmapWriter<RoaringBitmap> writer =
          RoaringBitmapWriter.writer().constantMemory().get();
      Arrays.stream(
              Files.readFirstLine(new File(classPathResource), StandardCharsets.UTF_8).split(","))
          .mapToInt(Integer::parseInt)
          .forEach(writer::add);
      writer.flush();
      return writer.getUnderlying();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String failureMessage(RoaringBitmap bitmap) {
    return Arrays.toString(bitmap.toArray());
  }
}
