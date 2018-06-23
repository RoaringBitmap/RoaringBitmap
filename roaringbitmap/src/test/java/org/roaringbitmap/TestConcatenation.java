package org.roaringbitmap;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.roaringbitmap.RandomisedTestData.TestDataSet.testCase;

@RunWith(Parameterized.class)
public class TestConcatenation {

  @Parameterized.Parameters
  public static Object[][] params() {
    return new Object[][]
            {
                    // the data set reported in issue #260
                    {read("src/test/resources/testdata/testIssue260.txt"), 5950},
                    {read("src/test/resources/testdata/offset_failure_case_1.txt"), 20},
                    {read("src/test/resources/testdata/offset_failure_case_2.txt"), 20},
                    {read("src/test/resources/testdata/offset_failure_case_3.txt"), 20},
                    // a range of test cases with offsets being divisors of 65536
                    {testCase().withBitmapAt(0).withRunAt(1).withArrayAt(2).build(), 1 << 16},
                    {testCase().withRunAt(0).withBitmapAt(1).withBitmapAt(2).build(), 1 << 16},
                    {testCase().withBitmapAt(0).withBitmapAt(1).withRunAt(2).build(), 1 << 16},
                    {testCase().withBitmapAt(0).withRunAt(2).withArrayAt(4).build(), 1 << 16},
                    {testCase().withRunAt(0).withBitmapAt(2).withBitmapAt(4).build(), 1 << 16},
                    {testCase().withArrayAt(0).withBitmapAt(2).withRunAt(4).build(), 1 << 16},
                    // awkward offsets
                    {testCase().withBitmapAt(0).build(), 20},
                    {testCase().withRunAt(0).build(), 20},
                    {testCase().withArrayAt(0).build(), 20},
                    {testCase().withBitmapAt(0).withRunAt(1).build(), 20},
                    {testCase().withRunAt(0).withBitmapAt(1).build(), 20},
                    {testCase().withArrayAt(0).withBitmapAt(1).build(), 20},
                    {testCase().withBitmapAt(0).withRunAt(2).build(), 20},
                    {testCase().withRunAt(0).withBitmapAt(2).build(), 20},
                    {testCase().withArrayAt(0).withBitmapAt(2).build(), 20},
                    {testCase().withBitmapAt(0).withRunAt(1).withArrayAt(2).build(), 20},
                    {testCase().withRunAt(0).withBitmapAt(1).withBitmapAt(2).build(), 20},
                    {testCase().withArrayAt(0).withBitmapAt(1).withRunAt(2).build(), 20},
                    {testCase().withBitmapAt(0).withRunAt(2).withArrayAt(4).build(), 20},
                    {testCase().withRunAt(0).withBitmapAt(2).withBitmapAt(4).build(), 20},
                    {testCase().withArrayAt(0).withBitmapAt(2).withRunAt(4).build(), 20},
                    {testCase().withRange(0, 1 << 16).build(), 20}
            };
  }

  private final RoaringBitmap bitmap;
  private final int offset;

  public TestConcatenation(RoaringBitmap bitmap, int offset) {
    this.bitmap = bitmap;
    this.offset = offset;
  }

  @Test
  public void offsetAppliedCorrectly() {
    int[] array1 = bitmap.toArray();
    for (int i = 0; i < array1.length; ++i) {
      array1[i] += offset;
    }
    String inputBitmapAsString = Arrays.toString(bitmap.toArray());
    RoaringBitmap shifted = RoaringBitmap.addOffset(bitmap, offset);
    assertEquals(inputBitmapAsString, bitmap.getCardinality(), shifted.getCardinality());
    assertArrayEquals(inputBitmapAsString, array1, shifted.toArray());
  }


  @Test
  public void canSerializeAndDeserialize() throws IOException {
    RoaringBitmap shifted = RoaringBitmap.addOffset(bitmap, offset);
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    shifted.serialize(out);

    RoaringBitmap deserialized = new RoaringBitmap();
    deserialized.deserialize(ByteStreams.newDataInput(out.toByteArray()));

    assertEquals(shifted, deserialized);

  }

  private static RoaringBitmap read(String classPathResource) {
    try {
      OrderedWriter writer = new OrderedWriter();
      Arrays.stream(Files.readFirstLine(new File(classPathResource), Charset.forName("UTF-8")).split(","))
              .mapToInt(Integer::parseInt)
              .forEach(writer::add);
      writer.flush();
      return writer.getUnderlying();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
