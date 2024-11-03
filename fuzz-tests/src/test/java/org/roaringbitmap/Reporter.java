package org.roaringbitmap;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class Reporter {

  private static final String OUTPUT_DIR =
      System.getProperty("org.roaringbitmap.fuzz.output", System.getProperty("user.dir"));
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static synchronized void report(
      String testName,
      Map<String, Object> context,
      Throwable error,
      ImmutableBitmapDataProvider... bitmaps) {
    try {
      Map<String, Object> output = new LinkedHashMap<>();
      output.put("testName", testName);
      output.put("error", getStackTrace(error));
      output.putAll(context);
      String[] base64 = new String[bitmaps.length];
      for (int i = 0; i < bitmaps.length; ++i) {
        ByteArrayDataOutput serialised =
            ByteStreams.newDataOutput(bitmaps[i].serializedSizeInBytes());
        bitmaps[i].serialize(serialised);
        base64[i] = Base64.getEncoder().encodeToString(serialised.toByteArray());
      }
      output.put("bitmaps", base64);
      Path dir = Paths.get(OUTPUT_DIR);
      if (!Files.exists(dir)) {
        Files.createDirectory(dir);
      }
      Files.write(
          dir.resolve(testName + "-" + UUID.randomUUID() + ".json"),
          MAPPER.writeValueAsBytes(output));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static String getStackTrace(Throwable t) {
    return String.join(
        "\n\t",
        Arrays.stream(t.getStackTrace()).map(StackTraceElement::toString).collect(toList()));
  }
}
