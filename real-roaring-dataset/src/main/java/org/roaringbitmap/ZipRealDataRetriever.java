package org.roaringbitmap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This class will retrieve bitmaps that have been previously stored in a portable format (as lists
 * of ints) inside a zip file
 *
 * @author Daniel Lemire
 */
public class ZipRealDataRetriever {

  private static final String REAL_ROARING_DATASET = "/real-roaring-dataset/";
  private static final String ZIP_EXTENSION = ".zip";

  private final String dataset;

  public ZipRealDataRetriever(String dataset) throws IOException, URISyntaxException {
    this.dataset = dataset;
  }

  public String getName() {
    return getResource().getPath();
  }

  /**
   * 
   * @return an {@link Iterable} of int[], as read from the resource
   * @throws IOException something went wrong while reading the resource
   */
  public List<int[]> fetchBitPositions() throws IOException {
    List<int[]> bitPositions = new ArrayList<>();

    try (final ZipInputStream zis = getResourceAsStream()) {
      BufferedReader buf = new BufferedReader(new InputStreamReader(zis));

      while (true) {
        ZipEntry nextEntry = zis.getNextEntry();
        if (nextEntry == null) {
          break;
        }

        try {
          String oneLine = buf.readLine(); // a single, perhaps very long, line
          String[] positions = oneLine.split(",");
          int[] ans = new int[positions.length];
          for (int i = 0; i < positions.length; i++) {
            ans[i] = Integer.parseInt(positions[i]);
          }
          bitPositions.add(ans);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return bitPositions;
  }

  private URL getResource() {
    return this.getClass().getResource(resource());
  }

  private ZipInputStream getResourceAsStream() {
    return new ZipInputStream(this.getClass().getResourceAsStream(resource()));
  }

  private String resource() {
    return REAL_ROARING_DATASET + dataset + ZIP_EXTENSION;
  }

}
