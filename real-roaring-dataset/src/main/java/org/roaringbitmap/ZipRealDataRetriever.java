package org.roaringbitmap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
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
  public Iterable<int[]> fetchBitPositions() throws IOException {
    final ZipInputStream zis = getResourceAsStream();

    return new Iterable<int[]>() {

      @Override
      public Iterator<int[]> iterator() {
        return new Iterator<int[]>() {

          ZipEntry nextEntry = nextEntry();

          @Override
          public boolean hasNext() {
            return nextEntry != null;
          }

          @Override
          public int[] next() {
            try (
                BufferedReader buf = new BufferedReader(new InputStreamReader(zis));
            ) {
              String oneLine = buf.readLine(); // a single, perhaps very long, line
              String[] positions = oneLine.split(",");
              int[] ans = new int[positions.length];
              for (int i = 0; i < positions.length; i++) {
                ans[i] = Integer.parseInt(positions[i]);
              }
              return ans;
            } catch (IOException e) {
              throw new RuntimeException(e);
            } finally {
              nextEntry = nextEntry();
            }
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          private ZipEntry nextEntry() {
            try {
              return zis.getNextEntry();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
      }

    };
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
