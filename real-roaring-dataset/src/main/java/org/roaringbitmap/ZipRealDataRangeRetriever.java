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
 * Retrieve range data in format start1-end1,start2-end2 from zip file
 *
 * @author Amit Desai
 */
public class ZipRealDataRangeRetriever<T> {

  private String REAL_ROARING_DATASET;
  private static final String ZIP_EXTENSION = ".zip";

  private final String dataset;

  public ZipRealDataRangeRetriever(String dataset, String folder)
      throws IOException, URISyntaxException {
    this.dataset = dataset;
    this.REAL_ROARING_DATASET = folder;
  }

  public String getName() {
    return getResource().getPath();
  }

  /**
   *
   * Returns next range from file
   * @return some Iterable
   * @throws IOException it can happen
   */
  public Iterable<int[][]> fetchNextRange() throws IOException {
    final ZipInputStream zis = getResourceAsStream();

    return new Iterable<int[][]>() {

      @Override
      public Iterator<int[][]> iterator() {
        return new Iterator<int[][]>() {

          ZipEntry nextEntry = nextEntry();

          @Override
          public boolean hasNext() {
            return nextEntry != null;
          }

          @Override
          public int[][] next() {
            try (BufferedReader buf = new BufferedReader(new InputStreamReader(zis))) {
              String oneLine = buf.readLine(); // a single, perhaps very long, line
              String[] positions = oneLine.split(",");
              int[][] ans = new int[positions.length][2];
              for (int i = 0; i < positions.length; i++) {
                String[] split = positions[i].split(":");
                ans[i][0] = Integer.parseInt(split[0]);
                ans[i][1] = Integer.parseInt(split[1]);
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
