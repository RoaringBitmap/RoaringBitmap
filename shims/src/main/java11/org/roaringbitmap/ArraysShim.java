package org.roaringbitmap;

import java.util.Arrays;

class ArraysShim {

  public static boolean equals(short[] x, int xmin, int xmax, short[] y, int ymin, int ymax) {
    return Arrays.equals(x, xmin, xmax, y, ymin, ymax);
  }
}
