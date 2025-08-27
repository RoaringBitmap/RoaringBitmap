package org.roaringbitmap.art;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.Container;

public class SimpleContainers {
  static Container makeContainer(int first) {
    Container result = new ArrayContainer(1);
    result.add((char) first);
    return result;
  }
}
