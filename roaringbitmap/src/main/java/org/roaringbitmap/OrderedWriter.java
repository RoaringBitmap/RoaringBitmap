package org.roaringbitmap;

public interface OrderedWriter {
  void add(int value);

  void flush();
}
