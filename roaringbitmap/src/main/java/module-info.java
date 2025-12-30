/**
 * This module class contains the public packages for the RoaringBitmap library.
 */
module roaringbitmap {
  requires jdk.incubator.vector;
  exports org.roaringbitmap;
  exports org.roaringbitmap.buffer;
  exports org.roaringbitmap.longlong;
  exports org.roaringbitmap.insights;
}
