package org.roaringbitmap.buffer;

import org.roaringbitmap.PeekableCharIterator;

import java.util.Random;

class ShowContainsValidity {
  public MappeableRunContainer rc;
  public MappeableBitmapContainer bc;
  public void setup() {
    rc = new MappeableRunContainer();
    bc = new MappeableBitmapContainer();
    Random r = new Random(0);
    int begin, end = 0;
    for (int i = 0; i < 9; i++) {
      begin = end + r.nextInt(10);
      end = begin + r.nextInt(100);
      rc = (MappeableRunContainer) rc.add(begin, end);
      int value = (begin & 0xFFFF) + r.nextInt(end - begin);
      bc = (MappeableBitmapContainer) bc.add((char) (value));
      System.out.println(rc.contains(bc) + " " + rc.contains2(bc));
      System.out.println(bc.getCardinality() + ": " + value + "|" + rc.getCardinality() + ": " + begin + "-" + end);
    }
    System.err.println("================================");
  }

  public static void main(String[] args) {
    ShowContainsValidity x = new ShowContainsValidity();
    x.setup();
    PeekableCharIterator xx = x.rc.getCharIterator();
    while(xx.hasNext()) {
      System.out.println("rc: " + (xx.next() & 0xFFFF));
    }
    System.out.println("==========");
    PeekableCharIterator xx2 = x.bc.getCharIterator();
    while(xx2.hasNext()) {
      System.out.println("bc:" + (xx2.next() & 0xFFFF));
    }
  }

}