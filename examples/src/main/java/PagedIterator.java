import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

// demonstrates how to iterator over blocks of pageSize integers efficiently
public class PagedIterator {

  public static void main(String[] args) {
    RoaringBitmap rr = new RoaringBitmap();
    for (int k = 0; k < 100; k++) {
      rr.add(k * 4 + 31);
    }

    IntIterator i = rr.getIntIterator();
    final int pageSize = 10;
    while (i.hasNext()) {
      // we print a page
      for (int k = 0; (k < pageSize) && i.hasNext(); k++) {
        System.out.print(i.next() + " ");
      }
      System.out.println();
    }
  }
}
