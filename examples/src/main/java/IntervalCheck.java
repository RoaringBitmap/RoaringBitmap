import org.roaringbitmap.RoaringBitmap;

public class IntervalCheck {

  public static void main(String[] args) {
    // some bitmap
    RoaringBitmap rr = RoaringBitmap.bitmapOf(1, 2, 3, 1000);

    // we want to check if it intersects a given range [10,1000]
    int low = 10;
    int high = 1000;
    RoaringBitmap range = new RoaringBitmap();
    range.add((long) low, (long) high + 1);
    //
    //

    System.out.println(RoaringBitmap.intersects(rr, range)); // prints true if they intersect
  }
}
