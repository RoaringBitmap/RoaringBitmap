import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class Bitmap64 {

  public static void main(String[] args) {
    LongBitmapDataProvider r = Roaring64NavigableMap.bitmapOf(1, 2, 100, 1000);
    r.addLong(1234);
    System.out.println(r.contains(1)); // true
    System.out.println(r.contains(3)); // false
    LongIterator i = r.getLongIterator();
    while (i.hasNext()) System.out.println(i.next());
  }
}
