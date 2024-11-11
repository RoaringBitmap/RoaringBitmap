import org.roaringbitmap.RoaringBitmap;

public class VeryLargeBitmap {

  public static void main(String[] args) {
    RoaringBitmap rb = new RoaringBitmap();
    rb.add(0L, 1L << 32); // the biggest bitmap we can create
    System.out.println(
        "memory usage: " + rb.getSizeInBytes() * 1.0 / (1L << 32) + " byte per value");
    if (rb.getLongCardinality() != (1L << 32)) throw new RuntimeException("bug!");
  }
}
