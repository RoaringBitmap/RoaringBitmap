import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ImmutableRoaringBitmapExample {
  public static void main(String[] args) throws IOException {
    MutableRoaringBitmap rr1 = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
    MutableRoaringBitmap rr2 = MutableRoaringBitmap.bitmapOf(2, 3, 1010);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    // If there were runs of consecutive values, you could
    // call rr1.runOptimize(); or rr2.runOptimize(); to improve compression
    rr1.serialize(dos);
    rr2.serialize(dos);
    dos.close();
    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap rrback1 = new ImmutableRoaringBitmap(bb);
    bb.position(bb.position() + rrback1.serializedSizeInBytes());
    ImmutableRoaringBitmap rrback2 = new ImmutableRoaringBitmap(bb);
    System.out.println(rrback1);
    System.out.println(rrback2);
  }
}
