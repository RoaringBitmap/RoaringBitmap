/**
 * This example shows how to serialize a Roaring bitmap to a String (Java 8)
 *
 *
 * It is not difficult to encode a byte array to a String so that it can be later recovered.
 * A standard way is to use Base 64 : https://en.wikipedia.org/wiki/Base64
 *
 */
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

public class SerializeToStringExample {

  // This example uses the Base64 class introduced in Java 8. Any byte[] to String encoder would do
  public static void main(String[] args) throws IOException {
    MutableRoaringBitmap mrb = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
    System.out.println("starting with  bitmap " + mrb);
    ByteBuffer outbb = ByteBuffer.allocate(mrb.serializedSizeInBytes());
    // If there were runs of consecutive values, you could
    // call mrb.runOptimize(); to improve compression
    mrb.serialize(outbb);
    //
    outbb.flip();
    String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
    ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(serializedstring));
    ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(newbb);
    System.out.println("read bitmap " + irb);
  }
}
