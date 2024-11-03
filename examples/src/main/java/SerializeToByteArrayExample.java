/**
 * This example shows how to serialize a Roaring bitmap to a byte array
 */
/***************
 * for performance considerations, see https://github.com/RoaringBitmap/RoaringBitmap/issues/319
 **************/

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SerializeToByteArrayExample {

  public static void main(String[] args) {
    RoaringBitmap mrb = RoaringBitmap.bitmapOf(1, 2, 3, 1000);
    System.out.println("starting with  bitmap " + mrb);
    mrb.runOptimize(); // to improve compression
    byte[] array = new byte[mrb.serializedSizeInBytes()];
    mrb.serialize(ByteBuffer.wrap(array));
    RoaringBitmap ret = new RoaringBitmap();
    try {
      ret.deserialize(ByteBuffer.wrap(array));
    } catch (IOException ioe) {
      ioe.printStackTrace(); // should not happen
    }
    if (!ret.equals(mrb)) throw new RuntimeException("bug");
    System.out.println("decoded from byte array : " + ret);
  }
}
