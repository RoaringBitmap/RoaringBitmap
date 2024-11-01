/**
 * This example shows how to serialize a Roaring bitmap to a ByteBuffer
 */
import java.io.*;
import java.nio.*;
import org.roaringbitmap.buffer.*;

public class SerializeToByteBufferExample {

  public static void main(String[] args) throws IOException {
    MutableRoaringBitmap mrb = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
    System.out.println("starting with  bitmap " + mrb);
    mrb.runOptimize(); // to improve compression
    ByteBuffer outbb = ByteBuffer.allocate(mrb.serializedSizeInBytes());
    mrb.serialize(outbb);
    outbb.flip();
    ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(outbb);
    System.out.println("read bitmap " + irb);
    if (!irb.equals(mrb)) throw new RuntimeException("bug");
  }
}
