import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MemoryMappingExample {

  public static void main(String[] args) throws IOException {
    File tmpfile = File.createTempFile("roaring", "bin");
    tmpfile.deleteOnExit();
    final FileOutputStream fos = new FileOutputStream(tmpfile);
    MutableRoaringBitmap Bitmap1 = MutableRoaringBitmap.bitmapOf(0, 2, 55, 64, 1 << 30);
    System.out.println("Created the bitmap " + Bitmap1);
    MutableRoaringBitmap Bitmap2 = MutableRoaringBitmap.bitmapOf(0, 2, 55, 654, 1 << 31);
    System.out.println("Created the bitmap " + Bitmap2);
    int pos1 = 0; // bitmap 1 is at offset 0
    // If there were runs of consecutive values, you could
    // call Bitmap1.runOptimize(); to improve compression
    Bitmap1.serialize(new DataOutputStream(fos));
    int pos2 = Bitmap1.serializedSizeInBytes(); // bitmap 2 will be right after it
    // If there were runs of consecutive values, you could
    // call Bitmap2.runOptimize(); to improve compression
    Bitmap2.serialize(new DataOutputStream(fos));
    long totalcount = fos.getChannel().position();
    if (totalcount != Bitmap1.serializedSizeInBytes() + Bitmap2.serializedSizeInBytes())
      throw new RuntimeException("This will not happen.");
    System.out.println("Serialized total count = " + totalcount + " bytes");
    fos.close();
    RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
    ByteBuffer bb =
        memoryMappedFile
            .getChannel()
            .map(
                FileChannel.MapMode.READ_ONLY,
                0,
                totalcount); // even though we have two bitmaps, we have one map, maps are
    // expensive!!!
    memoryMappedFile.close(); // we can safely close
    bb.position(pos1);
    ImmutableRoaringBitmap mapped1 = new ImmutableRoaringBitmap(bb);
    System.out.println("Mapped the bitmap " + mapped1);
    bb.position(pos2);
    ImmutableRoaringBitmap mapped2 = new ImmutableRoaringBitmap(bb);
    System.out.println("Mapped the bitmap " + mapped2);
    if (!mapped2.equals(Bitmap2)) throw new RuntimeException("This will not happen");
  }
}
