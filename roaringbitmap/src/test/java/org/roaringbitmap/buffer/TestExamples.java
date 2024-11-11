/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TestExamples {
  @Test
  public void serializationExample() throws IOException {
    File tmpfile = File.createTempFile("roaring", "bin");
    tmpfile.deleteOnExit();
    final FileOutputStream fos = new FileOutputStream(tmpfile);
    MutableRoaringBitmap Bitmap = MutableRoaringBitmap.bitmapOf(0, 2, 55, 64, 1 << 30);
    System.out.println("Created the bitmap " + Bitmap);
    // If there were runs of consecutive values, you could
    // call Bitmap.runOptimize(); to improve compression
    Bitmap.serialize(new DataOutputStream(fos));
    long totalcount = fos.getChannel().position();
    System.out.println("Serialized total count = " + totalcount + " bytes");
    fos.close();
    RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
    ByteBuffer bb = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalcount);
    ImmutableRoaringBitmap mapped = new ImmutableRoaringBitmap(bb);
    System.out.println("Mapped the bitmap " + mapped);
    memoryMappedFile.close();
    if (!mapped.equals(Bitmap)) {
      throw new RuntimeException("This will not happen");
    }
  }
}
