/**
 * This example shows how to serialize a Roaring bitmap to a file.
 *
 *
 *
 */
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class SerializeToDiskExample {

  public static void main(String[] args) throws IOException {
    RoaringBitmap rb = new RoaringBitmap();
    for (int k = 0; k < 100000; k += 1000) {
      rb.add(k);
    }
    for (int k = 100000; k < 200000; ++k) {
      rb.add(3 * k);
    }
    for (int k = 700000; k < 800000; ++k) {
      rb.add(k);
    }
    String file1 = "bitmapwithoutruns.bin";
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file1))) {
      rb.serialize(out);
    }
    rb.runOptimize();
    String file2 = "bitmapwithruns.bin";
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file2))) {
      rb.serialize(out);
    }
    // verify
    RoaringBitmap rbtest = new RoaringBitmap();
    try (DataInputStream in = new DataInputStream(new FileInputStream(file1))) {
      rbtest.deserialize(in);
      if (!rbtest.validate()) throw new RuntimeException("bug!");
    }
    if (!rbtest.equals(rb)) throw new RuntimeException("bug!");
    try (DataInputStream in = new DataInputStream(new FileInputStream(file2))) {
      rbtest.deserialize(in);
      if (!rbtest.validate()) throw new RuntimeException("bug!");
    }
    if (!rbtest.equals(rb)) throw new RuntimeException("bug!");
    System.out.println("Serialized bitmaps to " + file1 + " and " + file2);
  }
}
