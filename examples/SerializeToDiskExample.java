/**
* This example shows how to serialize a Roaring bitmap to a file.
*
*
*
*/
import org.roaringbitmap.*;
import java.io.*;
import java.nio.*;
import java.util.*;

public class SerializeToDiskExample {


    public static void main(String[] args) throws IOException {
        RoaringBitmap rb = new RoaringBitmap();
        for (int k = 0; k < 100000; k+= 1000) {
            rb.add(k);
        }
        for (int k = 100000; k < 200000; ++k) {
            rb.add(3*k);
        }
        for (int k = 700000; k < 800000; ++k) {
            rb.add(k);
        }
        DataOutputStream out;
        String file1 = "bitmapwithoutruns.bin";
        out = new DataOutputStream(new FileOutputStream(file1));
        rb.serialize(out);
        out.close();
        rb.runOptimize();
        String file2 = "bitmapwithruns.bin";
        out = new DataOutputStream(new FileOutputStream(file2));
        rb.serialize(out);
        out.close();
        // verify:
        DataInputStream in;
        in = new DataInputStream(new FileInputStream(file1));
        RoaringBitmap rbtest = new RoaringBitmap();
        rbtest.deserialize(in);
        if(!rbtest.equals(rb)) throw new RuntimeException("bug!");
        in = new DataInputStream(new FileInputStream(file2));
        rbtest.deserialize(in);
        if(!rbtest.equals(rb)) throw new RuntimeException("bug!");
        System.out.println("Serialized bitmaps to "+file1+" and "+file2);
    }
}
