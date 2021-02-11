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

public class ForEachExample {


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
        final int[] count = {0};
        rb.forEach(value -> {
            if((value % 1500) == 0) {
                count[0] ++;
            }
        });
        System.out.println("There are "+count[0]+" values divisible by 1500.");
    }
}
