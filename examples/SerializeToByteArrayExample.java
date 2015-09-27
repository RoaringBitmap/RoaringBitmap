/**
* This example shows how to serialize a Roaring bitmap to a byte array 
*/
import org.roaringbitmap.RoaringBitmap;
import java.io.*;
import java.nio.*;

public class SerializeToByteArrayExample {





    public static void main(String[] args)  {
        RoaringBitmap mrb = RoaringBitmap.bitmapOf(1,2,3,1000);
        System.out.println("starting with  bitmap "+ mrb);
        mrb.runOptimize(); //to improve compression
        byte[] array = new byte[mrb.serializedSizeInBytes()];
        try {
            mrb.serialize(new java.io.DataOutputStream(new java.io.OutputStream() {
                int c = 0;

                @Override
                public void close() {
                }

                @Override
                public void flush() {
                }

                @Override
                public void write(int b) {
                    array[c++] = (byte)b;
                }

                @Override
                public void write(byte[] b) {
                    write(b,0,b.length);
                }

                @Override
                public void write(byte[] b, int off, int l) {
                    System.arraycopy(b, off, array, c, l);
                    c += l;
                }
            }));
        } catch (IOException ioe) {
            // should never happen because we write to a byte array
            throw new RuntimeException("unexpected error while serializing to a byte array");
        }
        RoaringBitmap ret = new RoaringBitmap();
        try {
            ret.deserialize(new java.io.DataInputStream(new java.io.InputStream() {
                int c = 0;

                @Override
                public int read() {
                    return array[c++] & 0xff;
                }

                @Override
                public int read(byte b[]) {
                 return read(b, 0, b.length);
                }

                @Override
                public int read(byte[] b, int off, int l) {
                    System.arraycopy(array, c, b, off, l);
                    c += l;
                    return l;
                }
            }));
        } catch (IOException ioe) {
            // should never happen because we read from a byte array
            throw new RuntimeException("unexpected error while deserializing from a byte array");
        }
        if(!ret.equals(mrb)) throw new RuntimeException("bug");
        System.out.println("decoded from byte array : "+ret);

    }
}
