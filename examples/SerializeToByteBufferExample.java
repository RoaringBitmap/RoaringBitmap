/**
* This example shows how to serialize a Roaring bitmap to a ByteBuffer
*/
import org.roaringbitmap.buffer.*;
import java.io.*;
import java.nio.*;

public class SerializeToByteBufferExample {
    public static void main(String[] args) throws IOException{
        MutableRoaringBitmap mrb = MutableRoaringBitmap.bitmapOf(1,2,3,1000); 
        System.out.println("starting with  bitmap "+ mrb);
        ByteBuffer outbb = ByteBuffer.allocate(1024*1024);
        mrb.serialize(new DataOutputStream(new OutputStream(){
            ByteBuffer mBB;
            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
            public void close() {}
            public void flush() {}
            public void write(int b) {mBB.put((byte) b);}
            public void write(byte[] b) {}            
            public void write(byte[] b, int off, int l) {}
        }.init(outbb)));
        System.out.println("wrote "+outbb.position()+" bytes to ByteBuffer");
        //
        outbb.flip();
        ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(outbb);
        System.out.println("read bitmap "+ irb);        
    }
}