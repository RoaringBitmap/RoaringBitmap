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
        mrb.runOptimize(); //to improve compression
        ByteBuffer outbb = ByteBuffer.allocate(mrb.serializedSizeInBytes());
        mrb.serialize(new DataOutputStream(new OutputStream(){
            ByteBuffer mBB;
            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
            public void close() {}
            public void flush() {}
            public void write(int b) {
                mBB.put((byte) b);}
            public void write(byte[] b) {mBB.put(b);}
            public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
        }.init(outbb)));
        //
        outbb.flip();
        ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(outbb);
        System.out.println("read bitmap "+ irb);
        if( ! irb.equals(mrb) ) throw new RuntimeException("bug");

    }
}

/**
* The following classes could be used instead:
*/

class ByteBufferBackedInputStream extends InputStream {

  ByteBuffer buf;
  ByteBufferBackedInputStream( ByteBuffer buf){
    this.buf = buf;
  }
  public int read() throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    return 0xFF & buf.get();
  }
  public int read(byte[] bytes) throws IOException {
      int len = Math.min(bytes.length, buf.remaining());
      buf.get(bytes, 0, len);
      return len;
   }

  public long skip(long n) {
      int len = Math.min((int)n, buf.remaining());
      buf.position(buf.position() + (int)n);
      return len;
  }

  public int available() throws IOException {
      return buf.remaining();
  }

  public boolean markSupported() {
      return false;
  }

  public int read(byte[] bytes, int off, int len) throws IOException {
    len = Math.min(len, buf.remaining());
    buf.get(bytes, off, len);
    return len;
  }
}

class ByteBufferBackedOutputStream extends OutputStream{
  ByteBuffer buf;
  ByteBufferBackedOutputStream( ByteBuffer buf){
    this.buf = buf;
  }
  public synchronized void write(int b) throws IOException {
    buf.put((byte) b);
  }

  public synchronized void write(byte[] bytes) throws IOException {
    buf.put(bytes);
  }

  public synchronized void write(byte[] bytes, int off, int len) throws IOException {
    buf.put(bytes, off, len);
  }

}
