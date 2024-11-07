package org.roaringbitmap.longlong;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Issue319 {

  public static void main(String[] args) throws IOException {
    RoaringBitmap mrb = RoaringBitmap.bitmapOf(1, 2, 3, 1000);
    for (int k = 0; k < 1000000000; k += 13) mrb.add(k);
    mrb.runOptimize();
    int count = 30;
    byte[] array = serialize(mrb);
    long bef, aft;
    long sum = 0;
    System.out.println("recommended: ");

    for (int k = 0; k < count; k++) {
      bef = System.currentTimeMillis();
      RoaringBitmap ret = new RoaringBitmap();
      try {
        ret.deserialize(
            new java.io.DataInputStream(
                new java.io.InputStream() {
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
      aft = System.currentTimeMillis();
      System.out.print(aft - bef + " ms ");
      sum += aft - bef;
      if (!ret.equals(mrb)) throw new RuntimeException("bug");
    }
    System.out.println("\naverage: " + sum / count);

    System.out.println("via ByteArrayInputStream: ");
    sum = 0;
    for (int k = 0; k < count; k++) {
      bef = System.currentTimeMillis();
      RoaringBitmap ret = new RoaringBitmap();
      ret.deserialize(new DataInputStream(new ByteArrayInputStream(array)));
      aft = System.currentTimeMillis();
      System.out.print(aft - bef + " ms ");
      sum += aft - bef;
      if (!ret.equals(mrb)) throw new RuntimeException("bug");
    }
    System.out.println("\naverage: " + sum / count);

    System.out.println("via Immutable: ");
    sum = 0;
    for (int k = 0; k < count; k++) {
      bef = System.currentTimeMillis();
      RoaringBitmap ret = new ImmutableRoaringBitmap(ByteBuffer.wrap(array)).toRoaringBitmap();
      aft = System.currentTimeMillis();
      System.out.print(aft - bef + " ms ");
      sum += aft - bef;
      if (!ret.equals(mrb)) throw new RuntimeException("bug");
    }
    System.out.println("\naverage: " + sum / count);

    System.out.println("via Deserialize ByteBuffer: ");
    sum = 0;
    for (int k = 0; k < count; k++) {
      bef = System.currentTimeMillis();
      RoaringBitmap ret = new RoaringBitmap();
      ret.deserialize(ByteBuffer.wrap(array));
      aft = System.currentTimeMillis();
      System.out.print(aft - bef + " ms ");
      sum += aft - bef;
      if (!ret.equals(mrb)) throw new RuntimeException("bug");
    }
    System.out.println("\naverage: " + sum / count);
  }

  private static byte[] serialize(RoaringBitmap mrb) {
    byte[] array = new byte[mrb.serializedSizeInBytes()];
    try {
      mrb.serialize(
          new java.io.DataOutputStream(
              new java.io.OutputStream() {
                int c = 0;

                @Override
                public void close() {}

                @Override
                public void flush() {}

                @Override
                public void write(int b) {
                  array[c++] = (byte) b;
                }

                @Override
                public void write(byte[] b) {
                  write(b, 0, b.length);
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
    return array;
  }
}
