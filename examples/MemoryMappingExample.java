import org.roaringbitmap.buffer.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;


public class MemoryMappingExample {
    
    public static void main(String[] args) throws IOException {
        File tmpfile = File.createTempFile("roaring", "bin");
        tmpfile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream(tmpfile);
        MutableRoaringBitmap Bitmap = MutableRoaringBitmap.bitmapOf(0, 2, 55,
                                64, 1 << 30);
        System.out.println("Created the bitmap "+Bitmap);
        Bitmap.serialize(new DataOutputStream(fos));
        long totalcount = fos.getChannel().position();
        System.out.println("Serialized total count = "+totalcount+" bytes");
        fos.close();
        RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
        ByteBuffer bb = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalcount);
        ImmutableRoaringBitmap mapped = new ImmutableRoaringBitmap(bb);
        System.out.println("Mapped the bitmap "+mapped);
        if(!mapped.equals(Bitmap)) throw new RuntimeException("This will not happen");
        memoryMappedFile.close();
    }
}