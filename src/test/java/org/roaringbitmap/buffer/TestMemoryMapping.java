package org.roaringbitmap.buffer;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings({ "javadoc", "deprecation", "static-method" })
public class TestMemoryMapping {
        

        @Test
        public void basic() {
                for (int k = 0; k < mappedbitmaps.size(); ++k) {
                        Assert.assertTrue(mappedbitmaps.get(k).equals(
                                rambitmaps.get(k)));
                }
        }

        @Test
        public void complements() {
                System.out.println("testing complements");
                for (int k = 0; k < mappedbitmaps.size() - 1; k += 4) {
                        final RoaringBitmap rb = ImmutableRoaringBitmap.andNot(
                                mappedbitmaps.get(k), mappedbitmaps.get(k + 1));
                        final RoaringBitmap rbram = ImmutableRoaringBitmap
                                .andNot(rambitmaps.get(k),
                                        rambitmaps.get(k + 1));
                        Assert.assertTrue(rb.equals(rbram));
                }
        }

        @Test
        public void intersections() {
                System.out.println("testing intersections");
                for (int k = 0; k < mappedbitmaps.size() - 4; k += 4) {
                        final RoaringBitmap rb = FastAggregation.and(
                                mappedbitmaps.get(k), mappedbitmaps.get(k + 1),
                                mappedbitmaps.get(k + 3),
                                mappedbitmaps.get(k + 4));
                        final RoaringBitmap rbram = FastAggregation.and(
                                rambitmaps.get(k), rambitmaps.get(k + 1),
                                rambitmaps.get(k + 3), rambitmaps.get(k + 4));
                        Assert.assertTrue(rb.equals(rbram));
                }
        }

        @Test
        public void unions() {
                System.out.println("testing Unions");
                for (int k = 0; k < mappedbitmaps.size() - 4; k += 4) {
                        final RoaringBitmap rb = FastAggregation.or(
                                mappedbitmaps.get(k), mappedbitmaps.get(k + 1),
                                mappedbitmaps.get(k + 3),
                                mappedbitmaps.get(k + 4));
                        final RoaringBitmap rbram = FastAggregation.or(
                                rambitmaps.get(k), rambitmaps.get(k + 1),
                                rambitmaps.get(k + 3), rambitmaps.get(k + 4));
                        Assert.assertTrue(rb.equals(rbram));
                }
        }

        @Test
        public void XORs() {
                System.out.println("testing XORs");
                for (int k = 0; k < mappedbitmaps.size() - 4; k += 4) {
                        final RoaringBitmap rb = FastAggregation.xor(
                                mappedbitmaps.get(k), mappedbitmaps.get(k + 1),
                                mappedbitmaps.get(k + 3),
                                mappedbitmaps.get(k + 4));
                        final RoaringBitmap rbram = FastAggregation.xor(
                                rambitmaps.get(k), rambitmaps.get(k + 1),
                                rambitmaps.get(k + 3), rambitmaps.get(k + 4));
                        Assert.assertTrue(rb.equals(rbram));
                }
        }

        @AfterClass
        public static void clearFiles() {
                System.out.println("Cleaning memory-mapped file.");
                out = null;
                rambitmaps = null;
                mappedbitmaps = null;
                tmpfile.delete();
        }

        @BeforeClass
        public static void initFiles() throws IOException {
                System.out
                        .println("Setting up memory-mapped file. (Can take some time.)");
                final ArrayList<Long> offsets = new ArrayList<Long>();
                tmpfile = File.createTempFile("roaring", "bin");
                tmpfile.deleteOnExit();
                final FileOutputStream fos = new FileOutputStream(tmpfile);
                final DataOutputStream dos = new DataOutputStream(fos);
                for (int N = 65536 * 16; N <= 65536 * 128; N *= 2) {
                        for (int gap = 1; gap <= 65536; gap *= 4) {
                                final RoaringBitmap rb1 = new RoaringBitmap();
                                for (int x = 0; x < N; x += gap) {
                                        rb1.add(x);
                                }
                                rambitmaps.add(rb1);
                                offsets.add(fos.getChannel().position());
                                rb1.serialize(dos);
                                dos.flush();
                                for (int offset = 1; offset <= gap; offset *= 8) {
                                        final RoaringBitmap rb2 = new RoaringBitmap();
                                        for (int x = 0; x < N; x += gap) {
                                                rb2.add(x + offset);
                                        }
                                        offsets.add(fos.getChannel().position());
                                        rb2.serialize(dos);
                                        dos.flush();
                                        rambitmaps.add(rb2);
                                }
                        }
                }
                final long totalcount = fos.getChannel().position();
                System.out.println("Wrote " + totalcount / 1024 + " KB");
                offsets.add(totalcount);
                dos.close();
                final RandomAccessFile memoryMappedFile = new RandomAccessFile(
                        tmpfile, "r");
                out = memoryMappedFile.getChannel().map(
                        FileChannel.MapMode.READ_ONLY, 0, totalcount);
                final long bef = System.currentTimeMillis();
                for (int k = 0; k < offsets.size() - 1; ++k) {
                        out.position((int) offsets.get(k).longValue());
                        final ByteBuffer bb = out.slice();
                        bb.limit((int) (offsets.get(k + 1).longValue() - offsets
                                .get(k).longValue()));
                        mappedbitmaps.add(new ImmutableRoaringBitmap(bb));
                }
                final long aft = System.currentTimeMillis();
                System.out.println("Mapped " + (offsets.size() - 1)
                        + " bitmaps in " + (aft - bef) + "ms");
        }

        static ArrayList<ImmutableRoaringBitmap> mappedbitmaps = new ArrayList<ImmutableRoaringBitmap>();

        static MappedByteBuffer out;

        static ArrayList<RoaringBitmap> rambitmaps = new ArrayList<RoaringBitmap>();

        static File tmpfile;
}
