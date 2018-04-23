package org.roaringbitmap.buffer;


import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;


public class TestAdversarialInputs {

	// copy to a temporary file
	private File copy(String resourceName) throws IOException {
		// old-school Java, could be improved
        File tmpfile = File.createTempFile("RoaringBitmapTestAdversarialInputs", "bin");
        tmpfile.deleteOnExit();
        OutputStream resStreamOut = null;
        InputStream stream = null;
        try {
        	    ClassLoader classLoader = getClass().getClassLoader();
            stream = classLoader.getResourceAsStream(resourceName);
            if(stream == null) {
                throw new IOException("Cannot get resource \"" + resourceName + "\".");
            }
            int readBytes;
            byte[] buffer = new byte[4096];
            resStreamOut = new FileOutputStream(tmpfile);
            while ((readBytes = stream.read(buffer)) > 0) {
                resStreamOut.write(buffer, 0, readBytes);
            }
        } finally {
            if(stream != null) stream.close();
            if(resStreamOut != null) resStreamOut.close();
        }
        return tmpfile;
    }

	public ByteBuffer memoryMap(String resourceName) throws IOException {
		File tmpfile = copy(resourceName);
		long totalcount = tmpfile.length();
		RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
        ByteBuffer bb = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalcount); // even though we have two bitmaps, we have one map, maps are expensive!!!
        memoryMappedFile.close(); // we can safely close
        bb.position(0);
        return bb;
	}

	@Test
	public void testInputGoodFile1() throws IOException {
		File file = copy("testdata/bitmapwithruns.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not throw an exception
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		assertEquals(rb.getCardinality(), 200100);
		file.delete();
	}

	@Test
	public void testInputGoodFile1Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/bitmapwithruns.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		assertEquals(rb.getCardinality(), 200100);
	}

	@Test
	public void testInputGoodFile2() throws IOException {
		File file = copy("testdata/bitmapwithoutruns.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not throw an exception
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		assertEquals(rb.getCardinality(), 200100);
		file.delete();
	}

	@Test
	public void testInputGoodFile2Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/bitmapwithoutruns.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		assertEquals(rb.getCardinality(), 200100);
	}

	@Test(expected = IOException.class)
	public void testInputBadFile1() throws IOException {
		File file = copy("testdata/crashproneinput1.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testInputBadFile1Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/crashproneinput1.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		System.out.println(rb.getCardinality()); // won't get here
	}

	@Test(expected = IOException.class)
	public void testInputBadFile2() throws IOException {
		File file = copy("testdata/crashproneinput2.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testInputBadFile2Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/crashproneinput2.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		System.out.println(rb.getCardinality()); // won't get here
	}

	@Test(expected = IOException.class)
	public void testInputBadFile3() throws IOException {
		File file = copy("testdata/crashproneinput3.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testInputBadFile3Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/crashproneinput3.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		System.out.println(rb.getCardinality()); // won't get here
	}

	@Test(expected = IOException.class)
	public void testInputBadFile4() throws IOException {
		File file = copy("testdata/crashproneinput4.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testInputBadFile4Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/crashproneinput4.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		System.out.println(rb.getCardinality()); // won't get here
	}

	@Test(expected = IOException.class)
	public void testInputBadFile5() throws IOException {
		File file = copy("testdata/crashproneinput5.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testInputBadFile5Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/crashproneinput5.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		System.out.println(rb.getCardinality()); // won't get here
	}

	@Test(expected = IOException.class)
	public void testInputBadFile6() throws IOException {
		File file = copy("testdata/crashproneinput6.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testInputBadFile6Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/crashproneinput6.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		System.out.println(rb.getCardinality()); // won't get here
	}

	@Test(expected = IOException.class)
	public void testInputBadFile7() throws IOException {
		File file = copy("testdata/crashproneinput7.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}


	@Test(expected = IllegalArgumentException.class)
	public void testInputBadFile7Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/crashproneinput7.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		System.out.println(rb.getCardinality()); // won't get here
	}

	@Test(expected = IOException.class)
	public void testInputBadFile8() throws IOException {
		File file = copy("testdata/crashproneinput8.bin");
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testInputBadFile8Mapped() throws IOException {
		ByteBuffer bb = memoryMap("testdata/crashproneinput8.bin");
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		System.out.println(rb.getCardinality()); // won't get here
	}
}
