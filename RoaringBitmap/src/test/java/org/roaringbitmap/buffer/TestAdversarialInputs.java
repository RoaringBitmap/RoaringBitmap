package org.roaringbitmap.buffer;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestAdversarialInputs {

	public static Stream<Arguments> badFiles() {
		return IntStream.rangeClosed(1, 8)
				.mapToObj(i -> Arguments.of("testdata/crashproneinput" + 3 + ".bin"));
	}

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

	@ParameterizedTest
	@MethodSource("badFiles")
	public void testInputBadFileDeserialize(String file) {
		assertThrows(IOException.class, () -> deserialize(file));
	}

	@ParameterizedTest
	@MethodSource("badFiles")
	public void testInputBadFileMap(String file) {
		assertThrows(IndexOutOfBoundsException.class, () -> map(file));
	}




	private void deserialize(String fileName) throws IOException {
		File file = copy(fileName);
		MutableRoaringBitmap rb = new MutableRoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}

	private void map(String fileName) throws IOException {
		ByteBuffer bb = memoryMap(fileName);
		ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(bb);
		System.out.println(rb.getCardinality()); // won't get here
	}
}
