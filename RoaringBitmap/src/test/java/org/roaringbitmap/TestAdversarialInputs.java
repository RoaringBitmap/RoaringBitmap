package org.roaringbitmap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestAdversarialInputs {

	public static Stream<Arguments> badFiles() {
		return IntStream.rangeClosed(1, 7)
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

	@Test
	public void testInputGoodFile1() throws IOException {
		File file = copy("testdata/bitmapwithruns.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not throw an exception
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		assertEquals(rb.getCardinality(), 200100);
		file.delete();
	}

	@Test
	public void testInputGoodFile2() throws IOException {
		File file = copy("testdata/bitmapwithoutruns.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not throw an exception
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		assertEquals(rb.getCardinality(), 200100);
		file.delete();
	}

	@ParameterizedTest
	@MethodSource("badFiles")
	public void testInputBadFile8(String fileName) {
		assertThrows(IOException.class, () -> deserialize(fileName));
	}


	private void deserialize(String fileName) throws IOException {
		File file = copy(fileName);
		RoaringBitmap rb = new RoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}
}
