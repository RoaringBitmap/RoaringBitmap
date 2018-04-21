package org.roaringbitmap;

import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

	@Test(expected = IOException.class)
	public void testInputBadFile1() throws IOException {
		File file = copy("testdata/crashproneinput1.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}

	@Test(expected = IOException.class)
	public void testInputBadFile2() throws IOException {
		File file = copy("testdata/crashproneinput2.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}

	@Test(expected = IOException.class)
	public void testInputBadFile3() throws IOException {
		File file = copy("testdata/crashproneinput3.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}

	@Test(expected = IOException.class)
	public void testInputBadFile4() throws IOException {
		File file = copy("testdata/crashproneinput4.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}

	@Test(expected = IOException.class)
	public void testInputBadFile5() throws IOException {
		File file = copy("testdata/crashproneinput5.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}


	@Test(expected = IOException.class)
	public void testInputBadFile6() throws IOException {
		File file = copy("testdata/crashproneinput6.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}


	@Test(expected = IOException.class)
	public void testInputBadFile7() throws IOException {
		File file = copy("testdata/crashproneinput7.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}

	@Test(expected = IOException.class)
	public void testInputBadFile8() throws IOException {
		File file = copy("testdata/crashproneinput8.bin");
		RoaringBitmap rb = new RoaringBitmap();
		// should not work
		rb.deserialize(new DataInputStream(new FileInputStream(file)));
		file.delete();
	}
}
