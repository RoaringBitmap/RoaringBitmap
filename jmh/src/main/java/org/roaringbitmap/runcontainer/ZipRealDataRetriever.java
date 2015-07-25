package org.roaringbitmap.runcontainer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * This class will retrieve bitmaps that have been previously stored in a
 * portable format (as lists of ints) insize a zip file
 * 
 * 
 * @author Daneil Lemire
 */
 public class ZipRealDataRetriever {

    ZipFile directory;

    public ZipRealDataRetriever(String dirName) throws IOException {
        directory = new ZipFile(dirName);
        
    }
    
    public String getName() {
       return directory.getName();
    }
    
    Iterable<String> files() {
        @SuppressWarnings("unchecked")
        final Enumeration<ZipEntry> e = (Enumeration<ZipEntry>) directory.entries();
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    @Override
                    public boolean hasNext() {
                        return e.hasMoreElements();
                    }

                    @Override
                    public String next() {
                        return e.nextElement().getName();
                    }
                    
                };
            }
            
        };
    }
    
    
    Enumeration<String> entries() {
        @SuppressWarnings("unchecked")
        final Enumeration<ZipEntry> e = (Enumeration<ZipEntry>) directory.entries();
        return new Enumeration<String>() {

            @Override
            public boolean hasMoreElements() {
                return e.hasMoreElements();
            }

            @Override
            public String nextElement() {
                return e.nextElement().getName();
            }
            
        };
    }


    

    public int[] fetchBitPositions(String entrystr) {
        ZipEntry E = directory.getEntry(entrystr);
        ArrayList<Integer> l = new ArrayList<Integer>();
        try {
            InputStream is = directory.getInputStream(E);
            BufferedReader buf = new BufferedReader(new InputStreamReader(is));
            String oneLine = buf.readLine(); // a single, perhaps
                                                // very long, line
            buf.close();
            for (String entry : oneLine.split(","))
                l.add(Integer.parseInt(entry));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int[] ans = new int[l.size()];
        int ctr = 0;
        for (int val : l)
            ans[ctr++] = val;
        return ans;
    }

}