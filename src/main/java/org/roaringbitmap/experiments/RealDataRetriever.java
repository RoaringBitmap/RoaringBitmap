package org.roaringbitmap.experiments;

import java.io.*;
import java.util.*;

/**
 * This class will retrieve bitmaps that have been previously stored in a
 * portable format (as lists of ints)
 * 
 * 
 * @author Owen Kaser
 */
public class RealDataRetriever {

        File directory;

        public RealDataRetriever(String dirName) {
                directory = new File(dirName);
                if (!directory.exists() || !directory.isDirectory())
                        throw new RuntimeException("" + dirName
                                + " is not a directory");
        }

        public int[] fetchBitPositions(String dataset, int num) {
                File whichFile = new File(directory, dataset + num + ".txt");
                ArrayList<Integer> l = new ArrayList<Integer>();
                try {
                        BufferedReader buf = new BufferedReader(new FileReader(
                                whichFile));
                        String oneLine = buf.readLine(); // a single, perhaps
                                                         // very long, line
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
