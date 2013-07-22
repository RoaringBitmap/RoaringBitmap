package me.lemire.roaringbitmap.experiments;

import java.lang.reflect.Array;
import java.text.DecimalFormat;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map.Entry;

import me.lemire.roaringbitmap.ArrayContainer;
import me.lemire.roaringbitmap.BitmapContainer;
import me.lemire.roaringbitmap.Container;
import me.lemire.roaringbitmap.RoaringBitmap;

public class Benchmark {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		test(10, 18, 10);
	}
	
	public static void testRoaringBitmap(int[][] data1, int[][] data2, int repeat, DecimalFormat df){
		System.out.println("# RoaringBitmap");
		System.out
				.println("# size, construction time, time to recover set bits, time to compute unions  and intersections ");
		long bef, aft;
		String line = "";		
		int N = data1.length;
		int size = 0;
		
		bef = System.currentTimeMillis();				
		RoaringBitmap roaring = null;
		for (int r = 0; r < repeat; ++r) {
		    roaring = new RoaringBitmap();
			for (int k = 0; k < N; ++k) 				
				for (int x = 0; x < data1[k].length; ++x) {
					//System.out.println("data = "+data[k][x]);
					roaring.add(data1[k][x]);
				}							
		}
		aft = System.currentTimeMillis();
		
		size = roaring.getSizeInBytes(); // return the size in bytes of the index 		
		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			int array[] = null;
			roaring.getIntegers(array);
		}
	   
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		//construction of the 2nd RoaringBitmap index
		RoaringBitmap roaring2 = new RoaringBitmap();
		for (int k = 0; k < N; ++k) 				
			for (int x = 0; x < data2[k].length; ++x) {
				//System.out.println("data = "+data[k][x]);
				roaring2.add(data2[k][x]);
			}
		
		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			RoaringBitmap rbor = RoaringBitmap.or(roaring, roaring2); 
			int array [] = null;
			rbor.getIntegers(array);
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		// logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			RoaringBitmap rband = RoaringBitmap.and(roaring, roaring2); 
			int array [] = null;
			rband.getIntegers(array);			
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		System.out.println(line);		
	}
	
	public static void testBitSet(int[][] data, int[][] data2, int repeat, DecimalFormat df) {
		System.out.println("# BitSet");
		System.out.println("# size, construction time, time to recover set bits, " +
						"time to compute unions  and intersections ");
		long bef, aft;
		String line = "";		
		int N = data.length;
		bef = System.currentTimeMillis();
		BitSet[] bitmap = new BitSet[N];
		int size = 0;
		for (int r = 0; r < repeat; ++r) {
			size = 0;
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new BitSet();
				for (int x = 0; x < data[k].length; ++x) {
					bitmap[k].set(data[k][x]);
				}
				size += bitmap[k].size() / 8;
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = new int[bitmap[k].cardinality()];
				int pos = 0;
				for (int i = bitmap[k].nextSetBit(0); i >= 0; i = bitmap[k]
						.nextSetBit(i + 1)) {
					array[pos++] = i;
				}
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		BitSet[] bitmap2 = new BitSet[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new BitSet();
			for (int x = 0; x < data2[k].length; ++x)
				bitmap2[k].set(data2[k][x]);						
		}
		
		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
		BitSet bitmapor1 = (BitSet) bitmap[0].clone();
		BitSet bitmapor2 = (BitSet) bitmap2[0].clone();
		for (int k = 1; k < N; ++k) {				
			bitmapor1.or(bitmap[k]);
			bitmapor2.or(bitmap2[k]);
		}
		bitmapor1.or(bitmapor2);
		int[] array = new int[bitmapor1.cardinality()];
		int pos = 0;
		for (int i = bitmapor1.nextSetBit(0); i >= 0; i = bitmapor1
					.nextSetBit(i + 1)) {
			array[pos++] = i;
		}
				   
		}	
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		// logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			BitSet bitmapand1 = (BitSet) bitmap[0].clone();
			BitSet bitmapand2 = (BitSet) bitmap2[0].clone();
			for (int k = 1; k < N; ++k) {				
				bitmapand1.and(bitmap[k]);
				bitmapand2.and(bitmap2[k]);
			}
			bitmapand1.and(bitmapand2);
			int[] array = new int[bitmapand1.cardinality()];
			int pos = 0;
			for (int i = bitmapand1.nextSetBit(0); i >= 0; i = bitmapand1
						.nextSetBit(i + 1)) {
				array[pos++] = i;
			}					   
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		System.out.println(line);		
	}

	@SuppressWarnings("unused")
	public static void test(int N, int nbr, int repeat) {
		DecimalFormat df = new DecimalFormat("0.###");
		ClusteredDataGenerator cdg = new ClusteredDataGenerator();
		System.out.println("# For each instance, we report the size, the construction time, ");
		System.out.println("# the time required to recover the set bits,");
		System.out
				.println("# and the time required to compute logical ors (unions) between lots of bitmaps.");
		for (int sparsity = 1; sparsity < 31 - nbr; sparsity++) {
			int[][] data = new int[N][];
			int[][] data2 = new int[N][];
			int Max = (1 << (nbr + sparsity));
			System.out.println("# generating random data...");
			//Generating the first set
			int[] inter = cdg.generateClustered(1 << (nbr / 2), Max);
			int counter = 0;
			for (int k = 0; k < N; ++k) {
				data[k] = IntUtil.unite(inter,
						cdg.generateClustered(1 << nbr, Max));
				counter += data[k].length;
			}
			//Generating the 2nd set
			inter = cdg.generateClustered(1 << (nbr / 2), Max);
			counter = 0;
			for (int k = 0; k < N; ++k) {
				data2[k] = IntUtil.unite(inter,
						cdg.generateClustered(1 << nbr, Max));
				counter += data2[k].length;
			}
			System.out.println("# generating random data... ok.");
			System.out.println("#  average set bit per 32-bit word = "
					+ df.format((counter / (data.length / 32.0 * Max))));

			// building			
			testBitSet(data, data2, repeat, df);
			testRoaringBitmap(data, data2, repeat, df);
			
			System.out.println();
		}
	}
}
