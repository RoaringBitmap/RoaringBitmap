package me.lemire.roaringbitmap.experiments;

import java.text.DecimalFormat;
import java.util.BitSet;
import me.lemire.roaringbitmap.RoaringBitmap;

public class Benchmark {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		test(10, 18, 10);
	}
	

	public static void testRoaringBitmap(int[][] data, int[][] data2, int repeat, DecimalFormat df) {
		System.out.println("# RoaringBitmap");
		System.out.println("# size, construction time, time to recover set bits, " +
						"time to compute unions (OR), intersections (AND) " +
						"and exclusive unions (XOR) ");
		long bef, aft;
		String line = "";		
		int bogus  = 0;
		int N = data.length;
		bef = System.currentTimeMillis();
		RoaringBitmap[] bitmap = new RoaringBitmap[N];
		int size = 0;
		for (int r = 0; r < repeat; ++r) {
			size = 0;
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new RoaringBitmap();
				for (int x = 0; x < data[k].length; ++x) {
					bitmap[k].set(data[k][x]);
				}
				size += bitmap[k].getSizeInBytes();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);
		for(RoaringBitmap rb: bitmap) rb.validate();
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = bitmap[k].getIntegers();
				bogus += array.length;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		RoaringBitmap[] bitmap2 = new RoaringBitmap[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new RoaringBitmap();
			for (int x = 0; x < data2[k].length; ++x)
				bitmap2[k].set(data2[k][x]);						
		}
		for(RoaringBitmap rb: bitmap2) rb.validate();		
		// logical or + retrieval
		{
			RoaringBitmap bitmapor1 =  bitmap[0].clone();
			 bitmapor1.validate();
			RoaringBitmap bitmapor2 =  bitmap2[0].clone();
			bitmapor2.validate();
		for (int k = 1; k < N; ++k) {				
			bitmapor1 = RoaringBitmap.or(bitmapor1,bitmap[k]);
			bitmapor1.validate();
			bitmapor2 = RoaringBitmap.or(bitmapor2,bitmap2[k]);
			bitmapor2.validate();
		}

		bitmapor1 = RoaringBitmap.or(bitmapor1,bitmapor2);
		int[] array = bitmapor1.getIntegers();
		bogus += array.length;
		}
			

		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			RoaringBitmap bitmapor1 =  bitmap[0].clone();
			RoaringBitmap bitmapor2 =  bitmap2[0].clone();
		for (int k = 1; k < N; ++k) {				
			bitmapor1 = RoaringBitmap.or(bitmapor1,bitmap[k]);
			bitmapor2 = RoaringBitmap.or(bitmapor2,bitmap2[k]);
		}

		bitmapor1 = RoaringBitmap.or(bitmapor1,bitmapor2);
		int[] array = bitmapor1.getIntegers();
		bogus += array.length;
		}
			
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		{
			RoaringBitmap bitmapand1 = bitmap[0].clone();
			bitmapand1.validate();
			RoaringBitmap bitmapand2 = bitmap2[0].clone();
			bitmapand2.validate();
			for (int k = 1; k < N; ++k) {				
				bitmapand1 = RoaringBitmap.and(bitmapand1, bitmap[k]);
				bitmapand1.validate();
				bitmapand2 = RoaringBitmap.and(bitmapand2, bitmap2[k]);
				bitmapand2.validate();
			}
			bitmapand1 = RoaringBitmap.and(bitmapand1, bitmapand2);
			bitmapand1.validate();
			int[] array = bitmapand1.getIntegers();
			bogus += array.length;
	
		}
		
		// logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			RoaringBitmap bitmapand1 = bitmap[0].clone();
			RoaringBitmap bitmapand2 = bitmap2[0].clone();
			for (int k = 1; k < N; ++k) {				
				bitmapand1 = RoaringBitmap.and(bitmapand1, bitmap[k]);
				bitmapand2 = RoaringBitmap.and(bitmapand2, bitmap2[k]);
			}
			bitmapand1 = RoaringBitmap.and(bitmapand1, bitmapand2);
			int[] array = bitmapand1.getIntegers();
			bogus += array.length;
	
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		// logical xor + retrieval
		{
			RoaringBitmap bitmapxor1 = bitmap[0].clone();
			RoaringBitmap bitmapxor2 = bitmap2[0].clone();
			for (int k = 1; k < N; ++k) {				
				bitmapxor1 =RoaringBitmap.xor( bitmapxor1,bitmap[k]);
				bitmapxor1.validate();
				bitmapxor2 = RoaringBitmap.xor( bitmapxor2,bitmap2[k]);
				bitmapxor2.validate();
			}
			bitmapxor1 = RoaringBitmap.xor( bitmapxor1,bitmapxor2);
			bitmapxor1.validate();
			int[] array = bitmapxor1.getIntegers();		
			bogus += array.length;
		}

		bef = System.currentTimeMillis();
				for (int r = 0; r < repeat; ++r) {
					RoaringBitmap bitmapxor1 = bitmap[0].clone();
					RoaringBitmap bitmapxor2 = bitmap2[0].clone();
					for (int k = 1; k < N; ++k) {				
						bitmapxor1 =RoaringBitmap.xor( bitmapxor1,bitmap[k]);
						bitmapxor2 = RoaringBitmap.xor( bitmapxor2,bitmap2[k]);
					}
					bitmapxor1 = RoaringBitmap.xor( bitmapxor1,bitmapxor2);
					int[] array = bitmapxor1.getIntegers();		
					bogus += array.length;
				}
				aft = System.currentTimeMillis();
				line += "\t" + df.format((aft - bef) / 1000.0);

		System.out.println(line);	
		System.out.println("# ignore this "+bogus);
	}
	
	public static void testBitSet(int[][] data, int[][] data2, int repeat, DecimalFormat df) {
		System.out.println("# BitSet");
		System.out.println("# size, construction time, time to recover set bits, " +
						"time to compute unions (OR), intersections (AND) " +
						"and exclusive unions (XOR) ");
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
		
		// logical xor + retrieval
				bef = System.currentTimeMillis();
				for (int r = 0; r < repeat; ++r) {
					BitSet bitmapxor1 = (BitSet) bitmap[0].clone();
					BitSet bitmapxor2 = (BitSet) bitmap2[0].clone();
					for (int k = 1; k < N; ++k) {				
						bitmapxor1.xor(bitmap[k]);
						bitmapxor2.xor(bitmap2[k]);
					}
					bitmapxor1.xor(bitmapxor2);
					int[] array = new int[bitmapxor1.cardinality()];
					int pos = 0;
					for (int i = bitmapxor1.nextSetBit(0); i >= 0; i = bitmapxor1
								.nextSetBit(i + 1)) {
						array[pos++] = i;
					}					   
				}
				aft = System.currentTimeMillis();
				line += "\t" + df.format((aft - bef) / 1000.0);

		System.out.println(line);		
	}



	
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
