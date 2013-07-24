package me.lemire.roaringbitmap.experiments;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.devbrat.util.WAHBitSet;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;

import me.lemire.roaringbitmap.RoaringBitmap;

public class StarSchemaBenchmark {	
		
	/**
	 * Please use ArrayList instead of Vector. For speed.
	 */
	static TreeMap<String, ArrayList<Integer>> BitmapIdx = new TreeMap<String, ArrayList<Integer>>();
	/**
	 * @param args
	 */ 
	public static void main(String[] args) {
		String defaultpath = "G:/Downloads/smallssb.csv";
		if(args.length>0)
			defaultpath = args[0];
		StarSchemaBenchmark.BuildingSSBbitmaps(defaultpath);
		
		DecimalFormat df = new DecimalFormat("0.###");				
		StarSchemaBenchmark.testRoaringBitmap(10, df);
		StarSchemaBenchmark.testBitSet(10, df);	
		StarSchemaBenchmark.testConciseSet(10, df);
		//ssb.testWAH32(10, df);
		StarSchemaBenchmark.testEWAH64(10, df);
		StarSchemaBenchmark.testEWAH32(10, df);
		
	}
		
	public static void BuildingSSBbitmaps(String path) {
		try
		{
			
		  /**
		   * TODO: rely on a standard library like jcvs instead,
		   * as this approach might be naive and would probably give you headaches
		   * on actual CSV files.
		   * 
		   */
		   BufferedReader fichier_source = new BufferedReader(new FileReader(path));
		   String chaine;
		   int row = 1;
		 
		   try {
			while((chaine = fichier_source.readLine())!= null)
			   {			      
			         String[] tabChaine = chaine.split(",");
			         
			         for(int i=0; i<tabChaine.length; i++)
			        	 //System.out.print(tabChaine[i]+" ");System.out.println();
			        	 if(BitmapIdx.containsKey("C"+i+"_"+tabChaine[i]))			        		 
			        		 BitmapIdx.get("C"+i+"_"+tabChaine[i]).add(row);
			        	 
			         else {			        	 
			        	 ArrayList<Integer> bitmap = new ArrayList<Integer>();
			        	 bitmap.add(row);
			        	 BitmapIdx.put("C"+i+"_"+tabChaine[i], bitmap);			        	 
			         }
			         row++;
			   }		   
			fichier_source.close();
		   } catch (IOException e1) {e1.printStackTrace();}            
		}
		catch (FileNotFoundException e)		{
		   System.out.println("Sorry, file not found !");
		}	
	}
	
	public static void testRoaringBitmap(int repeat, DecimalFormat df) {
		System.out.println("# RoaringBitmap on Star Schema Benchmark");
		System.out.println("# size, construction time, time to recover set bits, " +
						"time to compute unions (OR), intersections (AND) " +
						"and exclusive unions (XOR) ");
		//Calculate the construction time
		long bef, aft;		
		String line = "";
		/**
		 * Bogus is essential otherwise the compiler could
		 * optimize away the computation.
		 */
		int bogus  = 0;
		int N = BitmapIdx.size()/2;
		bef = System.currentTimeMillis();
		RoaringBitmap[] bitmap = new RoaringBitmap[N];
		int size = 0;
		
		//for (int r = 0; r < repeat; ++r) {
			size = 0;
			final Iterator<Entry<String, ArrayList<Integer>>> I = BitmapIdx.entrySet().iterator();
			Entry<String, ArrayList<Integer>> s; 		
			
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new RoaringBitmap();
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				bitmap[k].set(s.getValue().get(j));
				
				size += bitmap[k].getSizeInBytes();
			}
		//}
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
		
		//Creating and filling the second roaringBitmap index
		//final Iterator<Entry<String, Vector<Integer>>> I = this.BitmapIdx.entrySet().iterator();
		//Entry<String, Vector<Integer>> s; 				
		RoaringBitmap[] bitmap2 = new RoaringBitmap[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new RoaringBitmap();
			while(I.hasNext()) {
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				bitmap2[k].set(s.getValue().get(j));
			}
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
	
	public static void testBitSet(int repeat, DecimalFormat df) {
		System.out.println("# BitSet");
		System.out.println("# size, construction time, time to recover set bits, " +
						"time to compute unions (OR), intersections (AND) " +
						"and exclusive unions (XOR) ");
		long bef, aft;
		String line = "";		
		int N = BitmapIdx.size()/2;
		bef = System.currentTimeMillis();
		BitSet[] bitmap = new BitSet[N];
		int size;
		
		//for (int r = 0; r < repeat; ++r) {
		size = 0;
		final Iterator<Entry<String, ArrayList<Integer>>> I = BitmapIdx.entrySet().iterator();
		Entry<String, ArrayList<Integer>> s; 			
						
		for (int k = 0; k < N; ++k) {
			bitmap[k] = new BitSet();
			s=I.next();
			for(int j=0; j<s.getValue().size(); j++)
			bitmap[k].set(s.getValue().get(j));
				
			size += bitmap[k].size() / 8;
		}
		//}
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
		
		//Creating and filling the 2nd bitset index
		BitSet[] bitmap2 = new BitSet[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new BitSet();
			while(I.hasNext()) {
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				bitmap2[k].set(s.getValue().get(j));
			}
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
	
	public static void testConciseSet(int repeat, DecimalFormat df) {
		System.out
				.println("# ConciseSet 32 bit using the extendedset_2.2 library");
		System.out
				.println("# size, construction time, time to recover set bits, time to compute unions  and intersections ");
		long bef, aft;
		String line = "";
		int bogus = 0;
		int N = BitmapIdx.size()/2;
		bef = System.currentTimeMillis();
		ConciseSet[] bitmap = new ConciseSet[N];
		int size = 0;
		//for (int r = 0; r < repeat; ++r) {
			size = 0;
			final Iterator<Entry<String, ArrayList<Integer>>> I = BitmapIdx.entrySet().iterator();
			Entry<String, ArrayList<Integer>> s; 			
				
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new ConciseSet();
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				bitmap[k].add(s.getValue().get(j));
				
				size += (int) (bitmap[k].size() * bitmap[k]
						.collectionCompressionRatio()) * 4;
			}
		//}
		aft = System.currentTimeMillis();
		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = bitmap[k].toArray();	
				bogus += array.length;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		//Creating and filling the 2nd Concise index
		ConciseSet[] bitmap2 = new ConciseSet[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new ConciseSet();
			while(I.hasNext()) {
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				bitmap2[k].add(s.getValue().get(j));
			}						
		}
				
		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			ConciseSet bitmapor1 = bitmap[0].clone();
			ConciseSet bitmapor2 = bitmap2[0].clone();
			for (int k = 1; k < N; ++k) {				
				bitmapor1.union(bitmap[k]);
				bitmapor2.union(bitmap2[k]);
			}
			bitmapor1.union(bitmapor2);
			int[] array = bitmapor1.toArray();	
			bogus += array.length;
			}	
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
				
		// logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			ConciseSet bitmapand1 = bitmap[0].clone();
			ConciseSet bitmapand2 = bitmap2[0].clone();
			for (int k = 1; k < N; ++k) {				
				bitmapand1.union(bitmap[k]);
				bitmapand2.union(bitmap2[k]);
			}
			bitmapand1.intersection(bitmapand2);
			int[] array = bitmapand1.toArray();		
			bogus += array.length;
		}	
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		System.out.println(line);
		System.out.println("# ignore this "+bogus);

	}
	
	public static void testWAH32(int repeat, DecimalFormat df) {
		System.out.println("# WAH 32 bit using the compressedbitset library");
		System.out.println("# size, construction time, time to recover set bits, " +
						"time to compute unions (OR), intersections (AND)");
		long bef, aft;
		String line = "";		
		int N = BitmapIdx.size()/2;
		bef = System.currentTimeMillis();
		WAHBitSet[] bitmap = new WAHBitSet[N];
		int size = 0;
		//for (int r = 0; r < repeat; ++r) {
			size = 0;
			final Iterator<Entry<String, ArrayList<Integer>>> I = BitmapIdx.entrySet().iterator();
			Entry<String, ArrayList<Integer>> s; 			
			
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new WAHBitSet();
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				bitmap[k].set(s.getValue().get(j));
				
				size += bitmap[k].memSize()*4;
			}
		//}
		aft = System.currentTimeMillis();
		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = new int[bitmap[k].cardinality()];
				int c = 0;
				for (@SuppressWarnings("unchecked")
				Iterator<Integer> i = bitmap[k].iterator(); i.hasNext(); 
						array[c++] = i.next().intValue()) {}
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		//Creating and filling the 2nd WAH index
		WAHBitSet[] bitmap2 = new WAHBitSet[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new WAHBitSet();
			while(I.hasNext()) {
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				bitmap2[k].set(s.getValue().get(j));
			}						
		}
		
		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
		WAHBitSet bitmapor1 = new WAHBitSet();		
		WAHBitSet bitmapor2 = new WAHBitSet();
		for (int k = 0; k < N; ++k) {				
			bitmapor1.or(bitmap[k]);
			bitmapor2.or(bitmap2[k]);
		}
		bitmapor1.or(bitmapor2);
		int[] array = new int[bitmapor1.cardinality()];
		int c = 0;
		for (@SuppressWarnings("unchecked")
		Iterator<Integer> i = bitmapor1.iterator(); i.hasNext(); 
				array[c++] = i.next().intValue()) {  }				   
		}	
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
				
		// logical and + retrieval
				bef = System.currentTimeMillis();
				for (int r = 0; r < repeat; ++r) {
				WAHBitSet bitmapand1 = bitmap[0];
				WAHBitSet bitmapand2 = bitmap2[0];
				for (int k = 1; k < N; ++k) {				
					bitmapand1.and(bitmap[k]);
					bitmapand2.and(bitmap2[k]);
				}
				bitmapand1.and(bitmapand2);
				int[] array = new int[bitmapand1.cardinality()];
				int c = 0;
				for (@SuppressWarnings("unchecked")
				Iterator<Integer> i = bitmapand1.iterator(); i.hasNext(); 
						array[c++] = i.next().intValue()) {  }						   
				}	
				aft = System.currentTimeMillis();
				line += "\t" + df.format((aft - bef) / 1000.0);			

		System.out.println(line);		
	}
	
	public static void testEWAH64(int repeat, DecimalFormat df) {
		System.out.println("# EWAH 64bits using the javaewah library");
		System.out
				.println("# size, construction time, time to recover set bits, time to compute unions  and intersections ");
		long bef, aft;
		String line = "";		
		int bogus = 0;
		int N = BitmapIdx.size()/2;
		bef = System.currentTimeMillis();
		EWAHCompressedBitmap[] ewah = new EWAHCompressedBitmap[N];
		int size = 0;
		//for (int r = 0; r < repeat; ++r) {
			final Iterator<Entry<String, ArrayList<Integer>>> I = BitmapIdx.entrySet().iterator();
			Entry<String, ArrayList<Integer>> s; 			
			
			size = 0;
			for (int k = 0; k < N; ++k) {
				ewah[k] = new EWAHCompressedBitmap();
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				ewah[k].set(s.getValue().get(j));
				size += ewah[k].sizeInBytes();
			}
		//}
		aft = System.currentTimeMillis();
		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = ewah[k].toArray();
				bogus += array.length;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		//Creating and filling the 2nd ewah64 index
		EWAHCompressedBitmap[] ewah2 = new EWAHCompressedBitmap[N];
		for (int k = 0; k < N; ++k) {
			ewah2[k] = new EWAHCompressedBitmap();
			while(I.hasNext()) {
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				ewah2[k].set(s.getValue().get(j));
			}						
		}
		
		// fast logical or + retrieval
		bef = System.currentTimeMillis();
		try {
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap ewahor1 = ewah[0].clone();
			EWAHCompressedBitmap ewahor2 = ewah2[0].clone();
			for (int k = 1; k < N; ++k) {				
				ewahor1.or(ewah[k]);
				ewahor2.or(ewah2[k]);
			}
		ewahor1.or(ewahor2);
		int[] array = ewahor1.toArray();
		bogus += array.length;
		}
		}catch(CloneNotSupportedException e){System.out.println("bug : clone ewah64 or");};		
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);			

		// fast logical and + retrieval
		bef = System.currentTimeMillis();
		try {
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap ewahand1 = ewah[0].clone();
			EWAHCompressedBitmap ewahand2 = ewah2[0].clone();
			for (int k = 1; k < N; ++k) {				
				ewahand1.and(ewah[k]);
				ewahand2.and(ewah2[k]);
			}
		ewahand1.and(ewahand2);
		int[] array = ewahand1.toArray();
		bogus += array.length;
		}
		}catch(CloneNotSupportedException e){System.out.println("bug : clone ewah64 and");};		
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		// fast logical xor + retrieval
		bef = System.currentTimeMillis();
		try {
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap ewahxor1 = ewah[0].clone();
			EWAHCompressedBitmap ewahxor2 = ewah2[0].clone();
			for (int k = 1; k < N; ++k) {				
				ewahxor1.xor(ewah[k]);
				ewahxor2.xor(ewah2[k]);
			}
		ewahxor1.xor(ewahxor2);
		int[] array = ewahxor1.toArray();
		bogus += array.length;
		}
		}catch(CloneNotSupportedException e){System.out.println("bug : clone ewah64 xor");};		
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);		
		
		System.out.println(line);
		System.out.println("# ignore this "+bogus);
	}
	
	public static void testEWAH32(int repeat, DecimalFormat df) {
		System.out.println("# EWAH 32-bit using the javaewah library");
		System.out
				.println("# size, construction time, time to recover set bits, time to compute unions  and intersections ");
		long bef, aft;
		String line = "";
		long bogus = 0;
		int N = BitmapIdx.size()/2;
		bef = System.currentTimeMillis();
		EWAHCompressedBitmap32[] ewah = new EWAHCompressedBitmap32[N];
		int size;
		//for (int r = 0; r < repeat; ++r) {
		final Iterator<Entry<String, ArrayList<Integer>>> I = BitmapIdx.entrySet().iterator();
		Entry<String, ArrayList<Integer>> s; 			
		
		size = 0;
		for (int k = 0; k < N; ++k) {
			ewah[k] = new EWAHCompressedBitmap32();
			s=I.next();
			for(int j=0; j<s.getValue().size(); j++)
			ewah[k].set(s.getValue().get(j));
			
		size += ewah[k].sizeInBytes();
		}
		//}
		aft = System.currentTimeMillis();
		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = ewah[k].toArray();
				bogus += array.length;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		//Creating and filling the 2nd ewah32 index
		EWAHCompressedBitmap32[] ewah2 = new EWAHCompressedBitmap32[N];
		for (int k = 0; k < N; ++k) {
			ewah2[k] = new EWAHCompressedBitmap32();
			while(I.hasNext()) {
				s=I.next();
				for(int j=0; j<s.getValue().size(); j++)
				ewah2[k].set(s.getValue().get(j));
			}						
		}
		
		// fast logical or + retrieval
		bef = System.currentTimeMillis();
		try {
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap32 ewahor1 = ewah[0].clone();
			EWAHCompressedBitmap32 ewahor2 = ewah2[0].clone();
			for (int k = 1; k < N; ++k) {				
				ewahor1.or(ewah[k]);
				ewahor2.or(ewah2[k]);
			}
		ewahor1.or(ewahor2);
		int[] array = ewahor1.toArray();
		bogus += array.length;
		}
		}catch(CloneNotSupportedException e){System.out.println("bug : clone ewah32 or");};		
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		// fast logical and + retrieval
		bef = System.currentTimeMillis();
		try {
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap32 ewahand1 = ewah[0].clone();
			EWAHCompressedBitmap32 ewahand2 = ewah2[0].clone();
			for (int k = 1; k < N; ++k) {				
				ewahand1.and(ewah[k]);
				ewahand2.and(ewah2[k]);
			}
		ewahand1.or(ewahand2);
		int[] array = ewahand1.toArray();
		bogus += array.length;
		}
		}catch(CloneNotSupportedException e){System.out.println("bug : clone ewah32 and");};		
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		// fast logical xor + retrieval
		bef = System.currentTimeMillis();
		try {
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap32 ewahxor1 = ewah[0].clone();
			EWAHCompressedBitmap32 ewahxor2 = ewah2[0].clone();
			for (int k = 1; k < N; ++k) {				
				ewahxor1.xor(ewah[k]);
				ewahxor2.xor(ewah2[k]);
			}
		ewahxor1.xor(ewahxor2);
		int[] array = ewahxor1.toArray();
		bogus += array.length;
		}
		}catch(CloneNotSupportedException e){System.out.println("bug : clone ewah32 xor");};		
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);		

		System.out.println(line);
		System.out.println("# ignore this "+bogus);

	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		final Iterator<Entry<String, ArrayList<Integer>>> I = StarSchemaBenchmark.BitmapIdx.entrySet().iterator();
		Entry<String, ArrayList<Integer>> s; 
		int counter = 0;
		
		while(I.hasNext()) {
			s = I.next();
			counter++;
			sb.append(s.getKey()+" :: "+s.getValue().toString()+"\n");
		}		
		System.out.println("cardinality = "+BitmapIdx.size()+" "+counter);
		return sb.toString();
	}
}
