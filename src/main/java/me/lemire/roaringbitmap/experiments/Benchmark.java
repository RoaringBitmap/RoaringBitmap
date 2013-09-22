package me.lemire.roaringbitmap.experiments;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.Iterator;
import java.util.Vector;

import me.lemire.roaringbitmap.*;
import me.lemire.roaringbitmap.experiments.LineCharts.LineChartDemo1;
import me.lemire.roaringbitmap.experiments.LineCharts.LineChartPoint;

import org.devbrat.util.WAHBitSet;
import sparsebitmap.SparseBitmap;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;

public class Benchmark {
	
	static ArrayList<Vector<LineChartPoint>> SizeGraphCoordinates;
	static ArrayList<Vector<LineChartPoint>> OrGraphCoordinates;
	static ArrayList<Vector<LineChartPoint>> XorGraphCoordinates;
	static ArrayList<Vector<LineChartPoint>> AndGraphCoordinates;
	static int nbTechnique = 6;
	static int FastAgregate = 1;
	static int ClassicAgregate = 0;
	private static UniformDataGenerator uniform;
	private static ZipfianDistribution zpf;
	private static ClusteredDataGenerator cdg;
	private static int distClustered = 2;
	private static int distUniform = 1;
	private static int distZipf = 0;
	private static int classic = 0;
	private static int Fast = 1;
	private static int inPlace = 2;
	private static int FastinPlace = 3;
	private static BufferedWriter bw = null;
	private static int max = 10000000;
	private static int nbBitmaps = 10;
	private static String CPU = "IntelCorei3_M330";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//test(10, 18, 10);
                if (args.length > 0) {                    
                	//Tests(nbBitmaps, 10, args[0], distUniform);
                	Tests(nbBitmaps, 10, args[0], distZipf);
                	//Tests(nbBitmaps, 10, args[0], distClustered);
                }
                else {
                        Tests(nbBitmaps, 10, null, distUniform);// no plots needed
                        Tests(nbBitmaps, 10, null, distZipf);
                        Tests(nbBitmaps, 10, null, distClustered);
                	}
	}
	
	private static RoaringBitmap fastOR(RoaringBitmap[] tabRB) {
		return FastAggregation.inplace_or(tabRB);
	}
	
	private static RoaringBitmap fastXOR(RoaringBitmap[] tabRB) {
		return FastAggregation.inplace_xor(tabRB);
	}
	
	private static RoaringBitmap fastAND(RoaringBitmap[] tabRB) {
		return FastAggregation.inplace_and(tabRB);
	}
	
	/*private static RoaringBitmap simpleOR(RoaringBitmap[] tabRB) {
		RoaringBitmap rb = tabRB[0];
		for(int i=0; i<tabRB.length; i++) 
			rb = RoaringBitmap.or(rb, tabRB[1]);
		return rb;
	}*/
	
	/**
	 * Generating N sets of nbInts integers using Zipfian distribution.
	 * @param N number of generated sets of integers
	 * @param repeat number of repetitions
	 */
	public static void Tests(int N, int repeat, String path, int distribution) {
				
		System.out.println("WARNING: Though I am called ZipfianTests, " +
				"I am using a uniform data generator. Maybe a better design would use the same method " +
				"and have the data type as a parameter.");
		zpf = new ZipfianDistribution();	
		uniform = new UniformDataGenerator();
		cdg = new ClusteredDataGenerator();	
		
		String distdir = null;

		//Creating the distribution folder
		switch(distribution) {
		case 0 : distdir = path+File.separator+"Benchmarks_n"+CPU+File.separator+"Zipf"; break;
		case 1 : distdir = path+File.separator+"Benchmarks_n"+CPU+File.separator+"Uniform";break;
		case 2 : distdir = path+File.separator+"Benchmarks_n"+CPU+File.separator+"Clustered";break;
		default : System.out.println("Can you choose a distribution ?");
				  System.exit(0);
		}
		
		launchBenchmark(distribution, N, repeat, distdir, classic);
		//launchBenchmark(distribution, N, repeat, df, distdir, Fast);
		//launchBenchmark(distribution, N, repeat, df, distdir, inPlace);
		//launchBenchmark(distribution, N, repeat, df, distdir, FastinPlace);
	}
	
	public static void launchBenchmark(int distribution, int N, int repeat, 
			String distdir, int optimisation) {
		
		String Chartsdir = null, Benchmarkdir = null, optdir = null;	
		DecimalFormat df = new DecimalFormat("0.###");
		
		//Creating the kind of optimization folder
		switch(optimisation) {
		case 0 : optdir = distdir+File.separator+"RoaringBitmap_Classic"; break;
		case 1 : optdir = distdir+File.separator+"RoaringBitmap_FastAggregations";	break;
		case 2 : optdir = distdir+File.separator+"RoaringBitmap_inPlace"; break;
		case 3 : optdir = distdir+File.separator+"RoaringBitmap_FastAgg_inPlace"; break;
		default : System.out.println("Can you choose a distribution ?");
				  System.exit(0);
		}
		
		//Creating the charts folder		
		Chartsdir = optdir+File.separator+"Charts";
		
		//Creating the benchmark results folder
		Benchmarkdir = optdir+File.separator+"Benchmark";
		
		try {
			boolean success = (new File(Chartsdir).mkdirs());
			boolean success2 = (new File(Benchmarkdir).mkdirs());
			if(success & success2) System.out.println("folders created with success");
		}	catch(Exception e) {e.getMessage();}
		
		try {			 
			File file = new File(Benchmarkdir+"/Benchmark.txt");
			
			// if file doesn't exists, then create it
			if (!file.exists()) 
				file.createNewFile();
						
			Date date = new Date();
		    SimpleDateFormat dateFormatComp;
		 
		    dateFormatComp = new SimpleDateFormat("dd MMM yyyy hh:mm:ss a");
		    //System.out.println(dateFormatComp.format(date));
		    String[] op = {"Classic", "FastAggregations", "inPlace operations", "FastAggregations & inPlace operations"};
		    
		    System.out.println("# For each instance, we report the size, the construction time, ");
		    System.out.println("# the time required to recover the set bits,");
		    System.out
		    .println("# and the time required to compute logical ors (unions) between lots of bitmaps." +
		    		"\n\n" 
		    		+"# Number of bitmaps = "+N
		    		+"\n# Bitmaps cardinality = "+max
		    		+"\n# Optimisation = "+op[optimisation]
		    		+"\n# "+dateFormatComp.format(date)
		    		+"\n# CPU = "+System.getProperty("os.arch")
		    		+"\n# "+System.getProperty("os.name")
		    		+"\n# Java "+System.getProperty("java.version"));		
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		bw=null;
		bw = new BufferedWriter(fw);
		bw.write("\n# For each instance, we report the size, the construction time, \n"
				+"# the time required to recover the set bits,"
				+"# and the time required to compute logical ors (unions) between lots of bitmaps."
				+"# and the time required to compute logical ors (unions) between lots of bitmaps." +
				"\n\n" +
				 "# Number of bitmaps = "+N
				+"\n# Bitmaps cardinality = "+max 
				+"\n# Optimisation = "+op[optimisation]
				+"\n# "+dateFormatComp.format(date)
				+"\n# CPU = "+System.getenv("os.arch")
				+"\n# "+System.getProperty("os.name")
				+"\n# Java "+System.getProperty("java.version")
				);
	} catch (IOException e) {e.printStackTrace();}
		
		for(double k=0.0001; k<1.0; k*=10) {
			SizeGraphCoordinates = new ArrayList<Vector<LineChartPoint>>();
			OrGraphCoordinates = new ArrayList<Vector<LineChartPoint>>();
			AndGraphCoordinates = new ArrayList<Vector<LineChartPoint>>();
			XorGraphCoordinates = new ArrayList<Vector<LineChartPoint>>();	
			
			for(int i =0; i< nbTechnique; i++) {
				SizeGraphCoordinates.add(new Vector<LineChartPoint>());
				OrGraphCoordinates.add(new Vector<LineChartPoint>());
				AndGraphCoordinates.add(new Vector<LineChartPoint>());
				XorGraphCoordinates.add(new Vector<LineChartPoint>());
			}
			for( double density = k; density<k*10.0; density+=density/*=10.0*/ )
			{
				if(density>=0.7) 
					density=0.6;			
				int SetSize = (int) (max*density);
				int data[][] = new int[N][];
				int data2[][] = new int[N][];
				
				System.out.println("\n\ndensity = "+density);
				System.out.println("# generating random data...");
				try {
					bw.write("\n\n\ndensity = "+density+
							"\n# generating random data...");
				} catch (IOException e1) 
					{e1.printStackTrace();}
				
				for(int i =0; i< nbTechnique; i++) {
					SizeGraphCoordinates.get(i).add(new LineChartPoint(0.0, String.valueOf(density), null));
					OrGraphCoordinates.get(i).add(new LineChartPoint(0.0, String.valueOf(density), null));
					AndGraphCoordinates.get(i).add(new LineChartPoint(0.0, String.valueOf(density), null));
					XorGraphCoordinates.get(i).add(new LineChartPoint(0.0, String.valueOf(density), null));
				}
				
				// Generating the first set				
				for(int i=0; i<N; i++)
				{	
					switch (distribution) {
					case 0 : data[i] = zpf.GenartingInts(SetSize, max);
							data2[i] = zpf.GenartingInts(SetSize, max);
							break;
					case 1 : data[i] = uniform.generateUniform(SetSize, max);
							 data2[i] = uniform.generateUniform(SetSize, max);
							 break;
					case 2 : int[] inter = cdg.generateClustered(1 << (18 / 2), max);
							 data[i] = IntUtil.unite(inter, cdg.generateClustered(1 << 18, max));
							 data2[i] = IntUtil.unite(inter, cdg.generateClustered(1 << 18, max));
							 break;
					default : System.out.println("Launching tests aborted");
							  System.exit(0);
					}				
					
					Arrays.sort(data[i]);
					Arrays.sort(data2[i]);				
					
					/*System.out.println("\n\n data1");
					int bigger = 0, aver = 0, val1 = data[i][0];
					for(int j=1; j<data[i].length; j++){
						bigger = bigger < Math.abs(data[i][j]-val1) ? (data[i][j]-val1) : bigger;
						aver += Math.abs(data[i][j] - val1);
						val1=data[i][j];
						//System.out.println(data[i][j]+" ");
					}
					System.out.println(bigger+" "+" average = "+(aver/data[i].length));*/
					/*System.out.println("\n data2");				
					for(int j=0; j<data2[i].length; j++)
						System.out.println(data2[i][j]+" ");*/				
				}
				
				// Start experiments with Zipfian data distribution
				System.out.println("# generating random data... ok.");
				System.out.println("#  density = "+ density+" nb setBits = "+SetSize);
				try {
					bw.write("\n# generating random data... ok.\n"
							+"#  density = "+ density+" nb setBits = "+SetSize);
				} catch (IOException e) 
					{e.printStackTrace();}
				
				// Launching benchmarks				
				//testBitSet(data, data2, repeat, df);
				testRoaringBitmap(data, data2, repeat, df, optimisation);
				testWAH32(        data.clone(), data2.clone(), repeat, df, optimisation);
				testConciseSet(   data.clone(), data2.clone(), repeat, df, optimisation);
				testSparseBitmap( data.clone(), data2.clone(), repeat, df, optimisation);
				testEWAH64(       data.clone(), data2.clone(), repeat, df, optimisation);
				testEWAH32(       data.clone(), data2.clone(), repeat, df, optimisation);
				System.out.println();		
			}		
	                        if (Chartsdir != null) {
	                        		String p = Chartsdir+File.separator;	                        		
	                        		
	                                new LineChartDemo1(
	                                        "Line_Chart_Compression_size_" + k + "_" + (k * 10), "size (KB)",
	                                        SizeGraphCoordinates, p);
	                                new LineChartDemo1(
	                                        "Line_Chart_OR_times_" + k + "_" + (k * 10),"Time (sec)",
	                                        OrGraphCoordinates, p);
	                                new LineChartDemo1(
	                                        "Line_Chart_AND_times_" + k + "_" + (k * 10),"Time (sec)",
	                                        AndGraphCoordinates, p);
	                                new LineChartDemo1(
	                                        "Line_Chart_XOR_times_" + k + "_" + (k * 10),"Time (sec)",
	                                        XorGraphCoordinates, p);
	                        }	            
			}
		try {
    		bw.close();
    	} catch (IOException e) {e.printStackTrace();}
	}

	public static void testRoaringBitmap(int[][] data, int[][] data2,
			int repeat, DecimalFormat df, int optimisation) {
		System.out.println("# RoaringBitmap");
		System.out
				.println("# cardinality, size, construction time, time to recover set bits, "
						+ "time to compute unions (OR), intersections (AND) "
						+ "and exclusive unions (XOR) ");
		try {
			bw.write("\n"+"# RoaringBitmap\n"+"# cardinality, size, construction time, time to recover set bits, "
							+ "time to compute unions (OR), intersections (AND) "
							+ "and exclusive unions (XOR) ");
		} catch (IOException e1) {e1.printStackTrace();}
		
		// Calculating the construction time
		long bef, aft;
		String line = "";
		int bogus = 0;
		int N = data.length, size = 0;
		
		bef = System.currentTimeMillis();
		RoaringBitmap[] bitmap = new RoaringBitmap[N];		
		for (int r = 0; r < repeat; ++r) {
			size = 0;
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new RoaringBitmap();
				for (int x = 0; x < data[k].length; ++x) {
					bitmap[k].set(data[k][x]);
				}				
				//if(r==0) System.out.println(bitmap[k].toString());				
			}
		}
		aft = System.currentTimeMillis();
		
		for (RoaringBitmap rb : bitmap)
			rb.validate();
		
		// Building the second array of RoaringBitmaps
		RoaringBitmap[] bitmap2 = new RoaringBitmap[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new RoaringBitmap();
			for (int x = 0; x < data2[k].length; ++x)
				bitmap2[k].set(data2[k][x]);
		}
		for (RoaringBitmap rb : bitmap2)
			rb.validate();
		
		//System.out.println("Average nb of shorts per node in this bitmap = "+bitmap[bitmap.length-1].getAverageNbIntsPerNode());
		
		//Calculating the all RoaringBitmaps size 
		for(int k=0; k<N; k++) {
			size += bitmap[k].getSizeInBytes(); //first array (bitmap)
			size += bitmap2[k].getSizeInBytes(); //second array (bitmap2)
		}
		
		int cardinality = 0, BC = 0, nbIntAC = 0;
		int size2 = 0;
		//Size with verification
		for(int k=0; k<N; k++) {
			size2 += bitmap[k].getNbNodes()*2;
			size2 += bitmap2[k].getNbNodes()*2; 
			int[] nbInts = bitmap[k].getIntsPerNode();
			//calculating the size of lowBits in nodes of all RoaringBitmaps in the array bitmap
			for (int x : nbInts) {
				cardinality+=x;
				if(x>=ArrayContainer.DEFAULTMAXSIZE) 
					{size2+=8192; BC++;}//if a bitmap container
				if(x< ArrayContainer.DEFAULTMAXSIZE) 
					{size2+=x*2 + 4; nbIntAC+=x;} //if an array container
			}
			//calculating the size of lowBits in nodes of all RoaringBitmaps in the array bitmap2
			nbInts = bitmap2[k].getIntsPerNode();
			for (int x : nbInts) {
				cardinality+=x;
				if(x>=ArrayContainer.DEFAULTMAXSIZE) 
					{size2+=8192; BC++;}//if a bitmap container
				if(x< ArrayContainer.DEFAULTMAXSIZE) 
					{size2+=x*2 + 4; nbIntAC+=x;}//if an array container
			}
		}		
		
		line += "\t"+cardinality+"\t" + size;
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		SizeGraphCoordinates.get(0).lastElement().setGname("Roaring Bitmap");
		SizeGraphCoordinates.get(0).lastElement().setY(size/1024);
		
		for (RoaringBitmap rb : bitmap)
			rb.validate();
		
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = bitmap[k].getIntegers();
				bogus += array.length;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);					

		// logical or + retrieval
		{
		RoaringBitmap bitmapor1 = null, bitmapor2;
		switch(optimisation)
		{			
		case 0 : bitmapor1 = bitmap[0];
				 bitmapor2 = bitmap2[0];	
				for (int k = 1; k < N; ++k) {
					bitmapor1 = RoaringBitmap.or(bitmapor1, bitmap[k]);
					bitmapor1.validate();
					bitmapor2 = RoaringBitmap.or(bitmapor2, bitmap2[k]);
					bitmapor2.validate();
				}
				bitmapor1 = RoaringBitmap.or(bitmapor1, bitmapor2);
				bitmapor1.validate();
				break;
		case 1 : bitmapor1 = null;
				 bitmapor2 = null;
				 bitmapor1 = FastAggregation.or(bitmap);
				 bitmapor1.validate();
				 bitmapor2 = FastAggregation.or(bitmap2);
				 bitmapor2.validate();
				 bitmapor1 = RoaringBitmap.or(bitmapor1, bitmapor2);
				 bitmapor1.validate();
				 break;
		case 2 : bitmapor1 = bitmap[0].clone();
				 bitmapor2 = bitmap2[0].clone();
				 for (int k = 1; k < N; ++k) {
					 bitmapor1.inPlaceOR(bitmap[k]);
					 bitmapor1.validate();
					 bitmapor2.inPlaceOR(bitmap2[k]);
					 bitmapor2.validate();
				 }
				 bitmapor1.inPlaceOR(bitmapor2);
				 bitmapor1.validate();
				 break;
		case 3 : bitmapor1 = null;
		 		bitmapor2 = null;
		 		bitmapor1 = FastAggregation.inplace_or(bitmap);
		 		bitmapor1.validate();
		 		bitmapor2 = FastAggregation.inplace_or(bitmap2);
		 		bitmapor2.validate();
		 		bitmapor1.inPlaceOR(bitmapor2);
		 		bitmapor1.validate();
		 		break;
			}
			int[] array = bitmapor1.getIntegers();
			bogus += array.length;
		}

		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			RoaringBitmap bitmapor1 = null, bitmapor2;
			switch(optimisation)
			{			
			case 0 : bitmapor1 = bitmap[0];
					 bitmapor2 = bitmap2[0];	
					for (int k = 1; k < N; ++k) {
						bitmapor1 = RoaringBitmap.or(bitmapor1, bitmap[k]);
						bitmapor2 = RoaringBitmap.or(bitmapor2, bitmap2[k]);
					}
					bitmapor1 = RoaringBitmap.or(bitmapor1, bitmapor2);
					break;
			case 1 : bitmapor1 = null;
					 bitmapor2 = null;
					 bitmapor1 = FastAggregation.or(bitmap);
					 bitmapor2 = FastAggregation.or(bitmap2);
					 bitmapor1 = RoaringBitmap.or(bitmapor1, bitmapor2);
					 break;
			case 2 : bitmapor1 = bitmap[0].clone();
					 bitmapor2 = bitmap2[0].clone();
					 for (int k = 1; k < N; ++k) {
						 bitmapor1.inPlaceOR(bitmap[k]);
						 bitmapor2.inPlaceOR(bitmap2[k]);
					 }
					 bitmapor1.inPlaceOR(bitmapor2);
					 break;
			case 3 : bitmapor1 = null;
			 		bitmapor2 = null;
			 		bitmapor1 = FastAggregation.inplace_or(bitmap);
			 		bitmapor2 = FastAggregation.inplace_or(bitmap2);
			 		bitmapor1.inPlaceOR(bitmapor2);
			 		break;
				}
			int[] array = bitmapor1.getIntegers();
			bogus += array.length;
		}

		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		OrGraphCoordinates.get(0).lastElement().setGname("Roaring Bitmap");
		OrGraphCoordinates.get(0).lastElement().setY((aft - bef) / 1000.0);
		
		// logical and + retrieval
		{
			RoaringBitmap bitmapand1 = null, bitmapand2;
			switch(optimisation)
			{			
			case 0 : bitmapand1 = bitmap[0];
					 bitmapand2 = bitmap2[0];	
					for (int k = 1; k < N; ++k) {
						bitmapand1 = RoaringBitmap.or(bitmapand1, bitmap[k]);
						bitmapand1.validate();
						bitmapand2 = RoaringBitmap.or(bitmapand2, bitmap2[k]);
						bitmapand2.validate();
					}
					bitmapand1 = RoaringBitmap.or(bitmapand1, bitmapand2);
					bitmapand1.validate();
					break;
			case 1 : bitmapand1 = null;
					 bitmapand2 = null;
					 bitmapand1 = FastAggregation.and(bitmap);
					 bitmapand1.validate();
					 bitmapand2 = FastAggregation.and(bitmap2);
					 bitmapand2.validate();
					 bitmapand1 = RoaringBitmap.and(bitmapand1, bitmapand2);
					 bitmapand1.validate();
					 break;
			case 2 : bitmapand1 = bitmap[0].clone();
					 bitmapand2 = bitmap2[0].clone();
					 for (int k = 1; k < N; ++k) {
						 bitmapand1.inPlaceOR(bitmap[k]);
						 bitmapand1.validate();
						 bitmapand2.inPlaceOR(bitmap2[k]);
						 bitmapand2.validate();
					 }
					 bitmapand1.inPlaceOR(bitmapand2);
					 bitmapand1.validate();
					 break;
			case 3 : bitmapand1 = null;
			 		bitmapand2 = null;
			 		bitmapand1 = FastAggregation.inplace_or(bitmap);
			 		bitmapand1.validate();
			 		bitmapand2 = FastAggregation.inplace_or(bitmap2);
			 		bitmapand2.validate();
			 		bitmapand1.inPlaceOR(bitmapand2);
			 		bitmapand1.validate();
			 		break;
				}
				int[] array = bitmapand1.getIntegers();
				bogus += array.length;
			}

		
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			RoaringBitmap bitmapand1 = null, bitmapand2;
			switch(optimisation)
			{			
			case 0 : bitmapand1 = bitmap[0];
					 bitmapand2 = bitmap2[0];	
					for (int k = 1; k < N; ++k) {
						bitmapand1 = RoaringBitmap.and(bitmapand1, bitmap[k]);
						bitmapand2 = RoaringBitmap.and(bitmapand2, bitmap2[k]);
					}
					bitmapand1 = RoaringBitmap.and(bitmapand1, bitmapand2);
					break;
			case 1 : bitmapand1 = null;
					 bitmapand2 = null;
					 bitmapand1 = FastAggregation.and(bitmap);
					 bitmapand2 = FastAggregation.and(bitmap2);
					 bitmapand1 = RoaringBitmap.and(bitmapand1, bitmapand2);
					 break;
			case 2 : bitmapand1 = bitmap[0].clone();
					 bitmapand2 = bitmap2[0].clone();
					 for (int k = 1; k < N; ++k) {
						 bitmapand1.inPlaceAND(bitmap[k]);
						 bitmapand2.inPlaceAND(bitmap2[k]);
					 }
					 bitmapand1.inPlaceAND(bitmapand2);
					 break;
			case 3 : bitmapand1 = null;
			 		bitmapand2 = null;
			 		bitmapand1 = FastAggregation.inplace_and(bitmap);
			 		bitmapand2 = FastAggregation.inplace_and(bitmap2);
			 		bitmapand1.inPlaceAND(bitmapand2);
			 		break;
				}
				int[] array = bitmapand1.getIntegers();
				bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		AndGraphCoordinates.get(0).lastElement().setGname("Roaring Bitmap");
		AndGraphCoordinates.get(0).lastElement().setY((aft - bef) / 1000.0);

		// logical xor + retrieval
		{
			RoaringBitmap bitmapxor1 = null, bitmapxor2;
			switch(optimisation)
			{			
			case 0 : //classic
					 bitmapxor1 = bitmap[0];
					 bitmapxor2 = bitmap2[0];	
					for (int k = 1; k < N; ++k) {
						bitmapxor1 = RoaringBitmap.xor(bitmapxor1, bitmap[k]);
						bitmapxor1.validate();
						bitmapxor2 = RoaringBitmap.xor(bitmapxor2, bitmap2[k]);
						bitmapxor2.validate();
					}
					bitmapxor1 = RoaringBitmap.xor(bitmapxor1, bitmapxor2);
					bitmapxor1.validate();
					break;
			case 1 : //Using FastAggregations
					 bitmapxor1 = null;
					 bitmapxor2 = null;
					 bitmapxor1 = FastAggregation.xor(bitmap);
					 bitmapxor1.validate();
					 bitmapxor2 = FastAggregation.xor(bitmap2);
					 bitmapxor2.validate();
					 bitmapxor1 = RoaringBitmap.xor(bitmapxor1, bitmapxor2);
					 bitmapxor1.validate();
					 break;
			case 2 : //Using inPlace operations
					 bitmapxor1 = bitmap[0].clone();
					 bitmapxor2 = bitmap2[0].clone();
					 for (int k = 1; k < N; ++k) {
						 bitmapxor1.inPlaceXOR(bitmap[k]);
						 bitmapxor1.validate();
						 bitmapxor2.inPlaceXOR(bitmap2[k]);
						 bitmapxor2.validate();
					 }
					 bitmapxor1.inPlaceXOR(bitmapxor2);
					 bitmapxor1.validate();
					 break;
			case 3 : //Using FastAggregations and inPlace operations 
					bitmapxor1 = null;
			 		bitmapxor2 = null;
			 		bitmapxor1 = FastAggregation.inplace_xor(bitmap);
			 		bitmapxor1.validate();
			 		bitmapxor2 = FastAggregation.inplace_xor(bitmap2);
			 		bitmapxor2.validate();
			 		bitmapxor1.inPlaceXOR(bitmapxor2);
			 		bitmapxor1.validate();
			 		break;
			}
			int[] array = bitmapxor1.getIntegers();
			bogus += array.length;
			}

		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			RoaringBitmap bitmapxor1 = null, bitmapxor2;
			switch(optimisation)
			{			
			case 0 : //classic
					 bitmapxor1 = bitmap[0];
					 bitmapxor2 = bitmap2[0];	
					for (int k = 1; k < N; ++k) {
						bitmapxor1 = RoaringBitmap.xor(bitmapxor1, bitmap[k]);
						bitmapxor2 = RoaringBitmap.xor(bitmapxor2, bitmap2[k]);
					}
					bitmapxor1 = RoaringBitmap.xor(bitmapxor1, bitmapxor2);
					break;
			case 1 : //Using FastAggregations
					 bitmapxor1 = null;
					 bitmapxor2 = null;
					 bitmapxor1 = FastAggregation.xor(bitmap);
					 bitmapxor2 = FastAggregation.xor(bitmap2);
					 bitmapxor1 = RoaringBitmap.xor(bitmapxor1, bitmapxor2);
					 break;
			case 2 : //Using inPlace operations
					 bitmapxor1 = bitmap[0].clone();
					 bitmapxor2 = bitmap2[0].clone();
					 for (int k = 1; k < N; ++k) {
						 bitmapxor1.inPlaceXOR(bitmap[k]);
						 bitmapxor2.inPlaceXOR(bitmap2[k]);
					 }
					 bitmapxor1.inPlaceXOR(bitmapxor2);
					 break;
			case 3 : //Using FastAggregations and inPlace operations 
					bitmapxor1 = null;
			 		bitmapxor2 = null;
			 		bitmapxor1 = FastAggregation.inplace_xor(bitmap);
			 		bitmapxor2 = FastAggregation.inplace_xor(bitmap2);
			 		bitmapxor1.inPlaceXOR(bitmapxor2);
			 		break;
			}
			int[] array = bitmapxor1.getIntegers();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		XorGraphCoordinates.get(0).lastElement().setGname("Roaring Bitmap");
		XorGraphCoordinates.get(0).lastElement().setY((aft - bef) / 1000.0);

		System.out.println(line+"\n# Real size = "+size2
				+" nbNodes = "+bitmap[1].getNbNodes()+" BC = "+BC+" nbIntsAC = "+nbIntAC
				+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
		System.out.println("# ignore this " + bogus);
		try {
				bw.write("\n"+line+"\n# Real size = "+size2+" nbNodes = "+bitmap[1].getNbNodes()
						+" BC = "+BC+" nbIntsAC = "+nbIntAC
						+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
				bw.write("\n# ignore this " + bogus+"\n\n");
			} catch (IOException e) {e.printStackTrace();}
	}

	public static void testBitSet(int[][] data, int[][] data2, int repeat,
		DecimalFormat df, int optimisation) {
		System.out.println("# BitSet");
		System.out.println("# size, construction time, time to recover set bits, "
						+ "time to compute unions (OR), intersections (AND) "
						+ "and exclusive unions (XOR) ");
		try {
			bw.write("\n"+"# BitSet\n"+"# size, construction time, time to recover set bits, "
							+ "time to compute unions (OR), intersections (AND) "
							+ "and exclusive unions (XOR) ");
		} catch (IOException e1) {e1.printStackTrace();}
		long bef, aft;
		String line = "";
		int N = data.length, size = 0;
		
		bef = System.currentTimeMillis();
		BitSet[] bitmap = new BitSet[N];		
		for (int r = 0; r < repeat; ++r) {
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new BitSet();
				for (int x = 0; x < data[k].length; ++x) {
					bitmap[k].set(data[k][x]);
				}				
			}
		}
		aft = System.currentTimeMillis();
		

		// Creating and filling the 2nd bitset index
		BitSet[] bitmap2 = new BitSet[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new BitSet();
			for (int x = 0; x < data2[k].length; ++x)
				bitmap2[k].set(data2[k][x]);
		}
		
		for(int k=0; k<N; k++) {
			size += bitmap[k].size() / 8;
			size += bitmap2[k].size() / 8;
		}
		
		line += "\t" + size;
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
		try {
			bw.write("\n"+line+"\n\n");
		} catch (IOException e) {e.printStackTrace();}
	}

	public static void testWAH32(int[][] data, int[][] data2, int repeat,
			DecimalFormat df, int optimisation) {
		System.out.println("# WAH 32 bit using the compressedbitset library");
		System.out
				.println("# cardinality, size, construction time, time to recover set bits, "
						+ "time to compute unions (OR), intersections (AND)");
		try {
			bw.write("\n"+"# WAH32bits\n"+"# cardinality, size, construction time, time to recover set bits, "
							+ "time to compute unions (OR), intersections (AND) "
							+ "and exclusive unions (XOR) ");
		} catch (IOException e1) {e1.printStackTrace();}
		long bef, aft;
		int bogus = 0;
		String line = "";
		int N = data.length, size = 0;
		
		//Calculating the construction time
		bef = System.currentTimeMillis();
		WAHBitSet[] bitmap = new WAHBitSet[N];
		
		for (int r = 0; r < repeat; ++r) {
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new WAHBitSet();
				for (int x = 0; x < data[k].length; ++x) {
					bitmap[k].set(data[k][x]);
				}			
			}
		}
		aft = System.currentTimeMillis();
		
		// Creating and filling the 2nd WAH index
		WAHBitSet[] bitmap2 = new WAHBitSet[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new WAHBitSet();
			for (int x = 0; x < data2[k].length; ++x)
				bitmap2[k].set(data2[k][x]);
		}
		
		for(int k=0; k<N; k++) {
			size += bitmap[k].memSize() * 4;
			size += bitmap2[k].memSize() * 4;
		}
		
		int cardinality = 0;
		//calculating the all cardinality
		for(int k=0; k<N; k++){
			cardinality += bitmap[k].cardinality();
			cardinality += bitmap2[k].cardinality();
		}
		
		line += "\t"+cardinality+"\t" + size;
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		SizeGraphCoordinates.get(1).lastElement().setGname("WAH 32bit");
		SizeGraphCoordinates.get(1).lastElement().setY(size/1024);
		
		
		
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = new int[bitmap[k].cardinality()];
				int c = 0;
				for (@SuppressWarnings("unchecked")
				Iterator<Integer> i = bitmap[k].iterator(); i.hasNext(); array[c++] = i
						.next().intValue()) {
				}
				bogus += c;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
       // logical or + retrieval
       bef = System.currentTimeMillis();
       for (int r = 0; r < repeat; ++r) {
   			WAHBitSet bitmapor1 = null, bitmapor2;
       		switch(optimisation) {		
                case 0 :
                case 2 : bitmapor1 = bitmap[0];
                		 bitmapor2 = bitmap2[0];	
                		 for (int k = 1; k < N; ++k) {            			 
                			bitmapor1 = bitmapor1.or(bitmap[k]);
                		 	bitmapor2 = bitmapor2.or(bitmap2[k]);
                		 }
                		 	bitmapor1 = bitmapor1.or(bitmapor2);
               		 		break;
                case 1 : 
                case 3 : bitmapor1 = WAHBitSetUtil.fastOR(bitmap);
               			 bitmapor2 = WAHBitSetUtil.fastOR(bitmap2);
               			 bitmapor1 = bitmapor1.or(bitmapor2);
               			 break;                		  
                }
                int[] array = new int[bitmapor1.cardinality()];
                int c = 0;
                for (@SuppressWarnings("unchecked")
                Iterator<Integer> i = bitmapor1.iterator(); i.hasNext(); array[c++] = i
                                        .next().intValue()) {
                }
                bogus += c;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

		
		OrGraphCoordinates.get(1).lastElement().setGname("WAH 32bit");
		OrGraphCoordinates.get(1).lastElement().setY((aft - bef) / 1000.0);

		// logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			WAHBitSet bitmapand1 = null, bitmapand2;
    		switch(optimisation)
    		{		
    		case 0 :
    		case 2 : bitmapand1 = bitmap[0];
    				 bitmapand2 = bitmap2[0];	
    				 for (int k = 1; k < N; ++k) {
    					bitmapand1 = bitmapand1.or(bitmap[k]);
    				 	bitmapand2 = bitmapand2.or(bitmap2[k]);
    				 }
    				 bitmapand1 = bitmapand1.and(bitmapand2);
   		 		 	 break;
    		case 1 : 
    		case 3 : bitmapand1 = WAHBitSetUtil.fastAND(bitmap);
   		 			 bitmapand2 = WAHBitSetUtil.fastAND(bitmap2);
   		  			 bitmapand1 = bitmapand1.and(bitmapand2);
   		 			 break;                		  
    		}
    		int[] array = new int[bitmapand1.cardinality()];
        	int c = 0;
        	for (@SuppressWarnings("unchecked")
        	Iterator<Integer> i = bitmapand1.iterator(); i.hasNext(); array[c++] = i
                            .next().intValue()) {
        	}
        	bogus += c;
		}
        aft = System.currentTimeMillis();
        line += "\t" + df.format((aft - bef) / 1000.0);
		
		AndGraphCoordinates.get(1).lastElement().setGname("WAH 32bit");
		AndGraphCoordinates.get(1).lastElement().setY((aft - bef) / 1000.0);
		
		XorGraphCoordinates.get(1).lastElement().setGname("WAH 32bit");
		XorGraphCoordinates.get(1).lastElement().setY(0.0);

		System.out.println(line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
		System.out.println("# ignore this " + bogus);
		try {
			bw.write("\n"+line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
			bw.write("\n# ignore this " + bogus+"\n\n");
		} catch (IOException e) {e.printStackTrace();}
	}

	public static void testConciseSet(int[][] data, int[][] data2, int repeat,
			DecimalFormat df, int optimisation) {
		System.out
				.println("# ConciseSet 32 bit using the extendedset_2.2 library");
		System.out
				.println("# cardinality, size, construction time, time to recover set bits, time to compute unions  and intersections ");
		try {
			bw.write("\n"+"# ConciseSet\n"+"# cardinality, size, construction time, time to recover set bits, "
							+ "time to compute unions (OR), intersections (AND) "
							+ "and exclusive unions (XOR) ");
		} catch (IOException e1) {e1.printStackTrace();}
		long bef, aft;
		String line = "";
		int bogus = 0;

		int N = data.length, size = 0;
		bef = System.currentTimeMillis();
		ConciseSet[] bitmap = new ConciseSet[N];
		
		for (int r = 0; r < repeat; ++r) {			
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new ConciseSet();
				for (int x = 0; x < data[k].length; ++x) {
					bitmap[k].add(data[k][x]);
				}
			}
		}
		aft = System.currentTimeMillis();
		
		// Creating and filling the 2nd Concise index
		ConciseSet[] bitmap2 = new ConciseSet[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new ConciseSet();
			for (int x = 0; x < data2[k].length; ++x)
				bitmap2[k].add(data2[k][x]);
		}
		
		for (int k=0; k<N; k++) {
		size += (int) (bitmap[k].size() * bitmap[k].collectionCompressionRatio()) * 4;
		size += (int) (bitmap2[k].size() * bitmap2[k].collectionCompressionRatio()) * 4;
		}
		
		int cardinality = 0;
		//calculating all bitmaps the cardinality
		for(int k=0; k<N; k++) {
			cardinality += bitmap[k].toArray().length;
			cardinality += bitmap2[k].toArray().length;
		}
		
		line += "\t"+cardinality+"\t" + size;
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		SizeGraphCoordinates.get(2).lastElement().setGname("Concise");
		SizeGraphCoordinates.get(2).lastElement().setY(size/1024);
		
		
		
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = bitmap[k].toArray();
				bogus += array.length;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
   			ConciseSet bitmapor1 = null;
			ConciseSet bitmapor2;
       		switch(optimisation) {		
                case 0 :
                case 2 : bitmapor1 = bitmap[0].clone();
                		 bitmapor2 = bitmap2[0].clone();	
                		 for (int k = 1; k < N; ++k) {
                			bitmapor1 = bitmapor1.union(bitmap[k]);
                		 	bitmapor2 = bitmapor2.union(bitmap2[k]);
                		 }
                		 	bitmapor1 = bitmapor1.union(bitmapor2);
               		 		break;
                case 1 : 
                case 3 : bitmapor1 = ConciseSetUtil.fastOR(bitmap);
               			 bitmapor2 = ConciseSetUtil.fastOR(bitmap2);
               			 bitmapor1 = bitmapor1.union(bitmapor2);
               			 break;                		  
            }
       		int[] array = bitmapor1.toArray();
            if(array!=null) bogus += array.length;
		}
            aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);
		

		
		OrGraphCoordinates.get(2).lastElement().setGname("Concise");
		OrGraphCoordinates.get(2).lastElement().setY((aft - bef) / 1000.0);

		// logical and + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
           			ConciseSet bitmapand1 = null;
        			ConciseSet bitmapand2;
               		switch(optimisation) {		
                        case 0 :
                        case 2 : bitmapand1 = bitmap[0].clone();
                        		 bitmapand2 = bitmap2[0].clone();	
                        		 for (int k = 1; k < N; ++k) {
                        			bitmapand1 = bitmapand1.intersection(bitmap[k]);
                        		 	bitmapand2 = bitmapand2.intersection(bitmap2[k]);
                        		 }
                        		 	bitmapand1 = bitmapand1.intersection(bitmapand2);
                       		 		break;
                        case 1 : 
                        case 3 : bitmapand1 = ConciseSetUtil.fastAND(bitmap);
                       			 bitmapand2 = ConciseSetUtil.fastAND(bitmap2);
                       			 bitmapand1 = bitmapand1.intersection(bitmapand2);
                       			 break;                		  
                    }
               		int[] array = bitmapand1.toArray();
                    if(array!=null) bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

		
		AndGraphCoordinates.get(2).lastElement().setGname("Concise");
		AndGraphCoordinates.get(2).lastElement().setY((aft - bef) / 1000.0);
		
		// logical xor + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                	ConciseSet bitmapxor1 = null;
        			ConciseSet bitmapxor2;
               		switch(optimisation) {		
                        case 0 :
                        case 2 : bitmapxor1 = bitmap[0].clone();
                        		 bitmapxor2 = bitmap2[0].clone();	
                        		 for (int k = 1; k < N; ++k) {
                        			bitmapxor1 = bitmapxor1.symmetricDifference(bitmap[k]);
                        		 	bitmapxor2 = bitmapxor2.symmetricDifference(bitmap2[k]);
                        		 }
                        		 	bitmapxor1 = bitmapxor1.symmetricDifference(bitmapxor2);
                       		 		break;
                        case 1 : 
                        case 3 : bitmapxor1 = ConciseSetUtil.fastXOR(bitmap);
                       			 bitmapxor2 = ConciseSetUtil.fastXOR(bitmap2);
                       			 bitmapxor1 = bitmapxor1.symmetricDifference(bitmapxor2);
                       			 break;                		  
                    }
               		int[] array = bitmapxor1.toArray();
                    if(array!=null) bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

		
		XorGraphCoordinates.get(2).lastElement().setGname("Concise");
		XorGraphCoordinates.get(2).lastElement().setY((aft - bef) / 1000.0);

		System.out.println(line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
		System.out.println("# ignore this " + bogus);
		try {
			bw.write("\n"+line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
			bw.write("\n# ignore this " + bogus+"\n\n");
		} catch (IOException e) {e.printStackTrace();}
	}

	public static void testSparseBitmap(int[][] data, int[][] data2,
			int repeat, DecimalFormat df, int optimisation) {
		System.out.println("# simple sparse bitmap implementation");
		System.out
				.println("# cardinality, size, construction time, time to recover set bits, time to compute unions (OR), intersections (AND) and exclusive unions (XOR) ");
		try {
			bw.write("\n"+"# simple sparse bitmap\n"+"# cardinality, size, construction time, time to recover set bits, "
							+ "time to compute unions (OR), intersections (AND) "
							+ "and exclusive unions (XOR) ");
		} catch (IOException e1) {e1.printStackTrace();}
		long bef, aft;
		int bogus = 0;
		String line = "";
		int N = data.length;
		bef = System.currentTimeMillis();
		SparseBitmap[] bitmap = new SparseBitmap[N];
		
		for (int r = 0; r < repeat; ++r) {			
			for (int k = 0; k < N; ++k) {
				bitmap[k] = new SparseBitmap();
				for (int x = 0; x < data[k].length; ++x) {
					bitmap[k].set(data[k][x]);
				}
			}
		}
		aft = System.currentTimeMillis();
		
		// Creating and filling the 2nd SparseBitmap index
		SparseBitmap[] bitmap2 = new SparseBitmap[N];
		for (int k = 0; k < N; ++k) {
			bitmap2[k] = new SparseBitmap();
			for (int x = 0; x < data2[k].length; ++x)
				bitmap2[k].set(data2[k][x]);
		}
		
		//Calculating the size
		int size = 0, cardinality = 0;
		for (int k=0; k<N; k++) {
			size += bitmap[k].sizeInBytes();
			size += bitmap2[k].sizeInBytes();
			cardinality += bitmap[k].cardinality;
			cardinality += bitmap2[k].cardinality;
		}		
		
		line += "\t"+cardinality+"\t" + size;
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		SizeGraphCoordinates.get(3).lastElement().setGname("Sparse Bitmap");
		SizeGraphCoordinates.get(3).lastElement().setY(size/1024);
		
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = bitmap[k].toArray();
				bogus += array.length;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			SparseBitmap bitmapor1 = new SparseBitmap();
			SparseBitmap bitmapor2 = new SparseBitmap();
			for (int k = 0; k < N; ++k) {
				bitmapor1.or(bitmap[k]);
				bitmapor2.or(bitmap2[k]);
			}
			bitmapor1.or(bitmapor2);
			int[] array = bitmapor1.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		OrGraphCoordinates.get(3).lastElement().setGname("Sparse Bitmap");
		OrGraphCoordinates.get(3).lastElement().setY((aft - bef) / 1000.0);

		// logical xor + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			SparseBitmap bitmapxor1 = new SparseBitmap();
			SparseBitmap bitmapxor2 = new SparseBitmap();
			for (int k = 0; k < N; ++k) {
				bitmapxor1.xor(bitmap[k]);
				bitmapxor2.xor(bitmap2[k]);
			}
			bitmapxor1.xor(bitmapxor2);
			int[] array = bitmapxor1.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		String xorTime = "\t" + df.format((aft - bef) / 1000.0);
		
		XorGraphCoordinates.get(3).lastElement().setGname("Sparse Bitmap");
		XorGraphCoordinates.get(3).lastElement().setY((aft - bef) / 1000.0);

		// logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			for (int k = 1; k < N; ++k) {
				bitmap[0].and(bitmap[k]);
				bitmap2[0].and(bitmap2[k]);
			}
			bitmap[0].and(bitmap2[0]);
			int[] array = bitmap[0].toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0) + xorTime;
		
		AndGraphCoordinates.get(3).lastElement().setGname("Sparse Bitmap");
		AndGraphCoordinates.get(3).lastElement().setY((aft - bef) / 1000.0);

		System.out.println(line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
		System.out.println("# ignore this " + bogus);
		try {
			bw.write("\n"+line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
			bw.write("\n# ignore this " + bogus+"\n\n");
		} catch (IOException e) {e.printStackTrace();}
	}

	public static void testEWAH64(int[][] data, int[][] data2, int repeat,
			DecimalFormat df, int optimisation) {
		System.out.println("# EWAH using the javaewah library");
		System.out
				.println("# cardinality, size, construction time, time to recover set bits, time to compute unions  and intersections ");
		try {
			bw.write("\n"+"# EWAH64bits\n"+"# cardinality, size, construction time, time to recover set bits, "
							+ "time to compute unions (OR), intersections (AND) "
							+ "and exclusive unions (XOR) ");
		} catch (IOException e1) {e1.printStackTrace();}
		long bef, aft;
		String line = "";
		int bogus = 0;
		int N = data.length;
		//Calculating the construction time and building the 1st array of ewah bitmaps 
		bef = System.currentTimeMillis();
		EWAHCompressedBitmap[] ewah = new EWAHCompressedBitmap[N];
		
		for (int r = 0; r < repeat; ++r) {			
			for (int k = 0; k < N; ++k) {
				ewah[k] = new EWAHCompressedBitmap();
				for (int x = 0; x < data[k].length; ++x) {
					ewah[k].set(data[k][x]);
				}
			}
		}
		aft = System.currentTimeMillis();
		
		// Creating and filling the 2nd array of ewah64 bitmaps
		EWAHCompressedBitmap[] ewah2 = new EWAHCompressedBitmap[N];
		for (int k = 0; k < N; ++k) {
			ewah2[k] = new EWAHCompressedBitmap();
			for (int x = 0; x < data2[k].length; ++x)
				ewah2[k].set(data2[k][x]);
		}
		
		int size = 0;
		for (int k=0; k<N; k++) {
			size += ewah[k].sizeInBytes();
			size += ewah2[k].sizeInBytes();
		}
		
		//Calculating the cardinality from the 1st array bitmaps
		int cardinality = 0;
		for(int k=0; k<N; k++) {
			cardinality += ewah[k].toArray().length;
			cardinality += ewah2[k].toArray().length;
		}
		
		line += "\t"+cardinality+"\t" + size;
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		SizeGraphCoordinates.get(4).lastElement().setGname("Ewah 64bits");
		SizeGraphCoordinates.get(4).lastElement().setY(size/1024);
		
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = ewah[k].toArray();
				bogus += array.length;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// fast logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap bitmapor1 = null, bitmapor2;
       		switch(optimisation) {		
                case 0 :
                case 2 : bitmapor1 = ewah[0];
                		 bitmapor2 = ewah2[0];	
                		 for (int k = 1; k < N; ++k) {
                			bitmapor1 = bitmapor1.or(ewah[k]);
                		 	bitmapor2 = bitmapor2.or(ewah2[k]);
                		 }
                		 	bitmapor1 = bitmapor1.or(bitmapor2);
               		 		break;
                case 1 : 
                case 3 : bitmapor1 = EWAHCompressedBitmap.or(Arrays
    					.copyOf(ewah, N));
               			 bitmapor2 = EWAHCompressedBitmap.or(Arrays
             					.copyOf(ewah2, N));
               			 bitmapor1 = bitmapor1.or(bitmapor2);
               			 break;                		  
            }       		
			int[] array = bitmapor1.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		OrGraphCoordinates.get(4).lastElement().setGname("Ewah 64bits");
		OrGraphCoordinates.get(4).lastElement().setY((aft - bef) / 1000.0);

		// fast logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap bitmapand1 = null, bitmapand2;
       		switch(optimisation) {		
                case 0 :
                case 2 : bitmapand1 = ewah[0];
                		 bitmapand2 = ewah2[0];	
                		 for (int k = 1; k < N; ++k) {
                			bitmapand1 = bitmapand1.and(ewah[k]);
                		 	bitmapand2 = bitmapand2.and(ewah2[k]);
                		 }
                		 	bitmapand1 = bitmapand1.and(bitmapand2);
               		 		break;
                case 1 : 
                case 3 : bitmapand1 = EWAHCompressedBitmap.and(Arrays
    					.copyOf(ewah, N));
               			 bitmapand2 = EWAHCompressedBitmap.and(Arrays
             					.copyOf(ewah2, N));
               			 bitmapand1 = bitmapand1.and(bitmapand2);
               			 break;                		  
            }       		
			int[] array = bitmapand1.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		AndGraphCoordinates.get(4).lastElement().setGname("Ewah 64bits");
		AndGraphCoordinates.get(4).lastElement().setY((aft - bef) / 1000.0);

		// fast logical xor + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap bitmapxor1 = null, bitmapxor2;
       		switch(optimisation) {		
                case 0 :
                case 2 : bitmapxor1 = ewah[0];
                		 bitmapxor2 = ewah2[0];	
                		 for (int k = 1; k < N; ++k) {
                			bitmapxor1 = bitmapxor1.xor(ewah[k]);
                		 	bitmapxor2 = bitmapxor2.xor(ewah2[k]);
                		 }
                		 bitmapxor1 = bitmapxor1.xor(bitmapxor2);
               		 	 break;
                case 1 : 
                case 3 : bitmapxor1 = EWAHCompressedBitmap.xor(Arrays
    									.copyOf(ewah, N));
               			 bitmapxor2 = EWAHCompressedBitmap.xor(Arrays
               					 					.copyOf(ewah2, N));
               			 bitmapxor1 = bitmapxor1.xor(bitmapxor2);
               			 break;                		  
            }       		
			int[] array = bitmapxor1.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		XorGraphCoordinates.get(4).lastElement().setGname("Ewah 64bits");
		XorGraphCoordinates.get(4).lastElement().setY((aft - bef) / 1000.0);

		System.out.println(line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
		System.out.println("# ignore this " + bogus);
		try {
			bw.write("\n"+line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
			bw.write("\n# ignore this " + bogus+"\n\n");
		} catch (IOException e) {e.printStackTrace();}
	}

	public static void testEWAH32(int[][] data, int[][] data2, int repeat,
			DecimalFormat df, int optimisation) {
		System.out.println("# EWAH 32-bit using the javaewah library");
		System.out
				.println("# cardinality, size, construction time, time to recover set bits, time to compute unions  and intersections ");
		try {
			bw.write("\n"+"# EWAH32bits\n"+"# cardinality, size, construction time, time to recover set bits, "
							+ "time to compute unions (OR), intersections (AND) "
							+ "and exclusive unions (XOR) ");
		} catch (IOException e1) {e1.printStackTrace();}
		long bef, aft;
		String line = "";
		long bogus = 0;
		int N = data.length;
		
		//Calculating the construction time and building the 1st array of ewah32 bitmaps 
		bef = System.currentTimeMillis();
		EWAHCompressedBitmap32[] ewah = new EWAHCompressedBitmap32[N];
		
		for (int r = 0; r < repeat; ++r) {			
			for (int k = 0; k < N; ++k) {
				ewah[k] = new EWAHCompressedBitmap32();
				for (int x = 0; x < data[k].length; ++x) {
					ewah[k].set(data[k][x]);
				}
			}
		}
		aft = System.currentTimeMillis();
		
		// Creating and filling the 2nd array of ewah32 bitmaps
		EWAHCompressedBitmap32[] ewah2 = new EWAHCompressedBitmap32[N];
		for (int k = 0; k < N; ++k) {
			ewah2[k] = new EWAHCompressedBitmap32();
			for (int x = 0; x < data2[k].length; ++x)
				ewah2[k].set(data2[k][x]);
		}
		
		int size = 0;
		for (int k=0; k<N; k++) {
			size += ewah[k].sizeInBytes();
			size += ewah2[k].sizeInBytes();
		}
		
		int cardinality = 0;
		//calculating the cardinality per EwahBitmap
		for(int k=0; k<N; k++) {
			cardinality += ewah[k].toArray().length;
			cardinality += ewah2[k].toArray().length;
		}
		
		line += "\t"+cardinality+"\t" + size;
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		SizeGraphCoordinates.get(5).lastElement().setGname("Ewah 32");
		SizeGraphCoordinates.get(5).lastElement().setY(size/1024);
		
		
		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			for (int k = 0; k < N; ++k) {
				int[] array = ewah[k].toArray();
				bogus += array.length;
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// fast logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap32 bitmapor1 = null, bitmapor2;
       		switch(optimisation) {		
                case 0 :
                case 2 : bitmapor1 = ewah[0];
                		 bitmapor2 = ewah2[0];	
                		 for (int k = 1; k < N; ++k) {
                			bitmapor1 = bitmapor1.or(ewah[k]);
                		 	bitmapor2 = bitmapor2.or(ewah2[k]);
                		 }
                		 	bitmapor1 = bitmapor1.or(bitmapor2);
               		 		break;
                case 1 : 
                case 3 : bitmapor1 = EWAHCompressedBitmap32.or(Arrays
    					.copyOf(ewah, N));
               			 bitmapor2 = EWAHCompressedBitmap32.or(Arrays
             					.copyOf(ewah2, N));
               			 bitmapor1 = bitmapor1.or(bitmapor2);
               			 break;                		  
            }       		
			int[] array = bitmapor1.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		OrGraphCoordinates.get(5).lastElement().setGname("Ewah 32");
		OrGraphCoordinates.get(5).lastElement().setY((aft - bef) / 1000.0);

		// fast logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap32 bitmapand1 = null, bitmapand2;
       		switch(optimisation) {		
                case 0 :
                case 2 : bitmapand1 = ewah[0];
                		 bitmapand2 = ewah2[0];	
                		 for (int k = 1; k < N; ++k) {
                			bitmapand1 = bitmapand1.and(ewah[k]);
                		 	bitmapand2 = bitmapand2.and(ewah2[k]);
                		 }
                		 	bitmapand1 = bitmapand1.and(bitmapand2);
               		 		break;
                case 1 : 
                case 3 : bitmapand1 = EWAHCompressedBitmap32.and(Arrays
    					.copyOf(ewah, N));
               			 bitmapand2 = EWAHCompressedBitmap32.and(Arrays
             					.copyOf(ewah2, N));
               			 bitmapand1 = bitmapand1.and(bitmapand2);
               			 break;                		  
            }       		
			int[] array = bitmapand1.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		AndGraphCoordinates.get(5).lastElement().setGname("Ewah 32");
		AndGraphCoordinates.get(5).lastElement().setY((aft - bef) / 1000.0);

		// fast logical xor + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			EWAHCompressedBitmap32 bitmapxor1 = null, bitmapxor2;
       		switch(optimisation) {		
                case 0 :
                case 2 : bitmapxor1 = ewah[0];
                		 bitmapxor2 = ewah2[0];	
                		 for (int k = 1; k < N; ++k) {
                			bitmapxor1 = bitmapxor1.xor(ewah[k]);
                		 	bitmapxor2 = bitmapxor2.xor(ewah2[k]);
                		 }
                		 	bitmapxor1 = bitmapxor1.xor(bitmapxor2);
               		 		break;
                case 1 : 
                case 3 : bitmapxor1 = EWAHCompressedBitmap32.xor(Arrays
    					.copyOf(ewah, N));
               			 bitmapxor2 = EWAHCompressedBitmap32.xor(Arrays
             					.copyOf(ewah2, N));
               			 bitmapxor1 = bitmapxor1.xor(bitmapxor2);
               			 break;                		  
            }       		
			int[] array = bitmapxor1.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		
		XorGraphCoordinates.get(5).lastElement().setGname("Ewah 32");
		XorGraphCoordinates.get(5).lastElement().setY((aft - bef) / 1000.0);

		System.out.println(line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
		System.out.println("# ignore this " + bogus);
		try {
			bw.write("\n"+line+"\n# bits/int = "+df.format(((float)size*8/(float)cardinality)));
			bw.write("\n# ignore this " + bogus+"\n\n");
		} catch (IOException e) {e.printStackTrace();}
	}

	public static void test(int N, int nbr, int repeat) {
		DecimalFormat df = new DecimalFormat("0.###");
		ClusteredDataGenerator cdg = new ClusteredDataGenerator();
		System.out
				.println("# For each instance, we report the size, the construction time, ");
		System.out.println("# the time required to recover the set bits,");
		System.out
				.println("# and the time required to compute logical ors (unions) between lots of bitmaps.");
		for (int sparsity = 1; sparsity < 31 - nbr; sparsity++) {
			int[][] data = new int[N][];
			int[][] data2 = new int[N][];
			int Max = (1 << (nbr + sparsity));
			System.out.println("# generating random data...");
			
			// Generating the first set
			int[] inter = cdg.generateClustered(1 << (nbr / 2), Max);
			int counter = 0;
			for (int k = 0; k < N; ++k) {
				data[k] = IntUtil.unite(inter,
						cdg.generateClustered(1 << nbr, Max));
				counter += data[k].length;
			}
			// Generating the 2nd set
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
			//testBitSet(data, data2, repeat, df);
			//testRoaringBitmap(data, data2, repeat, df,);
			//testWAH32(data, data2, repeat, df);
			//testConciseSet(data, data2, repeat, df);
			//testSparseBitmap(data, data2, repeat, df);
			//testEWAH64(data, data2, repeat, df);
			//testEWAH32(data, data2, repeat, df);

			System.out.println();
		}
	}

}
