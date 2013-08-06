package me.lemire.roaringbitmap.experiments;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import javax.swing.JFileChooser;

import org.devbrat.util.WAHBitSet;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;

import me.lemire.roaringbitmap.RoaringBitmap;

public class SerializableStarSchemaBenchmark {

	static TreeMap<String, TreeMap<String, ArrayList<Integer>>> TreeBitmapIdx = new TreeMap<String, TreeMap<String, ArrayList<Integer>>>();
	static int nbBitmaps = 0;

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws CloneNotSupportedException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, CloneNotSupportedException {

		System.out.println("Start building :");
		if (args.length > 0) {
			SerializableStarSchemaBenchmark.BuildingBigSSBbitmaps(args[0]);
		} else {
			SerializableStarSchemaBenchmark.BuildingBigSSBbitmaps(null);
		}
		System.out.println("Start experiments :");
		DecimalFormat df = new DecimalFormat("0.###");
		int repeat = 1;
		SerializableStarSchemaBenchmark.testRoaringBitmap(repeat, df);
		SerializableStarSchemaBenchmark.testConciseSet(repeat, df);
		SerializableStarSchemaBenchmark.testWAH32(repeat, df);
		SerializableStarSchemaBenchmark.testEWAH64(repeat, df);
		SerializableStarSchemaBenchmark.testEWAH32(repeat, df);
	}

	public static void BuildingSSBbitmaps() {
		try {
			/**
			 * TODO: rely on a standard library like jcvs instead, as this
			 * approach might be naive and would probably give you headaches on
			 * actual CSV files.
			 * 
			 */
			String path;
			do {
				JFileChooser file = new JFileChooser();
				int val = file.showOpenDialog(null);
				if (val == JFileChooser.CANCEL_OPTION)
					System.exit(0);
				if (val == JFileChooser.APPROVE_OPTION) {
					path = file.getSelectedFile().getAbsolutePath();
					break;
				}
			} while (true);

			BufferedReader source_file = new BufferedReader(
					new FileReader(path));
			String record;
			int row = 1;

			try {
				while ((record = source_file.readLine()) != null) {
					String[] ArrayLine = record.split(",");

					for (int i = 0; i < ArrayLine.length; i++)
						if (TreeBitmapIdx.containsKey("C" + i)) {
							if (TreeBitmapIdx.get("C" + i).containsKey(
									ArrayLine[i]))
								TreeBitmapIdx.get("C" + i).get(ArrayLine[i])
										.add(row);
							else {
								ArrayList<Integer> bitmap = new ArrayList<Integer>();
								bitmap.add(row);
								TreeBitmapIdx.get("C" + i).put(ArrayLine[i],
										bitmap);
								nbBitmaps++;
							}
						} else {
							ArrayList<Integer> bitmap = new ArrayList<Integer>();
							bitmap.add(row);
							TreeMap<String, ArrayList<Integer>> body = new TreeMap<String, ArrayList<Integer>>();
							body.put(ArrayLine[i], bitmap);
							TreeBitmapIdx.put("C" + i, body);
							nbBitmaps++;
						}
					row++;
				}
				source_file.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			System.out.println("Sorry, file not found !");
		}
	}

	public static void testRoaringBitmap(int repeat, DecimalFormat df)
			throws FileNotFoundException, IOException, ClassNotFoundException {
		System.out.println("# RoaringBitmap on the Star Schema Benchmark");
		System.out
				.println("# size, construction time, time to recover set bits, "
						+ "time to compute unions (OR), intersections (AND) "
						+ "and exclusive unions (XOR) ");

		long bef, aft;
		String line = "";

		/**
		 * Bogus is essential otherwise the compiler could optimize away the
		 * computation.
		 */
		int bogus = 0;
		int N = nbBitmaps, r;
		int size = 0;
		File tmpbitmaplocation = File.createTempFile("bitmap", "bin");
		tmpbitmaplocation.deleteOnExit();

		// Calculating the construction time
		bef = System.currentTimeMillis();
		for (r = 0; r < repeat; ++r) {
			DataInputStream in = null;
			ObjectOutputStream oos = null;
			try {
				in = new DataInputStream(new FileInputStream(tmpdatasource));
				oos = new ObjectOutputStream(new FileOutputStream(
						tmpbitmaplocation));
				int setBit;
				for (int i = 0; i < N; i++) {
					RoaringBitmap bitmap = new RoaringBitmap();
					try {
						while ((setBit = in.readInt()) != -1) {
							bitmap.set(setBit);
						}
					} catch (EOFException e) {
					}
					oos.writeObject(bitmap);
					bitmap = null;
				}
			} finally {
				if (in != null)
					in.close();
				if (oos != null)
					oos.close();
			}
		}
		aft = System.currentTimeMillis();

		ObjectInputStream ois = null;
		RoaringBitmap bitmap = null;

		// Validating that ArrayContainers contents are sorted
		// and BitmapContainers cardinalities are corrects
		// for(RoaringBitmap rb: bitmap) rb.validate();
		try {
			ois = new ObjectInputStream(new FileInputStream(tmpbitmaplocation));
			try {
				while ((bitmap = (RoaringBitmap) ois.readObject()) != null) {
					bitmap.validate();
				}
			} catch (EOFException e) {
			}
		} finally {
			if (ois != null)
				ois.close();
		}
		System.out.println("calculate the size");

		// Calculating the size
		try {
			ois = new ObjectInputStream(new FileInputStream(tmpbitmaplocation));
			try {
				while ((bitmap = (RoaringBitmap) ois.readObject()) != null) {
					size += bitmap.getSizeInBytes();
				}
			} catch (EOFException e) {
			}
		} finally {
			if (ois != null)
				ois.close();
		}

		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);

		// uncompressing
		bef = System.currentTimeMillis();
		for (r = 0; r < repeat; ++r)
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				try {
					while ((bitmap = (RoaringBitmap) ois.readObject()) != null) {
						int[] array = bitmap.getIntegers();
						bogus += array.length;
					}
				} catch (EOFException e) {
				}
			} finally {
				if (ois != null)
					ois.close();
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		{
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				RoaringBitmap bitmapor1 = (RoaringBitmap) ois.readObject();
				bitmapor1.validate();
				try {
					while ((bitmap = (RoaringBitmap) ois.readObject()) != null) {
						bitmapor1 = RoaringBitmap.or(bitmapor1, bitmap);
						bitmapor1.validate();
					}
				} catch (EOFException e) {
				}
				int[] array = bitmapor1.getIntegers();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}

		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				RoaringBitmap bitmapor1 = (RoaringBitmap) ois.readObject();
				try {
					while ((bitmap = (RoaringBitmap) ois.readObject()) != null) {
						bitmapor1 = RoaringBitmap.or(bitmapor1, bitmap);
					}
				} catch (EOFException e) {
				}
				int[] array = bitmapor1.getIntegers();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		{
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				RoaringBitmap bitmapand1 = (RoaringBitmap) ois.readObject();
				bitmapand1.validate();
				try {
					while ((bitmap = (RoaringBitmap) ois.readObject()) != null) {
						bitmapand1 = RoaringBitmap.and(bitmapand1, bitmap);
						bitmapand1.validate();
					}
				} catch (EOFException e) {
				}
				int[] array = bitmapand1.getIntegers();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}

		// logical and + retrieval
		bef = System.currentTimeMillis();
		for (r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				RoaringBitmap bitmapand1 = (RoaringBitmap) ois.readObject();
				try {
					while ((bitmap = (RoaringBitmap) ois.readObject()) != null) {
						bitmapand1 = RoaringBitmap.and(bitmapand1, bitmap);
					}
				} catch (EOFException e) {
				}
				int[] array = bitmapand1.getIntegers();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		{
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				RoaringBitmap bitmapxor1 = (RoaringBitmap) ois.readObject();
				bitmapxor1.validate();
				try {
					while ((bitmap = (RoaringBitmap) ois.readObject()) != null) {
						bitmapxor1 = RoaringBitmap.xor(bitmapxor1, bitmap);
						bitmapxor1.validate();
					}
				} catch (EOFException e) {
				}
				int[] array = bitmapxor1.getIntegers();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}

		// logical xor + retrieval
		bef = System.currentTimeMillis();
		for (r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				RoaringBitmap bitmapxor1 = (RoaringBitmap) ois.readObject();
				try {
					while ((bitmap = (RoaringBitmap) ois.readObject()) != null) {
						bitmapxor1 = RoaringBitmap.xor(bitmapxor1, bitmap);
					}
				} catch (EOFException e) {
				}
				int[] array = bitmapxor1.getIntegers();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		System.out.println(line);
		System.out.println("# ignore this " + bogus);
		tmpbitmaplocation.delete();
	}

	public static void testBitSet(int repeat, DecimalFormat df)
			throws ClassNotFoundException, IOException {
		System.out.println("# BitSet");
		System.out
				.println("# size, construction time, time to recover set bits, "
						+ "time to compute unions (OR), intersections (AND) "
						+ "and exclusive unions (XOR) ");
		long bef, aft;
		String line = "";
		int N = nbBitmaps;
		int size = 0;
		int bogus = 0;

		File tmpbitmaplocation = File.createTempFile("bitmap", "bin");
		tmpbitmaplocation.deleteOnExit();
		bef = System.currentTimeMillis();

		for (int r = 0; r < repeat; ++r) {
			DataInputStream in = null;
			ObjectOutputStream oos = null;
			try {
				in = new DataInputStream(new FileInputStream(tmpdatasource));
				oos = new ObjectOutputStream(new FileOutputStream(
						tmpbitmaplocation));
				int setBit;
				for (int i = 0; i < N; i++) {
					BitSet bitmap = new BitSet();
					try {
						while ((setBit = in.readInt()) != -1) {
							bitmap.set(setBit);
						}
					} catch (EOFException e) {
					}
					oos.writeObject(bitmap);
					bitmap = null;
				}
			} finally {
				if (in != null)
					in.close();
				if (oos != null)
					oos.close();
			}
		}
		aft = System.currentTimeMillis();

		ObjectInputStream ois = null;
		BitSet bitmap = null;

		// Calculating the size in bytes
		try {
			ois = new ObjectInputStream(new FileInputStream(tmpbitmaplocation));
			try {
				while ((bitmap = (BitSet) ois.readObject()) != null) {
					size += bitmap.size() / 8;
				}
			} catch (EOFException e) {
			}
		} finally {
			if (ois != null)
				ois.close();
		}

		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);

		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				try {
					while ((bitmap = (BitSet) ois.readObject()) != null) {
						int[] array = new int[bitmap.cardinality()];
						int pos = 0;
						for (int i = bitmap.nextSetBit(0); i >= 0; i = bitmap
								.nextSetBit(i + 1)) {
							array[pos++] = i;
						}
						bogus += array.length;
					}
				} catch (EOFException e) {
				}
			} finally {
				if (ois != null)
					ois.close();
			}

		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				BitSet bitmapor1 = (BitSet) ois.readObject();
				try {
					while ((bitmap = (BitSet) ois.readObject()) != null) {
						bitmapor1.or(bitmap);
					}
				} catch (EOFException e) {
				}
				int[] array = new int[bitmapor1.cardinality()];
				int pos = 0;
				for (int i = bitmapor1.nextSetBit(0); i >= 0; i = bitmapor1
						.nextSetBit(i + 1)) {
					array[pos++] = i;
				}
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				BitSet bitmapand1 = (BitSet) ois.readObject();
				try {
					while ((bitmap = (BitSet) ois.readObject()) != null) {
						bitmapand1.and(bitmap);
					}
				} catch (EOFException e) {
				}
				int[] array = new int[bitmapand1.cardinality()];
				int pos = 0;
				for (int i = bitmapand1.nextSetBit(0); i >= 0; i = bitmapand1
						.nextSetBit(i + 1)) {
					array[pos++] = i;
				}
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// logical xor + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				BitSet bitmapxor1 = (BitSet) ois.readObject();
				try {
					while ((bitmap = (BitSet) ois.readObject()) != null) {
						bitmapxor1.xor(bitmap);
					}
				} catch (EOFException e) {
				}
				int[] array = new int[bitmapxor1.cardinality()];
				int pos = 0;
				for (int i = bitmapxor1.nextSetBit(0); i >= 0; i = bitmapxor1
						.nextSetBit(i + 1)) {
					array[pos++] = i;
				}
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		System.out.println(line);
		tmpbitmaplocation.delete();
		System.out.println("# ignore this " + bogus);

	}

	public static void testConciseSet(int repeat, DecimalFormat df)
			throws IOException, ClassNotFoundException {
		System.out
				.println("# ConciseSet 32 bit using the extendedset_2.2 library");
		System.out
				.println("# size, construction time, time to recover set bits, time to compute unions  and intersections ");
		long bef, aft;
		String line = "";
		int bogus = 0;
		int N = nbBitmaps;
		int size = 0;
		File tmpbitmaplocation = File.createTempFile("bitmap", "bin");
		tmpbitmaplocation.deleteOnExit();

		bef = System.currentTimeMillis();

		for (int r = 0; r < repeat; ++r) {

			DataInputStream in = null;
			ObjectOutputStream oos = null;
			try {
				in = new DataInputStream(new FileInputStream(tmpdatasource));
				oos = new ObjectOutputStream(new FileOutputStream(
						tmpbitmaplocation));
				int setBit;
				for (int i = 0; i < N; i++) {
					ConciseSet bitmap = new ConciseSet();
					try {
						while ((setBit = in.readInt()) != -1) {
							bitmap.add(setBit);
						}
					} catch (EOFException e) {
					}
					oos.writeObject(bitmap);
					bitmap = null;
				}
			} finally {
				if (in != null)
					in.close();
				if (oos != null)
					oos.close();
			}
		}
		aft = System.currentTimeMillis();

		ObjectInputStream ois = null;
		ConciseSet bitmap = null;

		// Calculating the size in bytes
		try {
			ois = new ObjectInputStream(new FileInputStream(tmpbitmaplocation));
			try {
				while ((bitmap = (ConciseSet) ois.readObject()) != null) {
					size += (int) (bitmap.size() * bitmap
							.collectionCompressionRatio()) * 4;
					// System.out.print(setBit+" ");
				}
			} catch (EOFException e) {
			}
		} finally {
			if (ois != null)
				ois.close();
		}

		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);

		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				try {
					while ((bitmap = (ConciseSet) ois.readObject()) != null) {
						int[] array = bitmap.toArray();
						bogus += array.length;
					}
				} catch (EOFException e) {
				}
			} finally {
				if (ois != null)
					ois.close();
			}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				ConciseSet bitmapor1 = (ConciseSet) ois.readObject();
				try {
					while ((bitmap = (ConciseSet) ois.readObject()) != null) {
						bitmapor1.union(bitmap);
					}
				} catch (EOFException e) {
				}
				int[] array = bitmapor1.toArray();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// logical and + retrieval
		bef = System.currentTimeMillis();
		try {
			ois = new ObjectInputStream(new FileInputStream(tmpbitmaplocation));
			ConciseSet bitmapand1 = (ConciseSet) ois.readObject();
			try {
				while ((bitmap = (ConciseSet) ois.readObject()) != null) {
					bitmapand1.intersection(bitmap);
				}
			} catch (EOFException e) {
			}
			int[] array = bitmapand1.toArray();
			bogus += array.length;
		} finally {
			if (ois != null)
				ois.close();
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);
		System.out.println(line);
		System.out.println("# ignore this " + bogus);
		tmpbitmaplocation.delete();
	}

	public static void testWAH32(int repeat, DecimalFormat df)
			throws IOException, ClassNotFoundException {
		System.out.println("# WAH 32 bit using the compressedbitset library");
		System.out
				.println("# size, construction time, time to recover set bits, "
						+ "time to compute unions (OR), intersections (AND)");
		long bef, aft;
		String line = "";
		int N = nbBitmaps, bogus = 0;
		int size = 0;
		File tmpbitmaplocation = File.createTempFile("bitmap", "bin");
		tmpbitmaplocation.deleteOnExit();

		bef = System.currentTimeMillis();

		for (int r = 0; r < repeat; ++r) {
			DataInputStream in = null;
			ObjectOutputStream oos = null;
			try {
				in = new DataInputStream(new FileInputStream(tmpdatasource));
				oos = new ObjectOutputStream(new FileOutputStream(
						tmpbitmaplocation));
				int setBit;
				for (int i = 0; i < N; i++) {
					WAHBitSet bitmap = new WAHBitSet();
					// System.out.println("\nC"+i);
					try {
						while ((setBit = in.readInt()) != -1) {
							bitmap.set(setBit);
						}
					} catch (EOFException e) {
					}
					oos.writeObject(bitmap);
					bitmap = null;
				}
			} finally {
				if (in != null)
					in.close();
				if (oos != null)
					oos.close();
			}
		}
		aft = System.currentTimeMillis();

		ObjectInputStream ois = null;
		WAHBitSet bitmap = null;

		// Calculating the size in bytes
		try {
			ois = new ObjectInputStream(new FileInputStream(tmpbitmaplocation));
			try {
				while ((bitmap = (WAHBitSet) ois.readObject()) != null) {
					size += bitmap.memSize() * 4;
					// System.out.print(setBit+" ");
				}
			} catch (EOFException e) {
			}
		} finally {
			if (ois != null)
				ois.close();
		}

		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);

		// uncompressing
		bef = System.currentTimeMillis();

		for (int r = 0; r < repeat; ++r)
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				try {
					while ((bitmap = (WAHBitSet) ois.readObject()) != null) {
						int[] array = new int[bitmap.cardinality()];
						int c = 0;
						for (@SuppressWarnings("unchecked")
						Iterator<Integer> i = bitmap.iterator(); i.hasNext(); array[c++] = i
								.next().intValue()) {
						}
					}
				} catch (EOFException e) {
				}
			} finally {
				if (ois != null)
					ois.close();
			}

		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				WAHBitSet bitmapor1 = (WAHBitSet) ois.readObject();
				try {
					while ((bitmap = (WAHBitSet) ois.readObject()) != null) {
						bitmapor1.or(bitmap);
					}
				} catch (EOFException e) {
				}
				int[] array = new int[bitmapor1.cardinality()];
				int c = 0;
				for (@SuppressWarnings("unchecked")
				Iterator<Integer> i = bitmapor1.iterator(); i.hasNext(); array[c++] = i
						.next().intValue()) {
				}
				bogus += array[array.length - 1];
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				WAHBitSet bitmapand1 = (WAHBitSet) ois.readObject();
				try {
					while ((bitmap = (WAHBitSet) ois.readObject()) != null) {
						bitmapand1.and(bitmap);
					}
				} catch (EOFException e) {
				}
				int[] array = new int[bitmapand1.cardinality()];
				int c = 0;
				for (@SuppressWarnings("unchecked")
				Iterator<Integer> i = bitmapand1.iterator(); i.hasNext(); array[c++] = i
						.next().intValue()) {
				}
				bogus += array[array.length - 1];
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		System.out.println(line);
		System.out.println("# ignore this " + bogus);

	}

	public static void testEWAH64(int repeat, DecimalFormat df)
			throws IOException, ClassNotFoundException {
		System.out.println("# EWAH 64bits using the javaewah library");
		System.out
				.println("# size, construction time, time to recover set bits, "
						+ "time to compute unions (OR), intersections (AND) "
						+ "and exclusive unions (XOR) ");
		long bef, aft;
		String line = "";
		int bogus = 0;
		int N = nbBitmaps;
		int size = 0;

		File tmpbitmaplocation = File.createTempFile("bitmap", "bin");
		tmpbitmaplocation.deleteOnExit();

		bef = System.currentTimeMillis();

		for (int r = 0; r < repeat; ++r) {
			DataInputStream in = null;
			ObjectOutputStream oos = null;
			try {
				in = new DataInputStream(new FileInputStream(tmpdatasource));
				oos = new ObjectOutputStream(new FileOutputStream(
						tmpbitmaplocation));
				int setBit;
				for (int i = 0; i < N; i++) {
					EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
					try {
						while ((setBit = in.readInt()) != -1) {
							bitmap.set(setBit);
						}
					} catch (EOFException e) {
					}
					oos.writeObject(bitmap);
					bitmap = null;
				}
			} finally {
				if (in != null)
					in.close();
				if (oos != null)
					oos.close();
			}
		}
		aft = System.currentTimeMillis();

		ObjectInputStream ois = null;
		EWAHCompressedBitmap ewah = null;

		// Calculating the size in bytes
		try {
			ois = new ObjectInputStream(new FileInputStream(tmpbitmaplocation));
			try {
				while ((ewah = (EWAHCompressedBitmap) ois.readObject()) != null) {
					size += ewah.sizeInBytes();
				}
			} catch (EOFException e) {
			}
		} finally {
			if (ois != null)
				ois.close();
		}

		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);

		// uncompressing
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r)
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				try {
					while ((ewah = (EWAHCompressedBitmap) ois.readObject()) != null) {
						int[] array = ewah.toArray();
						bogus += array.length;
					}
				} catch (EOFException e) {
				}
			} finally {
				if (ois != null)
					ois.close();
			}
		/**
		 * Error checking: here ewah could be null
		 */
		if (ewah == null) {
			System.err.println("Couldn't load the bitmaps.");
			return;
		}
		for (int k = 0; k < N; ++k) {
			int[] array = ewah.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// fast logical or + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				EWAHCompressedBitmap ewahor1 = (EWAHCompressedBitmap) ois
						.readObject();
				try {
					while ((ewah = (EWAHCompressedBitmap) ois.readObject()) != null) {
						ewahor1.or(ewah);
					}
				} catch (EOFException e) {
				}
				int[] array = ewahor1.toArray();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}

		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// fast logical and + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				EWAHCompressedBitmap ewahand1 = (EWAHCompressedBitmap) ois
						.readObject();
				try {
					while ((ewah = (EWAHCompressedBitmap) ois.readObject()) != null) {
						ewahand1.and(ewah);
					}
				} catch (EOFException e) {
				}
				int[] array = ewahand1.toArray();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}

		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// fast logical xor + retrieval
		bef = System.currentTimeMillis();
		for (int r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				EWAHCompressedBitmap ewahxor1 = (EWAHCompressedBitmap) ois
						.readObject();
				try {
					while ((ewah = (EWAHCompressedBitmap) ois.readObject()) != null) {
						ewahxor1.xor(ewah);
					}
				} catch (EOFException e) {
				}
				int[] array = ewahxor1.toArray();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		System.out.println(line);
		System.out.println("# ignore this " + bogus);
		tmpbitmaplocation.delete();
	}

	public static void testEWAH32(int repeat, DecimalFormat df)
			throws IOException, ClassNotFoundException {
		System.out.println("# EWAH 32-bit using the javaewah library");
		System.out
				.println("# size, construction time, time to recover set bits, "
						+ "time to compute unions (OR), intersections (AND) "
						+ "and exclusive unions (XOR) ");
		long bef, aft;
		String line = "";
		long bogus = 0;
		int N = nbBitmaps, k, r;
		int size = 0;
		File tmpbitmaplocation = File.createTempFile("bitmap", "bin");
		tmpbitmaplocation.deleteOnExit();

		bef = System.currentTimeMillis();

		for (r = 0; r < repeat; ++r) {

			DataInputStream in = null;
			ObjectOutputStream oos = null;
			try {
				in = new DataInputStream(new FileInputStream(tmpdatasource));
				oos = new ObjectOutputStream(new FileOutputStream(
						tmpbitmaplocation));
				int setBit;
				for (int i = 0; i < N; i++) {
					EWAHCompressedBitmap32 bitmap = new EWAHCompressedBitmap32();
					// System.out.println("\nC"+i);
					try {
						while ((setBit = in.readInt()) != -1) {
							bitmap.set(setBit);
							// System.out.print(setBit+" ");
						}
					} catch (EOFException e) {
					}
					oos.writeObject(bitmap);
					bitmap = null;
				}
			} finally {
				if (in != null)
					in.close();
				if (oos != null)
					oos.close();
			}
		}
		aft = System.currentTimeMillis();

		ObjectInputStream ois = null;
		EWAHCompressedBitmap32 ewah = null;

		// Calculating the size in bytes
		try {
			ois = new ObjectInputStream(new FileInputStream(tmpbitmaplocation));
			try {
				while ((ewah = (EWAHCompressedBitmap32) ois.readObject()) != null) {
					size += ewah.sizeInBytes();
				}
			} catch (EOFException e) {
			}
		} finally {
			if (ois != null)
				ois.close();
		}

		line += "\t" + size / 1024;
		line += "\t" + df.format((aft - bef) / 1000.0);

		// uncompressing
		bef = System.currentTimeMillis();
		for (r = 0; r < repeat; ++r)
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				try {
					while ((ewah = (EWAHCompressedBitmap32) ois.readObject()) != null) {
						int[] array = ewah.toArray();
						bogus += array.length;
					}
				} catch (EOFException e) {
				}
			} finally {
				if (ois != null)
					ois.close();
			}
		/**
		 * Error checking: here ewah could be null
		 */
		if (ewah == null) {
			System.err.println("Couldn't load the bitmaps.");
			return;
		}
		for (k = 0; k < N; ++k) {
			int[] array = ewah.toArray();
			bogus += array.length;
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// fast logical or + retrieval
		bef = System.currentTimeMillis();
		for (r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				EWAHCompressedBitmap32 ewahor1 = (EWAHCompressedBitmap32) ois
						.readObject();
				try {
					while ((ewah = (EWAHCompressedBitmap32) ois.readObject()) != null) {
						ewahor1.or(ewah);
					}
				} catch (EOFException e) {
				}
				int[] array = ewahor1.toArray();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// fast logical and + retrieval
		bef = System.currentTimeMillis();
		for (r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				EWAHCompressedBitmap32 ewahand1 = (EWAHCompressedBitmap32) ois
						.readObject();
				try {
					while ((ewah = (EWAHCompressedBitmap32) ois.readObject()) != null) {
						ewahand1.or(ewah);
					}
				} catch (EOFException e) {
				}
				int[] array = ewahand1.toArray();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		// fast logical xor + retrieval
		bef = System.currentTimeMillis();
		for (r = 0; r < repeat; ++r) {
			try {
				ois = new ObjectInputStream(new FileInputStream(
						tmpbitmaplocation));
				EWAHCompressedBitmap32 ewahxor1 = (EWAHCompressedBitmap32) ois
						.readObject();
				try {
					while ((ewah = (EWAHCompressedBitmap32) ois.readObject()) != null) {
						ewahxor1.or(ewah);
					}
				} catch (EOFException e) {
				}
				int[] array = ewahxor1.toArray();
				bogus += array.length;
			} finally {
				if (ois != null)
					ois.close();
			}
		}
		aft = System.currentTimeMillis();
		line += "\t" + df.format((aft - bef) / 1000.0);

		System.out.println(line);
		System.out.println("# ignore this " + bogus);
		tmpbitmaplocation.delete();
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		final Iterator<Entry<String, TreeMap<String, ArrayList<Integer>>>> H = TreeBitmapIdx
				.entrySet().iterator();

		Entry<String, TreeMap<String, ArrayList<Integer>>> sh;
		Entry<String, ArrayList<Integer>> s;
		int counter = 0;

		while (H.hasNext()) {
			sh = H.next();
			sb.append(sh.getKey() + "\n");
			final Iterator<Entry<String, ArrayList<Integer>>> I = sh.getValue()
					.entrySet().iterator();
			while (I.hasNext()) {
				s = I.next();
				counter++;
				sb.append(s.getKey() + " :: " + s.getValue().toString() + "\n");
			}
		}
		System.out.println("cardinality = " + counter);
		return sb.toString();
	}

	static File tmpdatasource;

	public static void BuildingBigSSBbitmaps(String path) throws IOException {

		String record;
		tmpdatasource = File.createTempFile("bitmap", "bin");
		tmpdatasource.deleteOnExit();
		DataOutputStream oo = new DataOutputStream(new FileOutputStream(
				tmpdatasource));
		if (path == null)
			do {
				JFileChooser file = new JFileChooser();
				int val = file.showOpenDialog(null);
				if (val == JFileChooser.CANCEL_OPTION)
					System.exit(0);
				if (val == JFileChooser.APPROVE_OPTION) {
					path = file.getSelectedFile().getAbsolutePath();
					break;
				}
			} while (true);

		try {
			TreeMap<String, ArrayList<Integer>> Bitmaps = null;
			int column = 0, row;
			String[] ArrayLine = { "0" };

			while (column < ArrayLine.length) {
				row = 0;
				Bitmaps = new TreeMap<String, ArrayList<Integer>>();
				BufferedReader source_file = new BufferedReader(new FileReader(
						path));

				while ((record = source_file.readLine()) != null
						&& row <= 100000) {
					ArrayLine = record.split(",");
					if (Bitmaps.containsKey(ArrayLine[column]))
						Bitmaps.get(ArrayLine[column]).add(row);
					else {
						ArrayList<Integer> bitmap = new ArrayList<Integer>();
						bitmap.add(row);
						Bitmaps.put(ArrayLine[column], bitmap);
						nbBitmaps++;
					}
					row++;
				}
				source_file.close();
				Iterator<Entry<String, ArrayList<Integer>>> I = Bitmaps
						.entrySet().iterator();
				Entry<String, ArrayList<Integer>> s;
				while (I.hasNext()) {
					s = I.next();
					for (int i = 0; i < s.getValue().size(); i++)
						oo.writeInt(s.getValue().get(i));
					oo.writeInt(-1);
				}
				column++;
				Bitmaps = null;
				source_file = null;
			}
		} finally {
			oo.close();
		}
	}
}
