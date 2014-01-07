package me.lemire.roaringbitmap.experiments;

import java.util.HashSet;
import java.util.Random;

public class UniformDistribution {
	Random rand = new Random();

	/*
	 * Generating integers with a uniform distribution as described by Concise paper method
	 * SetSize the cardinality of the bitmaps. Number of distinct ints to generate
	 * max the upper bound. The ints are generated from the range [0, max]  
	 */
	public int[] GenartingInts(int SetSize, double max){
		
		int array[] = new int[SetSize];
		
		if(SetSize==max){ 
			for(int i=0; i<max; i++) array[i] = i;
			return array;
		}		
		HashSet<Integer> hs = new HashSet<Integer>();
		
		while(hs.size()<SetSize) {
			double a = this.rand.nextDouble();
			int val = (int) Math.floor(a * max);			
			hs.add(val);
		}
		Object[] tab = hs.toArray();
		for(int i=0; i<tab.length; i++) array[i]=(Integer) tab[i];
		return array;
	}
	
}
