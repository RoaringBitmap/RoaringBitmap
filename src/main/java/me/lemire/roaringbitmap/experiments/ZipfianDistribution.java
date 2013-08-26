package me.lemire.roaringbitmap.experiments;

import java.util.HashSet;
import java.util.Random;

public class ZipfianDistribution {
	
	Random rand = new Random();

	public int[] GenartingInts(int SetSize, double max){
		
		int array[] = new int[SetSize];
		
		if(SetSize==max){ 
			for(int i=0; i<max; i++) array[i] = i;
			return array;
		}		
		HashSet<Integer> hs = new HashSet<Integer>();
		
		while(hs.size()<SetSize) {
			double a = this.rand.nextDouble();
			int val = (int) (Math.pow(a, 2) * max);			
			hs.add(val);
		}
		Object[] tab = hs.toArray();
		for(int i=0; i<tab.length; i++) array[i]=(Integer) tab[i];
		return array;
	}
	
	private boolean contains(int[] array, int val) {
		for(int i=0; i<array.length; i++) 
			if(array[i]==val) return true;		
		return false;
	}
}
