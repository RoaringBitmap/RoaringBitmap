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
		
		int pos=0;
		while(pos<SetSize) {
			double a = this.rand.nextDouble();
			int val = (int) (Math.pow(a, 2) * max);			
			if(!contains(array, val)) array[pos++] = val;
		}				
		return array;
	}
	
	private boolean contains(int[] array, int val) {
		for(int i=0; i<array.length; i++) 
			if(array[i]==val) return true;
		
		return false;
	}

}
