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
		
		HashSet hs = new HashSet();
		int hsSize = 0;
		
		while(hs.size()<SetSize) {
			double a = this.rand.nextDouble();
			int val = (int) (Math.pow(a, 2) * max);
			hs.add(val);
			if(hs.size()>0) array[hs.size()-1] = val;
		}		
		
		return array;
	}

}
