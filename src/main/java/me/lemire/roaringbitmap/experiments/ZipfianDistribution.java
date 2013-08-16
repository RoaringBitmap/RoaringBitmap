package me.lemire.roaringbitmap.experiments;

import java.util.Random;

public class ZipfianDistribution {
	
	Random rand = new Random();

	public int[] GenartingInts(int SetSize, double max){
		
		int array[]= new int[SetSize];
		
		for(int j=0; j<array.length; j++) {
			double a = this.rand.nextDouble();
			array[j]= (int) (a * max);
		}
		
		return array;
	}

}
