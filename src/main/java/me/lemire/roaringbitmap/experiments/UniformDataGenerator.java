package me.lemire.roaringbitmap.experiments;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

/**
 * This class will generate "clustered" lists of random integers. That is, the
 * integers tend not to be randomly distributed.
 * 
 * @author Daniel Lemire
 */
public class UniformDataGenerator {
        Random rand = new Random();
    public UniformDataGenerator(){
    }

    /**
    * generates randomly N distinct integers from 0 to Max.
    */
    int[] generateUniform(int N, int Max) {
        if (N > Max)
            throw new RuntimeException("not possible");
        int[] ans = new int[N];
        if (N == Max) {
            for (int k = 0; k < N; ++k)
                ans[k]=k;
            return ans;
        }
        HashSet<Integer> s = new HashSet<Integer>();
        while (s.size() < N)
            s.add(new Integer(this.rand.nextInt(Max)));
        Iterator<Integer> i = s.iterator();
        for (int k = 0; k < N; ++k)
            ans[k]=i.next().intValue();
        Arrays.sort(ans);
        return ans;
    }

    
}
