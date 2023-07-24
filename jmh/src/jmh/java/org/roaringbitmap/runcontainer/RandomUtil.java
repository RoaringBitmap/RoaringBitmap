package org.roaringbitmap.runcontainer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.roaringbitmap.Container;

public class RandomUtil {
  /**
   * generates randomly N distinct integers from 0 to Max.
   */
  static int[] generateUniformHash(Random rand, int N, int Max) {
    if (N > Max)
      throw new RuntimeException("not possible");
    if (N > Max / 2) {
      return negate(generateUniformHash(rand, Max - N, Max), Max);
    }
    int[] ans = new int[N];
    HashSet<Integer> s = new HashSet<Integer>();
    while (s.size() < N)
      s.add(rand.nextInt(Max));
    Iterator<Integer> i = s.iterator();
    for (int k = 0; k < N; ++k)
      ans[k] = i.next().intValue();
    Arrays.sort(ans);
    return ans;
  }

  static int[] generateCrazyRun(Random rand, int Max) {
    int[] answer = new int[Max / 2];
    int start = Max / 3;
    for (int k = 0; k < answer.length; ++k)
      answer[k] = k + start;
    return answer;
  }

  /**
   * output all integers from the range [0,Max) that are not in the array
   */
  static int[] negate(int[] x, int Max) {
    int[] ans = new int[Max - x.length];
    int i = 0;
    int c = 0;
    for (int j = 0; j < x.length; ++j) {
      int v = x[j];
      for (; i < v; ++i)
        ans[c++] = i;
      ++i;
    }
    while (c < ans.length)
      ans[c++] = i++;
    return ans;
  }

  public static Container fillMeUp(Container c, int[] values) {
    if (c.getCardinality() != 0)
      throw new RuntimeException("Please provide an empty container. ");
    if (values.length == 0)
      throw new RuntimeException("You are trying to create an empty bitmap! ");
    for (int k = 0; k < values.length; ++k)
      c = c.add((char) values[k]);
    if (c.getCardinality() != values.length)
      throw new RuntimeException("add failure");
    return c;
  }

}
