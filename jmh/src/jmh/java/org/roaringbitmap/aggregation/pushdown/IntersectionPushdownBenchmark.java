package org.roaringbitmap.aggregation.pushdown;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RandomData;
import org.roaringbitmap.RoaringBitmap;

import org.openjdk.jmh.annotations.*;

/**
 * Benchmarks queries of the form (x AND (y OR z))
 */
@State(Scope.Benchmark)
public class IntersectionPushdownBenchmark {

  public static class Pair {
    RoaringBitmap toIntersect;
    RoaringBitmap[] toUnite;

    Pair(RoaringBitmap toIntersect, RoaringBitmap... toUnite) {
      this.toIntersect = toIntersect;
      this.toUnite = toUnite;
    }
  }

  public static enum Scenario {
    EQUAL {
      @Override
      Pair create(int keysInIntersection, int unionSize) {
        RoaringBitmap toIntersect = RandomData.randomBitmap(keysInIntersection);
        RoaringBitmap[] toUnite = new RoaringBitmap[unionSize];
        for (int i = 0; i < unionSize; i++) {
          toUnite[i] = toIntersect.clone();
        }
        return new Pair(toIntersect, toUnite);
      }
    },
    STEPS {
      @Override
      Pair create(int keysInIntersection, int unionSize) {
        int startKey = 0;
        RoaringBitmap toIntersect = RandomData.randomContiguousBitmap(startKey, keysInIntersection);
        RoaringBitmap[] toUnite = new RoaringBitmap[unionSize];
        for (int i = 0; i < unionSize; i++) {
          toUnite[i] = RandomData.randomContiguousBitmap(startKey, keysInIntersection);
          startKey += keysInIntersection;
        }
        return new Pair(toIntersect, toUnite);
      }
    };

    abstract Pair create(int keysInIntersection, int unionSize);
  }

  @Param({"10", "100"})
  int unionSize;

  @Param({"10", "100", "1000"})
  int keysInIntersection;

  @Param Scenario scenario;

  private RoaringBitmap toIntersect;
  private RoaringBitmap[] toUnite;

  @Setup(Level.Trial)
  public void setup() {
    Pair pair = scenario.create(keysInIntersection, unionSize);
    toIntersect = pair.toIntersect;
    toUnite = pair.toUnite;
  }

  @Benchmark
  public RoaringBitmap orThenAnd() {
    return RoaringBitmap.and(toIntersect, FastAggregation.or(toUnite));
  }

  @Benchmark
  public RoaringBitmap andEachThenOr() {
    RoaringBitmap result = new RoaringBitmap();
    for (RoaringBitmap bitmap : toUnite) {
      result.or(RoaringBitmap.and(toIntersect, bitmap));
    }
    return result;
  }

  @Benchmark
  public RoaringBitmap orWithContext() {
    return FastAggregation.orWithContext(toIntersect, toUnite);
  }
}
