
package org.roaringbitmap.bsi.buffer;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.bsi.BitmapSliceIndex;
import org.roaringbitmap.bsi.BitmapSliceIndex.Operation;
import org.roaringbitmap.bsi.Pair;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ParallelAggregationBase
 */
public class BitSliceIndexBase {

  /**
   * the maxValue of this bsi
   */
  protected int maxValue;

  /**
   * the minValue of this bsi
   */
  protected int minValue;

  /**
   * the bit component slice Array of this bsi
   */
  protected ImmutableRoaringBitmap[] bA;

  /**
   * the exist bitmap of this bsi which means the columnId have value in this bsi
   */
  protected ImmutableRoaringBitmap ebM;


  public int bitCount() {
    return this.bA.length;
  }

  public long getLongCardinality() {
    return this.ebM.getLongCardinality();
  }

  /**
   * GetValue gets the value at the column ID.  Second param will be false for non-existence values.
   */
  public Pair<Integer, Boolean> getValue(int columnId) {
    boolean exists = this.ebM.contains(columnId);
    if (!exists) {
      return Pair.newPair(0, false);
    }
    int value = 0;
    for (int i = 0; i < this.bitCount(); i++) {
      if (this.bA[i].contains(columnId)) {
        value |= (1 << i);
      }
    }
    return Pair.newPair(value, true);
  }


  /**
   * valueExists tests whether the value exists.
   */
  public boolean valueExist(Long columnId) {
    return this.ebM.contains(columnId.intValue());
  }

  //=====================================================================================
  // parallel execute frame
  //======================================================================================

  /**
   * use java threadPool to parallel exec
   *
   * @param func    to exec
   * @param parallelism
   * @param foundSet
   * @param pool    threadPool to exec
   * @return a list of completable futures
   */
  protected <R> List<CompletableFuture<R>> parallelExec(Function<int[], R> func,
                              int parallelism,
                              ImmutableRoaringBitmap foundSet,
                              ExecutorService pool) {
    int batchSize = foundSet.getCardinality() / parallelism;
    // fix when batchSize < parallelism
    batchSize = Math.max(batchSize, parallelism);

    // todo RoaringBitmap's batchIterator return max 2^16
    batchSize = Math.min(batchSize, 65536);


    List<int[]> batches = new ArrayList<>();

    final BatchIterator batchIterator = foundSet.getBatchIterator();
    while (batchIterator.hasNext()) {
      int[] buffer = new int[batchSize];
      int cardinality = batchIterator.nextBatch(buffer);
      if (cardinality > 0) {
        if (cardinality == batchSize) {
          batches.add(buffer);
        } else {
          int[] buff = new int[cardinality];
          System.arraycopy(buffer, 0, buff, 0, cardinality);
          batches.add(buff);
        }
      }
    }

    List<CompletableFuture<R>> futures = new ArrayList<>();
    for (int[] batch : batches) {
      CompletableFuture<R> future = invokeAsync(() -> {
        return func.apply(batch);
      }, null, pool);
      futures.add(future);
    }
    return futures;
  }

  protected <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futuresList) {
    CompletableFuture<Void> allFuturesResult =
        CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[0]));
    return allFuturesResult.thenApply(v ->
        futuresList.stream().
            map(CompletableFuture::join).
            collect(Collectors.<T>toList())
    );
  }

  protected ImmutableRoaringBitmap parallelMR(int parallelism,
                        ImmutableRoaringBitmap foundSet,
                        Function<int[], ImmutableRoaringBitmap> func,
                        ExecutorService pool)
      throws InterruptedException, ExecutionException {

    List<CompletableFuture<ImmutableRoaringBitmap>> futures = parallelExec(func, parallelism, foundSet, pool);

    allOf(futures);

    ImmutableRoaringBitmap[] rbs = new ImmutableRoaringBitmap[futures.size()];
    for (int i = 0; i < futures.size(); i++) {
      rbs[i] = futures.get(i).get();
    }

    return MutableRoaringBitmap.or(rbs);
  }

  protected <T> CompletableFuture<T> invokeAsync(Supplier<T> supplier,
                           Function<Exception, T> exceptionHandler,
                           Executor forkJoinExecutor) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return supplier.get();
      } catch (Exception e) {
        if (exceptionHandler == null) {
          throw e;
        }
        return exceptionHandler.apply(e);
      }
    }, forkJoinExecutor);
  }

  /**
   * O'Neil range using a bit-sliced index
   *
   * @param operation
   * @param predicate
   * @param foundSet
   * @return ImmutableRoaringBitmap
   * see https://github.com/lemire/BitSliceIndex/blob/master/src/main/java/org/roaringbitmap/circuits/comparator/BasicComparator.java
   */
  private ImmutableRoaringBitmap oNeilCompare(BitmapSliceIndex.Operation operation,
                        int predicate,
                        ImmutableRoaringBitmap foundSet) {
    ImmutableRoaringBitmap fixedFoundSet = foundSet == null ? this.ebM : foundSet;

    MutableRoaringBitmap GT = operation == Operation.GT || operation == Operation.GE
        ? new MutableRoaringBitmap() : null;
    MutableRoaringBitmap LT = operation == Operation.LT || operation == Operation.LE
        ? new MutableRoaringBitmap() : null;
    ImmutableRoaringBitmap EQ = this.ebM;


    for (int i = this.bitCount() - 1; i >= 0; i--) {
      int bit = (predicate >> i) & 1;
      if (bit == 1) {
        if (operation == Operation.LT || operation == Operation.LE) {
          LT = ImmutableRoaringBitmap.or(LT, ImmutableRoaringBitmap.andNot(EQ, this.bA[i]));
        }
        EQ = ImmutableRoaringBitmap.and(EQ, this.bA[i]);
      } else {
        if (operation == Operation.GT || operation == Operation.GE) {
          GT = ImmutableRoaringBitmap.or(GT, ImmutableRoaringBitmap.and(EQ, this.bA[i]));
        }
        EQ = ImmutableRoaringBitmap.andNot(EQ, this.bA[i]);
      }

    }
    if (operation != Operation.LT && operation != Operation.GT) {
      EQ = ImmutableRoaringBitmap.and(fixedFoundSet, EQ);
    }
    switch (operation) {
      case EQ:
        return EQ;
      case GT:
        return ImmutableRoaringBitmap.and(GT, fixedFoundSet);
      case LT:
        return ImmutableRoaringBitmap.and(LT, fixedFoundSet);
      case LE:
        return ImmutableRoaringBitmap.or(LT, EQ);
      case GE:
        return ImmutableRoaringBitmap.or(GT, EQ);
      default:
        throw new IllegalArgumentException("");
    }
  }

  /**
   * this as per Owen's tech report, section 4.4.2 but uses horizontal aggregation
   *
   * @param predicate >=predicate
   * @return ImmutableRoaringBitmap
   * see https://github.com/lemire/BitSliceIndex/blob/master/src/main/java/org/roaringbitmap/circuits/comparator/OwenComparator.java
   */
  private ImmutableRoaringBitmap owenGreatEqual(int predicate,
                          ImmutableRoaringBitmap foundSet) {
    ImmutableRoaringBitmap lastSpineGate = null;
    int beGtrThan = predicate - 1;
    List<ImmutableRoaringBitmap> orInputs = new ArrayList<>();
    int leastSignifZero = Long.numberOfTrailingZeros(~beGtrThan);
    // work from most significant bit down to the last 1.
    for (int workingBit = this.bitCount() - 1; workingBit >= leastSignifZero; --workingBit) {
      if ((beGtrThan & (1L << workingBit)) == 0L) {
        if (lastSpineGate == null) // don't make a singleton AND!
          orInputs.add(this.bA[workingBit]);
        else {
          // really make the AND
          orInputs.add(MutableRoaringBitmap.and(lastSpineGate, this.bA[workingBit]));
        }
      } else {
        if (lastSpineGate == null)
          lastSpineGate = this.bA[workingBit];
        else
          lastSpineGate = ImmutableRoaringBitmap.and(lastSpineGate, this.bA[workingBit]);
      }
    }

    ImmutableRoaringBitmap result = BufferFastAggregation.horizontal_or(orInputs.toArray(new ImmutableRoaringBitmap[0]));
    //horizontal_or the performance is better
    // return BufferFastAggregation.or(orInputs.toArray(new ImmutableRoaringBitmap[0]))

    if (null == foundSet) {
      return result;
    } else {
      return ImmutableRoaringBitmap.and(result, foundSet);
    }
  }

  //------------------------------------------------------------------------------------------------------------
  // See Bit-Sliced Index Arithmetic
  // Finding the rows with the k largest values in a BSI. Given a BSI, S = S^P,S^(P-1)...S^1,S^0  over a table T
  // and a positive integer k<= |T|, we wish to find the bitmap F (for "found set") of rows r with the k largest
  // S-values, S(r), in T.
  // Algorithm 4.1. Find k rows with largest values in a BSI.
  //------------------------------------------------------------------------------------------------------------
  //  if (k > COUNT(EBM) or k < 0)            -- test if parameter k is valid
  //  Error ("k is invalid")              -- if not, exit; otherwise, kth largest S-value exists
  //  G = empty set; E = EBM;               -- G starts with no rows; E with all rows
  //  for (i = P; i >= 0; i--) {              -- i is a descending loop index for bit-slice number
  //    X = G OR (E AND S^i)              -- X is trial set: G OR {rows in E with 1-bit in position i}
  //    if ((n = COUNT(X) ) > k)            -- if n = COUNT(X) has more than k rows
  //        E = E AND S^i             -- E in next pass contains only rows r with bit i on in S(r)
  //    else if (n < k) {               -- if n = COUNT(X) has less than k rows
  //        G = X                 -- G in next pass gets all rows in X
  //        E = E AND (NOT S^i)           -- E in next pass contains no rows r with bit i on in S(r)
  //    }
  //    else {                    -- n = k; might never happen
  //        E = E AND S^i             -- all rows r with bit i on in S(r) will be in E
  //      break;                  -- done looping
  //    }
  //  }                         -- we know at this point that COUNT(G) <= k
  //  F = G OR E                      -- might be too many rows in F; check below
  //  if ((n = (COUNT(F) - k) > 0)            -- if n too many rows in F
  //   {turn off n bits from E in F};           -- throw out some ties to return exactly k rows 
  public MutableRoaringBitmap topK(ImmutableRoaringBitmap foundSet, int k) {
    ImmutableRoaringBitmap fixedFoundSet = foundSet == null ? this.ebM : foundSet;
    if (k > fixedFoundSet.getLongCardinality() || k < 0) {
      throw new IllegalArgumentException("TopK param error,cardinality:"
          + fixedFoundSet.getLongCardinality() + " k:" + k);
    }

    MutableRoaringBitmap G = new MutableRoaringBitmap();
    ImmutableRoaringBitmap E = fixedFoundSet;

    for (int i = this.bitCount() - 1; i >= 0; i--) {
      MutableRoaringBitmap X = ImmutableRoaringBitmap.or(G, ImmutableRoaringBitmap.and(E, this.bA[i]));
      long n = X.getLongCardinality();
      if (n > k) {
        E = ImmutableRoaringBitmap.and(E, this.bA[i]);
      } else if (n < k) {
        G = X;
        E = ImmutableRoaringBitmap.andNot(E, this.bA[i]);
      } else {
        E = ImmutableRoaringBitmap.and(E, this.bA[i]);
        break;
      }
    }

    MutableRoaringBitmap F = ImmutableRoaringBitmap.or(G, E);
    long n = F.getLongCardinality() - k;
    if (n > 0) {
      IntIterator i = F.getIntIterator();
      while (i.hasNext() && n > 0) {
        F.remove(i.next());
        --n;
      }
    }

    if (F.getCardinality() != k)
      throw new RuntimeException("bugs found when compute topK");

    return F;
  }


  /**
   * EQ: =
   *
   * @param foundSet
   * @param predicate
   * @return the computed immutable bitmap
   */
  public ImmutableRoaringBitmap rangeEQ(ImmutableRoaringBitmap foundSet, int predicate) {
    // Start with set of columns with values set.
    ImmutableRoaringBitmap eqBitmap = this.ebM;

    if (foundSet != null) {
      eqBitmap = ImmutableRoaringBitmap.and(eqBitmap, foundSet);
    }

    // https://github.com/RoaringBitmap/RoaringBitmap/issues/549
    ImmutableRoaringBitmap result = compareUsingMinMax(BitmapSliceIndex.Operation.EQ, predicate, 0, foundSet);
    if (result != null) {
      return result;
    }

    for (int i = this.bA.length - 1; i >= 0; i--) {
      ImmutableRoaringBitmap slice = this.bA[i];
      int bit = (predicate >> i) & 1;
      if (bit == 1) {
        eqBitmap = ImmutableRoaringBitmap.and(eqBitmap, slice);
      } else {
        eqBitmap = ImmutableRoaringBitmap.andNot(eqBitmap, slice);
      }
    }
    return eqBitmap;
  }

  /**
   * NEQ: !=
   *
   * @param foundSet
   * @param predicate
   * @return the computed immutable bitmap
   */
  public ImmutableRoaringBitmap rangeNEQ(ImmutableRoaringBitmap foundSet, int predicate) {
    ImmutableRoaringBitmap eqBitmap = rangeEQ(foundSet, predicate);
    return ImmutableRoaringBitmap.andNot(this.ebM, eqBitmap);
  }

  public ImmutableRoaringBitmap rangeLT(ImmutableRoaringBitmap foundSet, int predicate) {
    return compare(BitmapSliceIndex.Operation.LT, predicate, 0, foundSet);
  }

  public ImmutableRoaringBitmap rangeLE(ImmutableRoaringBitmap foundSet, int predicate) {
    return compare(BitmapSliceIndex.Operation.LE, predicate, 0, foundSet);
  }

  public ImmutableRoaringBitmap rangeGT(ImmutableRoaringBitmap foundSet, int predicate) {
    return compare(BitmapSliceIndex.Operation.GT, predicate, 0, foundSet);
  }

  public ImmutableRoaringBitmap rangeGE(ImmutableRoaringBitmap foundSet, int predicate) {
    return compare(BitmapSliceIndex.Operation.GE, predicate, 0, foundSet);
  }

  public ImmutableRoaringBitmap range(ImmutableRoaringBitmap foundSet, int start, int end) {
    return compare(BitmapSliceIndex.Operation.RANGE, start, end, foundSet);

  }

  /**
   * BSI Compare use single thread
   * this Function compose algorithm from O'Neil and Owen Kaser
   * the GE algorithm is from Owen since the performance is better.  others are from O'Neil
   *
   * @param operation
   * @param startOrValue the start or value of comparison, when the comparison operation is range, it's start,
   *           when others,it's value.
   * @param end      the end value of comparison. when the comparison operation is not range,the end = 0
   * @param foundSet   columnId set we want compare,using RoaringBitmap to express
   * @return columnId set we found in this bsi with giving conditions, using RoaringBitmap to express
   */
  public ImmutableRoaringBitmap compare(BitmapSliceIndex.Operation operation, int startOrValue, int end, ImmutableRoaringBitmap foundSet) {
    ImmutableRoaringBitmap result = compareUsingMinMax(operation, startOrValue, end, foundSet);
    if (result != null) {
      return result;
    }

    switch (operation) {
      case EQ:
        return rangeEQ(foundSet, startOrValue);
      case NEQ:
        return rangeNEQ(foundSet, startOrValue);
      case GE:
        return owenGreatEqual(startOrValue, foundSet);
      case GT: {
        return oNeilCompare(BitmapSliceIndex.Operation.GT, startOrValue, foundSet);
      }
      case LT:
        return oNeilCompare(BitmapSliceIndex.Operation.LT, startOrValue, foundSet);

      case LE:
        return oNeilCompare(BitmapSliceIndex.Operation.LE, startOrValue, foundSet);

      case RANGE: {
        ImmutableRoaringBitmap left = owenGreatEqual(startOrValue, foundSet);
        ImmutableRoaringBitmap right = oNeilCompare(BitmapSliceIndex.Operation.LE, end, foundSet);

        return ImmutableRoaringBitmap.and(left, right);
      }
      default:
        throw new IllegalArgumentException("not support operation!");
    }
  }

  private ImmutableRoaringBitmap compareUsingMinMax(BitmapSliceIndex.Operation operation, int startOrValue, int end, ImmutableRoaringBitmap foundSet) {
    ImmutableRoaringBitmap all = foundSet == null ? this.ebM.clone() : ImmutableRoaringBitmap.and(this.ebM, foundSet);
    ImmutableRoaringBitmap empty = new MutableRoaringBitmap();

    switch (operation) {
      case LT:
        if (startOrValue > maxValue) {
          return all;
        } else if (startOrValue <= minValue) {
          return empty;
        }

        break;
      case LE:
        if (startOrValue >= maxValue) {
          return all;
        } else if (startOrValue < minValue) {
          return empty;
        }

        break;
      case GT:
        if (startOrValue < minValue) {
          return all;
        } else if (startOrValue >= maxValue) {
          return empty;
        }

        break;
      case GE:
        if (startOrValue <= minValue) {
          return all;
        } else if (startOrValue > maxValue) {
          return empty;
        }

        break;
      case EQ:
        if (minValue == maxValue && minValue == startOrValue) {
          return all;
        } else if (startOrValue < minValue || startOrValue > maxValue) {
          return empty;
        }

        break;
      case NEQ:
        if (minValue == maxValue) {
          return minValue == startOrValue ? empty : all;
        }

        break;
      case RANGE:
        if (startOrValue <= minValue && end >= maxValue) {
          return all;
        } else if (startOrValue > maxValue || end < minValue) {
          return empty;
        }

        break;
      default:
        return null;
    }

    return null;
  }

  public Pair<Long, Long> sum(ImmutableRoaringBitmap foundSet) {
    if (null == foundSet || foundSet.isEmpty()) {
      return Pair.newPair(0L, 0L);
    }
    long count = foundSet.getLongCardinality();

    Long sum = IntStream.range(0, this.bitCount())
        .mapToLong(x -> (long) (1 << x) * ImmutableRoaringBitmap.andCardinality(this.bA[x], foundSet))
        .sum();

    return Pair.newPair(sum, count);
  }

  public List<Pair<Integer, Integer>> toPairList() {
    List<Pair<Integer, Integer>> pairList = new ArrayList<>();
    this.ebM.forEach((IntConsumer) cid -> {
      pairList.add(Pair.newPair(cid, this.getValue(cid).getKey()));
    });
    return pairList;
  }

  public List<Pair<Integer, Integer>> toPairList(ImmutableRoaringBitmap foundSet) {
    List<Pair<Integer, Integer>> pairList = new ArrayList<>();
    ImmutableRoaringBitmap bitmap = ImmutableRoaringBitmap.and(this.ebM, foundSet);
    bitmap.forEach((IntConsumer) cid -> {
      pairList.add(Pair.newPair(cid, this.getValue(cid).getKey()));
    });
    return pairList;
  }

  protected MutableBitSliceIndex transposeWithCount(int[] batch) {
    MutableBitSliceIndex result = new MutableBitSliceIndex();

    for (int columnId : batch) {
      Pair<Integer, Boolean> value = this.getValue(columnId);
      if (value.getValue()) {
        Pair<Integer, Boolean> val = result.getValue(value.getKey());
        if (!val.getValue()) {
          result.setValue(value.getKey(), 1);
        } else {
          int count = val.getKey() + 1;
          result.setValue(value.getKey(), count);
        }
      }
    }
    return result;
  }

  /**
   * TransposeWithCounts is a matrix transpose function that returns a BSI that has a columnID system defined by the values
   * contained within the input BSI.   Given that for BSIs, different columnIDs can have the same value.  TransposeWithCounts
   * is useful for situations where there is a one-to-many relationship between the vectored integer sets.  The resulting BSI
   * contains the number of times a particular value appeared in the input BSI as an integer count.
   *
   * @param foundSet
   * @return the transpose
   */
  public MutableBitSliceIndex parallelTransposeWithCount(ImmutableRoaringBitmap foundSet,
                               int parallelism,
                               ExecutorService pool)
      throws ExecutionException, InterruptedException {

    ImmutableRoaringBitmap fixedFoundSet = foundSet == null ? this.ebM : foundSet;

    Function<int[], MutableBitSliceIndex> func = (int[] batch) -> {
      return transposeWithCount(batch);
    };
    List<CompletableFuture<MutableBitSliceIndex>> futures = parallelExec(func, parallelism, fixedFoundSet, pool);

    allOf(futures);

    MutableBitSliceIndex result = new MutableBitSliceIndex();
    for (CompletableFuture<MutableBitSliceIndex> bsiFuture : futures) {
      result.add((MutableBitSliceIndex) bsiFuture.get());
    }
    return result;
  }

  /**
   * parallelIn search the given Set values, 
   * we scan the bsi,if the value in values, we add it to result Bitmap
   *
   * @param parallelism
   * @param foundSet
   * @param values
   * @param pool
   * @return the computed immutable bitmap
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public ImmutableRoaringBitmap parallelIn(int parallelism,
                       ImmutableRoaringBitmap foundSet,
                       Set<Integer> values,
                       ExecutorService pool
  ) throws ExecutionException, InterruptedException {

    ImmutableRoaringBitmap fixedFoundSet = foundSet == null ? this.ebM : foundSet;

    Function<int[], ImmutableRoaringBitmap> func = (int[] batch) -> {
      return batchIn(batch, values);
    };

    return parallelMR(parallelism, fixedFoundSet, func, pool);

  }


  protected ImmutableRoaringBitmap batchIn(int[] batch, Set<Integer> values) {

    MutableRoaringBitmap result = new MutableRoaringBitmap();

    for (int cID : batch) {
      Pair<Integer, Boolean> value = this.getValue(cID);
      if (value.getValue() && values.contains(value.getKey())) {
        result.add(cID);
      }
    }
    return result;
  }
}

