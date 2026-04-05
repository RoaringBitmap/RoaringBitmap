package org.roaringbitmap.longlong;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapSupplier;
import org.roaringbitmap.TestAdversarialInputs;
import org.roaringbitmap.Util;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmapSupplier;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.longlong.TestRoaring64Bitmap.getSourceForAllKindsOfNodeTypes;

public class TestRoaring64NavigableMap_concurrentReads {
  // number of threads
  int concurrency = 1024;
  // number of tasks per threads
  int taskMultiplicator = 128;
  // This should be increased to a large value, as each run has one change to trigger a race-condition
  int nbRuns = 1;
  @Test
  public void testConcurrencyIsEmpty_manyRuns() throws InterruptedException {
    for (int i = 0; i < nbRuns; i++) {
      System.out.println("Run #" + i);
      oneRunConcurrencyIsEmpty();
    }
  }

  private void oneRunConcurrencyIsEmpty() throws InterruptedException {
    Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);

    // Write values for multiple highs, so that the cache array has size > 1
    bitmap.add(170L);
    for (int i = 0; i < 500; i++) {
      bitmap.add(170L + i * (long) Integer.MAX_VALUE);
    }

    List<Callable<Object>> tasks = new ArrayList<>();
    for (int i = 0; i < taskMultiplicator * concurrency; i++) {
      tasks.add(() -> {
        unsafeMethod(bitmap);
        return null;
      });
    }

    List<? extends Future<?>> futures = executor.invokeAll(tasks);

    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IllegalArgumentException(e);
      }
    }

    executor.shutdown();
  }

  private void unsafeMethod(Roaring64NavigableMap bitmap) {
    bitmap.isEmpty();
    bitmap.getLongCardinality();
    bitmap.rankLong(2L * (long) Integer.MAX_VALUE);
  }
}
