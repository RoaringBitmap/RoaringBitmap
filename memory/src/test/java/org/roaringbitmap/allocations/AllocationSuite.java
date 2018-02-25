package org.roaringbitmap.allocations;

import com.google.monitoring.runtime.instrumentation.AllocationRecorder;
import com.google.monitoring.runtime.instrumentation.Sampler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public abstract class AllocationSuite {

  protected static void printAllocations(Runnable block) {
    instrument(block, AllocationsStatsSampler::print);
  }

  protected static void nop(Runnable block) {
    instrument(block, result -> {
    });
  }

  protected static void instrument(Runnable block, Consumer<Map<String, AllocationStats>> result) {
    AllocationsStatsSampler s = new AllocationsStatsSampler();
    AllocationRecorder.addSampler(s);
    block.run();
    AllocationRecorder.removeSampler(s);
    s.clearNoise();
    result.accept(s.getAllocations());
  }
}


class AllocationsStatsSampler implements Sampler {
  Map<String, AllocationStats> getAllocations() {
    return allocations;
  }

  private Map<String, AllocationStats> allocations = new HashMap<>(AVOID_ALLOCATIONS_CAPACITY);

  @Override
  public void sampleAllocation(int count, String desc, Object newObj, long size) {
    String key = (count != -1) ? desc + "[]" : desc;
    AllocationStats stats = allocations.getOrDefault(key, new AllocationStats());
    stats.countAllocation(size);
    allocations.put(key, stats);
  }

  void clearNoise() {
    final String trash = "com/google/monitoring/runtime/instrumentation/Sampler[]";
    final String testIterators = "java/util/ArrayList$Itr";
    final String classNameTrash = "java/lang/String";
    final String classNameTrash2 = "char[]";
    allocations.remove(testIterators);
    allocations.remove(trash);
    // it is safe, this is instrumenter class names generation
    allocations.remove(classNameTrash);
    // roaring is not allocating strings
    allocations.remove(classNameTrash2);
  }

  static void print(Map<String, AllocationStats> stats) {
    stats.forEach((clazz, s) ->
        System.out.printf("%-40s %s\n", clazz, s));
    AllocationStats total = stats.values().stream().reduce(new AllocationStats(), AllocationStats::merge);
    System.out.printf("%-40s %s\n", "Total", total);
    System.out.println();
  }

  private static int AVOID_ALLOCATIONS_CAPACITY = 10_000;
}


