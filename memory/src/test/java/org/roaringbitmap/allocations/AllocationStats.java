package org.roaringbitmap.allocations;

public class AllocationStats {
  private int allocations;
  private long bytes;

  void countAllocation(long bytes) {
    this.allocations += 1;
    this.bytes += bytes;
  }

  public int getAllocations() {
    return allocations;
  }

  public long getTotalBytes() {
    return bytes;
  }

  public static AllocationStats merge(AllocationStats l, AllocationStats r) {
    AllocationStats result = new AllocationStats();
    result.allocations = l.allocations + r.allocations;
    result.bytes = l.bytes + r.bytes;
    return result;
  }

  @Override
  public String toString() {
    return "AllocationStats(allocations=" + allocations + ", bytes=" + bytes + ')';
  }
}
