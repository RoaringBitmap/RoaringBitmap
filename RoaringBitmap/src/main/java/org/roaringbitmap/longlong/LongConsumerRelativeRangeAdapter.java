package org.roaringbitmap.longlong;

import org.roaringbitmap.RelativeRangeConsumer;

/**
 * Wrapper to use a LongConsumer where a RelativeRangeConsumer is expected.
 */
public class LongConsumerRelativeRangeAdapter implements RelativeRangeConsumer {
  final long start;
  final LongConsumer absolutePositionConsumer;

  public LongConsumerRelativeRangeAdapter(long start, final LongConsumer lc) {
    this.start = start;
    this.absolutePositionConsumer = lc;
  }

  @Override
  public void acceptPresent(int relativePos) {
    absolutePositionConsumer.accept(start + relativePos);
  }

  @Override
  public void acceptAbsent(int relativePos) {
    // nothing to do
  }

  @Override
  public void acceptAllPresent(int relativeFrom, int relativeTo) {
    for (long pos = start + relativeFrom; pos < start + relativeTo; pos++) {
      absolutePositionConsumer.accept(pos);
    }
  }

  @Override
  public void acceptAllAbsent(int relativeFrom, int relativeTo) {
    // nothing to do
  }
}
