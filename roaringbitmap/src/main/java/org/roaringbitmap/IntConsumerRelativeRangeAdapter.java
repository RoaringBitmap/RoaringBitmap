package org.roaringbitmap;

/**
 * Wrapper to use an IntConsumer where a RelativeRangeConsumer is expected.
 */
public class IntConsumerRelativeRangeAdapter implements RelativeRangeConsumer {
  final int start;
  final IntConsumer absolutePositionConsumer;

  public IntConsumerRelativeRangeAdapter(int start, final IntConsumer lc) {
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
    for (int pos = start + relativeFrom; pos < start + relativeTo; pos++) {
      absolutePositionConsumer.accept(pos);
    }
  }

  @Override
  public void acceptAllAbsent(int relativeFrom, int relativeTo) {
    // nothing to do
  }
}
