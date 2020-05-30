package org.roaringbitmap;

import org.roaringbitmap.buffer.*;

import java.util.function.Supplier;

public interface RoaringBitmapWriter<T extends BitmapDataProvider> extends Supplier<T> {

  static Wizard<Container, RoaringBitmap> writer() {
    return new RoaringBitmapWizard();
  }

  static Wizard<MappeableContainer, MutableRoaringBitmap> bufferWriter() {
    return new BufferWizard();
  }

  abstract class Wizard<C extends WordStorage<C>,
          T extends BitmapDataProvider & AppendableStorage<C>>
          implements Supplier<RoaringBitmapWriter<T>> {

    protected int initialCapacity = RoaringArray.INITIAL_CAPACITY;
    protected boolean constantMemory;
    protected boolean partiallySortValues = false;
    protected boolean runCompress = true;
    protected Supplier<C> containerSupplier;
    protected int expectedContainerSize = 16;

    Wizard() {
      containerSupplier = arraySupplier();
    }

    /**
     * Choose this option if it is known that most containers will be sparse.
     * @return this
     */
    public Wizard<C, T> optimiseForArrays() {
      containerSupplier = arraySupplier();
      return this;
    }

    /**
     * Choose this option if the bitmap is expected to be RLE compressible.
     * Will buffer additions into a RunContainer.
     * @return this
     */
    public Wizard<C, T> optimiseForRuns() {
      containerSupplier = runSupplier();
      return this;
    }

    /**
     * By default the bitmap will be run-compressed on the fly,
     * but it can be disabled (and run compressed at the end).
     * @param runCompress whether to apply run compression on the fly.
     * @return this
     */
    public Wizard<C, T> runCompress(boolean runCompress) {
      this.runCompress = runCompress;
      return this;
    }

    /**
     *
     * @param count how many values are expected to fall within any 65536 bit range.
     * @return this
     */
    public Wizard<C, T> expectedValuesPerContainer(int count) {
      sanityCheck(count);
      this.expectedContainerSize = count;
      if (count < ArrayContainer.DEFAULT_MAX_SIZE) {
        return optimiseForArrays();
      } else if (count < 1 << 14) {
        return constantMemory();
      } else {
        return optimiseForRuns();
      }
    }

    public Wizard<Container, FastRankRoaringBitmap> fastRank() {
      throw new IllegalStateException("Fast rank not yet implemented for byte buffers");
    }

    /**
     * All writes are buffered into the same buffer of 8kB, before converting to
     * the best container representation and appending to the bitmap.
     * This option overrides any optimiseForArrays, optimiseForRuns and optimiseForBitmaps settings.
     * @return this
     */
    public Wizard<C, T> constantMemory() {
      constantMemory = true;
      return this;
    }

    /**
     * Influence default container choice by how dense the bitmap is expected to be.
     * @param density value in [0.0, 1.0], density of the bitmap
     * @return this
     */
    public Wizard<C, T> expectedDensity(double density) {
      return expectedValuesPerContainer((int) (0xFFFF * density));
    }

    /**
     * Guesses the number of prefices required based on an estimate of the range the bitmap
     * will contain, assumes that all prefices in the range will be required. This is a good
     * heuristic for a contiguous bitmap, and, for instance, a very bad heuristic for a bitmap
     * with just two values far apart.
     * @param min the inclusive min value
     * @param max the exclusive max value
     * @return this
     */
    public Wizard<C, T> expectedRange(long min, long max) {
      return initialCapacity((int) ((max - min) >>> 16) + 1);
    }

    /**
     * Takes control of the size of the prefix array, in case it can be precalculated
     * or estimated. This can potentially save many array allocations during building
     * the bitmap.
     * @param count an estimate of the number of prefix keys required.
     * @return this
     */
    public Wizard<C, T> initialCapacity(int count) {
      sanityCheck(count);
      initialCapacity = count;
      return this;
    }

    /**
     * Will partially sort values, which can allocate O(n) temporary
     * memory but can significantly speed up adding unsorted values
     * to a bitmap.
     * @return this
     */
    public Wizard<C, T> doPartialRadixSort() {
      partiallySortValues = true;
      return this;
    }

    protected abstract Supplier<C> arraySupplier();

    protected abstract Supplier<C> runSupplier();

    protected abstract T createUnderlying(int initialCapacity);

    /**
     * Builds a bitmap writer based on the supplied options.
     * A call to this method is repeatable, and will not fail because the wizard
     * should already be in a valid state.
     * @return a new RoaringBitmapWriter
     */
    @Override
    public RoaringBitmapWriter<T> get() {
      int capacity = initialCapacity;
      return new ContainerAppender<>(partiallySortValues, runCompress,
          () -> createUnderlying(capacity), containerSupplier);
    }

    private static void sanityCheck(int count) {
      if (count >= 0xFFFF) {
        throw new IllegalArgumentException(count + " > 65536");
      }
      if (count < 0) {
        throw new IllegalArgumentException(count + " < 0");
      }
    }
  }

  class BufferWizard extends Wizard<MappeableContainer, MutableRoaringBitmap> {

    @Override
    protected Supplier<MappeableContainer> arraySupplier() {
      int size = expectedContainerSize;
      return () -> new MappeableArrayContainer(size);
    }

    @Override
    protected Supplier<MappeableContainer> runSupplier() {
      return MappeableRunContainer::new;
    }

    @Override
    protected MutableRoaringBitmap createUnderlying(int initialCapacity) {
      return new MutableRoaringBitmap(new MutableRoaringArray(initialCapacity));
    }
  }

  abstract class RoaringWizard<T extends RoaringBitmap> extends Wizard<Container, T> {

    @Override
    protected Supplier<Container> arraySupplier() {
      int size = expectedContainerSize;
      return () -> new ArrayContainer(size);
    }

    @Override
    protected Supplier<Container> runSupplier() {
      return RunContainer::new;
    }

    @Override
    public Wizard<Container, FastRankRoaringBitmap> fastRank() {
      return new FastRankRoaringBitmapWizard(this);
    }

    @Override
    public RoaringBitmapWriter<T> get() {
      if (constantMemory) {
        int capacity = initialCapacity;
        return new ConstantMemoryContainerAppender<>(
                partiallySortValues, runCompress, () -> createUnderlying(capacity));
      }
      return super.get();
    }
  }

  class FastRankRoaringBitmapWizard extends RoaringWizard<FastRankRoaringBitmap> {

    FastRankRoaringBitmapWizard(Wizard<Container, ? extends RoaringBitmap> wizard) {
      this.constantMemory = wizard.constantMemory;
      this.initialCapacity = wizard.initialCapacity;
      this.containerSupplier = wizard.containerSupplier;
      this.partiallySortValues = wizard.partiallySortValues;
    }

    @Override
    protected FastRankRoaringBitmap createUnderlying(int initialCapacity) {
      return new FastRankRoaringBitmap(new RoaringArray(initialCapacity));
    }
  }

  class RoaringBitmapWizard extends RoaringWizard<RoaringBitmap> {

    @Override
    protected RoaringBitmap createUnderlying(int initialCapacity) {
      return new RoaringBitmap(new RoaringArray(initialCapacity));
    }
  }

  /**
   * Gets the bitmap being written to.
   * @return the bitmap
   */
  T getUnderlying();

  /**
   * buffers a value to be added to the bitmap.
   * @param value the value
   */
  void add(int value);

  /**
   * Add a range to the bitmap
   * @param min the inclusive min value
   * @param max the exclusive max value
   */
  void add(long min, long max);

  /**
   * Adds many values to the bitmap.
   * @param values the values to add
   *
   */
  void addMany(int... values);

  /**
   * Flushes all pending changes to the bitmap.
   */
  void flush();

  /**
   * flushes any pending changes to the bitmap and returns the bitmap
   * @return the underlying bitmap
   */
  default T get() {
    flush();
    return getUnderlying();
  }

  /**
   * Resets the writer so it can be reused, must release the reference to the underlying bitmap
   */
  void reset();
}
