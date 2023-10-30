package org.roaringbitmap;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsPrepend =
    {
        "-XX:-TieredCompilation",
        "-XX:+UseSerialGC", // "-XX:+UseParallelGC",
        "-mx2G",
        "-ms2G",
        "-XX:+AlwaysPreTouch"
    })
public class AddOffsetBenchmark {
  @Param({
      "ARRAY_ALL_IN_LOW", "ARRAY_HALF_TO_HALF", "ARRAY_ALL_SHIFTED_TO_HIGH",
      // all variant provided for completeness only, original and current implementation are the same
      // "RUN_ALL_IN_LOW", "RUN_HALF_TO_HALF", "RUN_ALL_SHIFTED_TO_HIGH",
      // "BITMAP_ALL_IN_LOW", "BITMAP_HALF_TO_HALF", "BITMAP_ALL_SHIFTED_TO_HIGH",
      // "BITMAP_HALF_TO_HALF_MULTIPLE_OF_LONG", "BITMAP_ALL_SHIFTED_TO_HIGH_MULTIPLE_OF_LONG",
  })
  Scenario scenario;

  @SuppressWarnings("unused")
  public enum Scenario {
    RUN_ALL_IN_LOW(ContainerType.RUN, 0),
    RUN_HALF_TO_HALF(ContainerType.RUN, 35000),
    RUN_ALL_SHIFTED_TO_HIGH(ContainerType.RUN, 60000),

    ARRAY_ALL_IN_LOW(ContainerType.ARRAY, 0),
    ARRAY_HALF_TO_HALF(ContainerType.ARRAY, 35000),
    ARRAY_ALL_SHIFTED_TO_HIGH(ContainerType.ARRAY, 60000),

    BITMAP_ALL_IN_LOW(ContainerType.BITMAP, 0),
    BITMAP_HALF_TO_HALF(ContainerType.BITMAP, 35000),
    BITMAP_ALL_SHIFTED_TO_HIGH(ContainerType.BITMAP, 60000),

    BITMAP_HALF_TO_HALF_MULTIPLE_OF_LONG(ContainerType.BITMAP, 32768 + Long.SIZE * 32), // ~ 35000
    BITMAP_ALL_SHIFTED_TO_HIGH_MULTIPLE_OF_LONG(ContainerType.BITMAP, 65536 - Long.SIZE),
    ;

    private final ContainerType containerType;
    private final char offsets;

    Scenario(ContainerType containerType, int offsets) {
      this.containerType = containerType;
      this.offsets = (char) offsets;
    }

    public ContainerType getContainerType() {
      return containerType;
    }

    public char getOffsets() {
      return offsets;
    }

    public Container createContainer() {
      return containerType.createContainer();
    }
  }

  public enum ContainerType {
    RUN {
      @Override
      public Container createContainer() {
        RunContainer rc = new RunContainer();
        for (int i = 0; i < numberOfValues; i++) {
          rc.add(minValue + i * 2, minValue + i * 2 + 1);
        }
        return rc;
      }
    },
    BITMAP {
      @Override
      public Container createContainer() {
        BitmapContainer bc = new BitmapContainer();
        for (int i = 0; i < numberOfValues; i++) {
          bc.add((char) (minValue + i * 2));
        }
        return bc;
      }
    }, ARRAY {
      @Override
      public Container createContainer() {
        ArrayContainer ac = new ArrayContainer();
        for (int i = 0; i < numberOfValues; i++) {
          ac.add((char) (minValue + i * 2));
        }
        return ac;
      }
    };
    // value = minValue + i * 2 for i < numberOfValues, thus they lies in interval 20000 - 40000
    private static final int minValue = 20_000;
    private static final int numberOfValues = 10000;

    public abstract Container createContainer();
  }

  public Container container;
  public char offsets;


  @Setup(Level.Invocation)
  public void setUp() {
    container = scenario.createContainer();
    offsets = scenario.getOffsets();
  }

  @Benchmark
  public Container[] optimized() {
    return Util.addOffset(container, offsets);
  }

  @Benchmark
  public Container[] optimizedFieldIncrement() {
    if (container instanceof ArrayContainer) {
      // only for comparison with optimized version
      return addOffsetIncrementField((ArrayContainer) container, offsets);
    } else {
      return new Container[0];
    }
  }

  @Benchmark
  public Container[] original() {
    return addOffsetOriginal(container, offsets);
  }

  @SuppressWarnings({"RedundantCast", "PointlessArithmeticExpression", "GrazieInspection"})
  public static Container[] addOffsetOriginal(Container source, char offsets) {
    // could be a whole lot faster, this is a simple implementation
    if (source instanceof ArrayContainer) {
      ArrayContainer c = (ArrayContainer) source;
      ArrayContainer low = new ArrayContainer(c.cardinality);
      ArrayContainer high = new ArrayContainer(c.cardinality);
      for (int k = 0; k < c.cardinality; k++) {
        int val = c.content[k];
        val += (int) (offsets);
        if (val <= 0xFFFF) {
          low.content[low.cardinality++] = (char) val;
        } else {
          high.content[high.cardinality++] = (char) val;
        }
      }
      return new Container[]{low, high};
    } else if (source instanceof BitmapContainer) {
      BitmapContainer c = (BitmapContainer) source;
      BitmapContainer low = new BitmapContainer();
      BitmapContainer high = new BitmapContainer();
      low.cardinality = -1;
      high.cardinality = -1;
      final int b = (int) (offsets) >>> 6;
      final int i = (int) (offsets) % 64;
      if (i == 0) {
        System.arraycopy(c.bitmap, 0, low.bitmap, b, 1024 - b);
        System.arraycopy(c.bitmap, 1024 - b, high.bitmap, 0, b);
      } else {
        low.bitmap[b + 0] = c.bitmap[0] << i;
        for (int k = 1; k < 1024 - b; k++) {
          low.bitmap[b + k] = (c.bitmap[k] << i) | (c.bitmap[k - 1] >>> (64 - i));
        }
        for (int k = 1024 - b; k < 1024; k++) {
          high.bitmap[k - (1024 - b)] =
              (c.bitmap[k] << i)
                  | (c.bitmap[k - 1] >>> (64 - i));
        }
        high.bitmap[b] = (c.bitmap[1024 - 1] >>> (64 - i));
      }
      return new Container[]{low.repairAfterLazy(), high.repairAfterLazy()};
    } else if (source instanceof RunContainer) {
      RunContainer input = (RunContainer) source;
      RunContainer low = new RunContainer();
      RunContainer high = new RunContainer();
      for (int k = 0; k < input.nbrruns; k++) {
        int val = (input.getValue(k));
        val += (int) (offsets);
        int finalval = val + (input.getLength(k));
        if (val <= 0xFFFF) {
          if (finalval <= 0xFFFF) {
            low.smartAppend((char) val, input.getLength(k));
          } else {
            low.smartAppend((char) val, (char) (0xFFFF - val));
            high.smartAppend((char) 0, (char) finalval);
          }
        } else {
          high.smartAppend((char) val, input.getLength(k));
        }
      }
      return new Container[]{low, high};
    }
    throw new RuntimeException("unknown container type"); // never happens
  }

  private static Container[] addOffsetIncrementField(ArrayContainer source, char offsets) {
    ArrayContainer low;
    ArrayContainer high;
    if (source.first() + offsets > 0xFFFF) {
      low = new ArrayContainer();
      high = new ArrayContainer(source.cardinality);
      for (int k = 0; k < source.cardinality; k++) {
        int val = source.content[k] + offsets;
        high.content[k] = (char) val;
      }
      high.cardinality = source.cardinality;
    } else if (source.last() + offsets < 0xFFFF) {
      low = new ArrayContainer(source.cardinality);
      high = new ArrayContainer();
      for (int k = 0; k < source.cardinality; k++) {
        int val = source.content[k] + offsets;
        low.content[k] = (char) val;
      }
      low.cardinality = source.cardinality;
    } else {
      int splitIndex = Util.unsignedBinarySearch(source.content, 0, source.cardinality,
          (char) ~offsets);
      if (splitIndex < 0) {
        splitIndex = -splitIndex - 1;
      }
      low = new ArrayContainer(splitIndex);
      high = new ArrayContainer(source.cardinality - splitIndex);
      for (int k = 0; k < splitIndex; k++) {
        int val = source.content[k] + offsets;
        low.content[low.cardinality++] = (char) val;
      }
      for (int k = splitIndex; k < source.cardinality; k++) {
        int val = source.content[k] + offsets;
        high.content[high.cardinality++] = (char) val;
      }
    }
    return new Container[]{low, high};
  }
}