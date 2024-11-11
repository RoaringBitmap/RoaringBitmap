package org.roaringbitmap.spe150271.roaring.state;

import static org.roaringbitmap.RealDataset.CENSUS_INCOME;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class RealDataBenchmarkState extends org.roaringbitmap.AbstractBenchmarkState {

  @Param({ // putting the data sets in alpha. order
    CENSUS_INCOME
  })
  public String dataset;

  @Param({ROARING})
  public String type;

  @Param({
    "false",
  })
  public boolean immutable;

  public RealDataBenchmarkState() {}

  @Setup
  public void setup() throws Exception {
    super.setup(dataset, type, immutable);
  }
}
