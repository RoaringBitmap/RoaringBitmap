package org.roaringbitmap.needwork.state;

import static org.roaringbitmap.RealDataset.CENSUS1881;
import static org.roaringbitmap.RealDataset.CENSUS1881_SRT;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME_SRT;
import static org.roaringbitmap.RealDataset.DIMENSION_003;
import static org.roaringbitmap.RealDataset.DIMENSION_008;
import static org.roaringbitmap.RealDataset.DIMENSION_033;
import static org.roaringbitmap.RealDataset.USCENSUS2000;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85_SRT;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES_SRT;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING_WITH_RUN;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.AbstractBenchmarkState;

@State(Scope.Benchmark)
public class NeedWorkBenchmarkState extends AbstractBenchmarkState {

  @Param({// putting the data sets in alpha. order
      CENSUS_INCOME, CENSUS1881, DIMENSION_008, DIMENSION_003, DIMENSION_033, USCENSUS2000,
      WEATHER_SEPT_85, WIKILEAKS_NOQUOTES, CENSUS_INCOME_SRT, CENSUS1881_SRT, WEATHER_SEPT_85_SRT,
      WIKILEAKS_NOQUOTES_SRT})
  public String dataset;

  @Param({ROARING, ROARING_WITH_RUN})
  public String type;

  @Param({"false", "true"})
  public boolean immutable;


  public NeedWorkBenchmarkState() {}

  @Setup
  public void setup() throws Exception {
    super.setup(dataset, type, immutable);
  }

}
