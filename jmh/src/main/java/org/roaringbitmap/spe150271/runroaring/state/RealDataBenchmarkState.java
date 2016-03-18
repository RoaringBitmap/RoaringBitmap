package org.roaringbitmap.spe150271.runroaring.state;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import static org.roaringbitmap.RealDataset.*;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.*;

@State(Scope.Benchmark)
public class RealDataBenchmarkState extends org.roaringbitmap.AbstractBenchmarkState {

   @Param({// putting the data sets in alpha. order
           CENSUS_INCOME
   })
   public String dataset;

   @Param({
           ROARING_WITH_RUN
   })
   public String type;

   @Param({
           "false",
   })
   public boolean immutable;


   public RealDataBenchmarkState() {
   }

   @Setup
   public void setup() throws Exception {
       super.setup(dataset, type, immutable);
   }

}
