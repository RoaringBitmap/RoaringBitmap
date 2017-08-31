package org.roaringbitmap.realdata;

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
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.CONCISE;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.EWAH;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.EWAH32;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING_ONLY;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING_WITH_RUN;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.WAH;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.roaringbitmap.realdata.state.RealDataBenchmarkState;
import org.roaringbitmap.realdata.wrapper.BitmapFactory;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public abstract class RealDataBenchmarkSanityTest {

  public static final String[] REAL_DATA_SETS = {CENSUS_INCOME, CENSUS1881, DIMENSION_008,
      DIMENSION_003, DIMENSION_033, USCENSUS2000, WEATHER_SEPT_85, WIKILEAKS_NOQUOTES,
      CENSUS_INCOME_SRT, CENSUS1881_SRT, WEATHER_SEPT_85_SRT, WIKILEAKS_NOQUOTES_SRT};

  public static final String[] BITMAP_TYPES =
      ROARING_ONLY.equals(System.getProperty(BitmapFactory.BITMAP_TYPES))
          ? new String[] {ROARING, ROARING_WITH_RUN}
          : new String[] {CONCISE, WAH, EWAH, EWAH32, ROARING, ROARING_WITH_RUN};

  public static final Boolean[] BITMAP_IMMUTABILITY = {false, true};


  // Ensure all tests related to the same dataset are run consecutively in order to take advantage
  // of any cache
  @SuppressWarnings("unchecked")
  @Parameterized.Parameters(name = "dataset={0}, type={1}, immutable={2}")
  public static Collection<Object[]> params() {
    Set<? extends List<?>> params =
        Sets.cartesianProduct(Sets.newLinkedHashSet(FluentIterable.of(REAL_DATA_SETS)),
            Sets.newLinkedHashSet(FluentIterable.of(BITMAP_TYPES)),
            Sets.newLinkedHashSet(FluentIterable.of(BITMAP_IMMUTABILITY)));
    return FluentIterable.from(params).transform(new Function<List<?>, Object[]>() {
      @Override
      public Object[] apply(List<?> list) {
        return list.toArray(new Object[] {list.size()});
      }
    }).toList();
  }

  @Parameterized.Parameter(value = 0)
  public String dataset;

  @Parameterized.Parameter(value = 1)
  public String type;

  @Parameterized.Parameter(value = 2)
  public boolean immutable;


  protected RealDataBenchmarkState bs;

  @Before
  public void setup() throws Exception {
    bs = new RealDataBenchmarkState();
    bs.dataset = dataset;
    bs.type = type;
    bs.immutable = immutable;
    bs.setup();
  }

  @After
  public void tearDown() throws Exception {
    if (bs != null) {
      bs.tearDown();
    }
  }

}
