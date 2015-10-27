package org.roaringbitmap.realdata;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.roaringbitmap.realdata.state.RealDataBenchmarkState;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.roaringbitmap.RealDataset.*;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.*;

@RunWith(Parameterized.class)
public abstract class RealDataBenchmarkSanityTest {

    public static final String[] REAL_DATA_SETS = { CENSUS_INCOME, CENSUS1881,
                                                    DIMENSION_008, DIMENSION_003,
                                                    DIMENSION_033, USCENSUS2000,
                                                    WEATHER_SEPT_85, WIKILEAKS_NOQUOTES,
                                                    CENSUS_INCOME_SRT, CENSUS1881_SRT,
                                                    WEATHER_SEPT_85_SRT, WIKILEAKS_NOQUOTES_SRT };

    public static final String[] BITMAP_TYPES = { CONCISE, WAH,
                                                  EWAH, EWAH32,
                                                  ROARING, ROARING_WITH_RUN };

    public static final Boolean[] BITMAP_IMMUTABILITY = { false, true };


    @SuppressWarnings("unchecked")
    @Parameterized.Parameters(name = "dataset={0}, type={1}, immutable={2}")
    public static Collection<Object[]> params() {
        Set params = Sets.cartesianProduct(
                Sets.newLinkedHashSet(FluentIterable.of(REAL_DATA_SETS)),
                Sets.newLinkedHashSet(FluentIterable.of(BITMAP_TYPES)),
                Sets.newLinkedHashSet(FluentIterable.of(BITMAP_IMMUTABILITY)));
        return FluentIterable.from(params)
                .transform(new Function<List, Object[]>() {
                    @Override
                    public Object[] apply(List list) {
                        return list.toArray(new Object[] { list.size() });
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
        if(bs!=null) {
            bs.tearDown();
        }
    }

}
