package org.roaringbitmap;

import com.carrotsearch.junitbenchmarks.IResultsConsumer;
import com.carrotsearch.junitbenchmarks.WriterConsumer;
import com.carrotsearch.junitbenchmarks.h2.H2Consumer;

import java.io.File;

/**
 * This class is used to initialize results consumers required by junitbenchmarks.
 */
public class BenchmarkConsumers {

    public static final IResultsConsumer CONSOLE_CONSUMER = new WriterConsumer();
    public static final IResultsConsumer H2_CONSUMER = new H2Consumer(new File("PerformanceBenchmarks"));

}
