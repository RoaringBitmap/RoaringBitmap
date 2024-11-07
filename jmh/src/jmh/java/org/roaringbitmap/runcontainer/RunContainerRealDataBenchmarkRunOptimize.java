package org.roaringbitmap.runcontainer;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RunContainerRealDataBenchmarkRunOptimize {

  @Benchmark
  public int clone(BenchmarkState benchmarkState) {
    int total = 0;
    for (int i = 0; i < benchmarkState.ac.size(); i++) {
      RoaringBitmap bitmap = benchmarkState.ac.get(i).clone();
      total += bitmap.getCardinality();
    }
    return total;
  }

  @Benchmark
  public int cloneAndrunOptimize(BenchmarkState benchmarkState) {
    int total = 0;
    for (int i = 0; i < benchmarkState.ac.size(); i++) {
      RoaringBitmap bitmap = benchmarkState.ac.get(i).clone();
      bitmap.runOptimize();
      total += bitmap.getCardinality();
    }
    return total;
  }

  @Benchmark
  public int serializeToBAOSFromClone(BenchmarkState benchmarkState) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    for (int i = 0; i < benchmarkState.ac.size(); i++) {
      RoaringBitmap bitmap = benchmarkState.ac.get(i).clone();
      bitmap.serialize(dos);
    }
    dos.flush();
    return bos.size();
  }

  @Benchmark
  public int serializeToBAOSNoClone(BenchmarkState benchmarkState) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    for (int i = 0; i < benchmarkState.ac.size(); i++) {
      RoaringBitmap bitmap = benchmarkState.ac.get(i);
      bitmap.serialize(dos);
    }
    dos.flush();
    return bos.size();
  }

  @Benchmark
  public int serializeToBAOSFromClonePreOpti(BenchmarkState benchmarkState) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    for (int i = 0; i < benchmarkState.rc.size(); i++) {
      RoaringBitmap bitmap = benchmarkState.rc.get(i).clone();
      bitmap.serialize(dos);
    }
    dos.flush();
    return bos.size();
  }

  @Benchmark
  public int serializeToBAOSNoClonePreOpti(BenchmarkState benchmarkState) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    for (int i = 0; i < benchmarkState.rc.size(); i++) {
      RoaringBitmap bitmap = benchmarkState.rc.get(i);
      bitmap.serialize(dos);
    }
    dos.flush();
    return bos.size();
  }

  @Benchmark
  public int runOptimizeAndserializeToBAOSFromClone(BenchmarkState benchmarkState)
      throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    for (int i = 0; i < benchmarkState.ac.size(); i++) {
      RoaringBitmap bitmap = benchmarkState.ac.get(i).clone();
      bitmap.runOptimize();
      bitmap.serialize(dos);
    }
    dos.flush();
    return bos.size();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({ // putting the data sets in alpha. order
      "census-income",
      "census1881",
      "dimension_008",
      "dimension_003",
      "dimension_033",
      "uscensus2000",
      "weather_sept_85",
      "wikileaks-noquotes",
      "census-income_srt",
      "census1881_srt",
      "weather_sept_85_srt",
      "wikileaks-noquotes_srt"
    })
    String dataset;

    ArrayList<RoaringBitmap> ac = new ArrayList<RoaringBitmap>();
    ArrayList<RoaringBitmap> rc = new ArrayList<RoaringBitmap>();

    public BenchmarkState() {}

    @Setup
    public void setup() throws Exception {
      ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
      System.out.println();
      System.out.println("Loading files from " + dataRetriever.getName());

      for (int[] data : dataRetriever.fetchBitPositions()) {
        RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
        ac.add(basic);
        RoaringBitmap opti = basic.clone();
        opti.runOptimize();
        rc.add(opti);
      }
    }
  }
}
