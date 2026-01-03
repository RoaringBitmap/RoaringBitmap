
From the main root of the project, you can run benchmarks as follows:
```
./gradlew :jmh:shadowJar
java --add-modules jdk.incubator.vector \
  -jar jmh/build/libs/benchmarks.jar \
  'org.roaringbitmap.realdata.RoaringBenchmark.*' \
  -rf JSON -rff jmh/build/reports/jmh/results.json
 ```


# To run the quick test
The following command to compile the cose
```
./gradlew :jmh:shadowJar
```

The following command to run the test
```
java --add-modules jdk.incubator.vector \
  -jar jmh/build/libs/benchmarks.jar \
  'org.roaringbitmap.realdata.RoaringBenchmarkQuick.*' \
  -p maxBitmaps=96
```

# To run the complete test

Output in consolle
```
./gradlew :jmh:shadowJar
java --add-modules jdk.incubator.vector \
  -jar jmh/build/libs/benchmarks.jar \
  'org.roaringbitmap.realdata.RoaringBenchmark.*' \
  -rf JSON -rff jmh/build/reports/jmh/results.json
```

Output su json
```
./gradlew :jmh:shadowJar
java --add-modules jdk.incubator.vector \
  -jar jmh/build/libs/benchmarks.jar \
  'org.roaringbitmap.realdata.RoaringBenchmarkQuick.*' \
  -rf JSON -rff jmh/build/reports/jmh/results.json
```