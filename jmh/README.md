
From the main root of the project, you can run benchmarks as follows:
```
 ./jmh/run.sh org.roaringbitmap.contains
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