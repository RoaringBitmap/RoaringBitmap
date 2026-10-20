
From the main root of the project, you can run benchmarks as follows:
```
 ./jmh/run.sh org.roaringbitmap.contains
 ```

### Persistent bitmaps

`PersistentRoaringBitmapBenchmark` compares the binary operations of `PersistentRoaringBitmap`,
which share the containers that pass through, with those of `RoaringBitmap`, which copy them. Its
`...ThenWrite...` cases and `persistentOperandThenWrite` charge the copies that sharing defers to
later writes, and `persistentOrOfPlainBitmaps` charges the conversions. `-prof gc` reports the
allocation per operation:
```
 ./jmh/run.sh 'org.roaringbitmap.PersistentRoaringBitmapBenchmark' -prof gc
 ```
