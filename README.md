RoaringBitmap
=============

This is a first prototype of a new format for (compressed) bitmap indexes. 
The goal is to beat JavaEWAH and other alternatives, at least some of the time.

Usage:

* Get java
* Get maven 2

* mvn compile will compile
* mvn test will run the unit tests
* mvn package will package in a jar (found in target)
* mvn exec:java will run a benchmark

To run a benchmark manually, do

* mvn package (you may use mvn -Dmaven.test.skip=true package)
* cd target
* java -cp RoaringBitmap-0.0.1-SNAPSHOT.jar:lib/* org.roaringbitmap.experiments.colantonio.Benchmark

Some benchmarks can use  SizeOf.jar for memory usage estimation. Try:
* mvn package (you may use mvn -Dmaven.test.skip=true package)
* cd target
* java -javaagent:lib/SizeOf.jar -cp RoaringBitmap-0.0.1-SNAPSHOT.jar:lib/* org.roaringbitmap.experiments.colantonio.Benchmark


Note: Eclipse supports maven projects (though a plugin might be required)
