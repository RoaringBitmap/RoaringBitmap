RoaringBitmap
=============

This is a first prototype of a new format for (compressed) bitmap indexes. 
The goal is to beat JavaEWAH and other alternatives, at least some of the time.

Usage:

Get java
Get maven 2

mvn compile will compile
mvn test will run the unit tests
mvn package will package in a jar (found in target)
mvn exec:java will run a benchmark

Pour lancer un benchmark manuellement, faire  
mvn package
cd target
java -cp "RoaringBitmap-0.0.1-SNAPSHOT.jar:lib/*" me.lemire.roaringbitmap.experiments.Benchmark


Note: Eclipse supports maven projects (though a plugin might be required)
