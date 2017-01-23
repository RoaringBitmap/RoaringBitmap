## Running just one test without script

        mvn -f ../pom.xml clean install -DskipTests -Dgpg.skip=true
        mvn -f ../real-roaring-dataset/pom.xml clean install
        mvn  clean install  -DskipTests
        java -jar ./target/benchmarks.jar true -wi 5 -i 5 -f1 IteratorsBenchmark


## Focusing on just roaring bitmaps


You can simply execute the appropriate script (Mac/Linux) : 
    $ ./roaringbenchmark.sh

## Generic  Usage

Try:

     $ ./run.sh org.roaringbitmap.realdata

for a large, long running set of in-memory benchmarks. (Windows
	users might want to use the run.bat script instead.)

You can select one particular benchmark from this list :

     $ ls src/main/java/org/roaringbitmap/realdata

See samplescript.sh for a more elaborate example.

You can then parse files with the grabresults.sh script if you want:

     $ ./grabresults.sh horizontallog.txt

## Need work

There are some cases where we get results that we believed were underwhelming, meaning that
at some point, we thought it should be possible to double the performance or more. You can run these benchmarks as:

     $ ./run.sh org.roaringbitmap.needwork

or, more simply, as

     $ ./run.sh needwork


We can analyze the result with JMC.

     $ java -jar target/benchmarks.jar needwork.SlowORaggregate3 -wi 5 -i 5 -f 1 -jvmArgs="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=duration=60s,filename=SlowORaggregate.jfr"
     $ jmc SlowORaggregate.jfr

# spe150271

For the manuscript Consistently faster and smaller compressed bitmaps with Roaring, we used a script (spe150271.sh) to record performance data.
