## Usage

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
