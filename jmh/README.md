## Usage

Try:

     $ ./run.sh RunContainerRealDataBenchmark

for a large, long running set of in-memory benchmarks.

You can select one particular benchmark from this list :
     $ls src/main/java/org/roaringbitmap/runcontainer/RunContainerRealDataBenchmark*

For a large, long running set of memory-mapped benchmarks, 
try :

     $./run.sh MappedRunContainerRealDataBenchmark

For specific queries, try :

     $ ls src/main/java/org/roaringbitmap/runcontainer/MappedRunContainerRealDataBenchmark*

Here is an example: 

     $ ./run.sh RunContainerRealDataBenchmarkIterate > iteratelog.txt
     $ ./run.sh RunContainerRealDataBenchmarkAnd > andlog.txt
     $ ./run.sh RunContainerRealDataBenchmarkOr > orlog.txt

See samplescript.sh for a more elaborate example.

You can then parse files with a script:

     $ ./grabresults.sh horizontallog.txt


## Need work


There are some cases where we get results that we believe are underwhelming, meaning that
it should be possible to double the performance or more. You can run these benchmarks as:

     $ ./run.sh org.roaringbitmap.needwork

or, more simply, as 

     $ ./run.sh needwork
