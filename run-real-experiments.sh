N_EXPER=2  #we have enough data to go up to 100
for ds in census1881.csv census-income.csv wikileaks-noquotes.csv uscensus2000.csv weather_large_withjan82.csv ; do
    java -cp ~/lemur-git/RoaringBitmap/target/RoaringBitmap-0.0.1-SNAPSHOT.jar:$CLASSPATH -javaagent:/home/owen/lemur-git/RoaringBitmap/lib/SizeOf.jar me.lemire.roaringbitmap.experiments.BenchmarkReal real-roaring-datasets $ds $N_EXPER
done



