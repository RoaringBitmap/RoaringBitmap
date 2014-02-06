N_EXPER=100  #we have enough data to go up to 100

sudo /home/owen/bin/cpuscaling.sh performance    #avoid on demand stuff
echo turbo boost status:
turboboost.sh off
#full weather lead to crash   weather_large_withjan82.csv 

for ds in census1881.csv census-income.csv wikileaks-noquotes.csv uscensus2000.csv weather_sept_85.csv ; do
    java -cp ~/lemur-git/RoaringBitmap/target/RoaringBitmap-0.0.1-SNAPSHOT.jar:$CLASSPATH -javaagent:/home/owen/lemur-git/RoaringBitmap/lib/SizeOf.jar me.lemire.roaringbitmap.experiments.BenchmarkReal real-roaring-datasets $ds $N_EXPER
done



