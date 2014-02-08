N_EXPER=100  #we have enough data to go up to 100
mvn package -Dmaven.test.skip=true
echo turbo boost status:

for ds in census1881.csv census-income.csv wikileaks-noquotes.csv uscensus2000.csv weather_sept_85.csv; do
    java -cp ./target/RoaringBitmap-0.0.1-SNAPSHOT.jar:./target/lib/*  -javaagent:./lib/SizeOf.jar org.roaringbitmap.experiments.BenchmarkReal real-roaring-datasets $ds $N_EXPER
done



