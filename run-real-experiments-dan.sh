N_EXPER=1  #we have enough data to go up to 100
mvn package -Dmaven.test.skip=true
#sudo /home/owen/bin/cpuscaling.sh performance    #avoid on demand stuff
echo turbo boost status:
#turboboost.sh off
#full weather lead to crash   weather_large_withjan82.csv 

for ds in census-income.csv  ; do
    java -cp ./target/RoaringBitmap-0.0.1-SNAPSHOT.jar:./target/lib/*  -javaagent:./lib/SizeOf.jar org.roaringbitmap.experiments.BenchmarkReal real-roaring-datasets $ds $N_EXPER
done



