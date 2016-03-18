#!/bin/bash

set -o errexit

BASEDIR=$(dirname $0)
echo "doing flight recorder tests for SP150271 paper"


echo "Building RoaringBitmap jar"
rm -f $BASEDIR/../target/RoaringBitmap*.jar
mvn -f $BASEDIR/../pom.xml clean install -DskipTests -Dgpg.skip=true

echo "Building Real Roaring Dataset jar"
rm -f $BASEDIR/../real-roaring-dataset/target/real-roaring-dataset*.jar
mvn -f $BASEDIR/../real-roaring-dataset/pom.xml clean install

echo "Building benchmarks jar"
rm -f  $BASEDIR/target/benchmarks.jar
mvn -f $BASEDIR/pom.xml clean install -DskipTests -DfailIfNoTests=false
declare -a TESTS
declare -a MODELS
MODELS=(roaring runroaring)
TESTS=(RealDataBenchmarkAnd RealDataBenchmarkOr RealDataBenchmarkWideOrNaive)

echo "running actual tests"
myfiles=""
for t in ${TESTS[@]}; do
for m in ${MODELS[@]}; do
 myfiles+=" "$m.$t.jfr
 rm -f $m.$t.jfr
 java -jar target/benchmarks.jar spe150271.$m.$t -wi 20 -i 20 -f 1 -jvmArgs="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=dumponexit=true,filename=$m.$t.jfr"
done
done
echo "the flight recorder files are " $myfiles

