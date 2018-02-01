#!/bin/bash
BASEDIR=$(dirname $0)


echo "Building RoaringBitmap jar"
rm -f $BASEDIR/../target/RoaringBitmap*.jar
mvn -f $BASEDIR/../pom.xml clean install -DskipTests -Dgpg.skip=true -Dcheckstyle.skip

[[ $? -eq 0 ]] || exit

echo "Building Real Roaring Dataset jar"
rm -f $BASEDIR/../real-roaring-dataset/target/real-roaring-dataset*.jar
mvn -f $BASEDIR/../real-roaring-dataset/pom.xml clean install

[[ $? -eq 0 ]] || exit

echo "Building benchmarks jar"
rm -f  $BASEDIR/target/benchmarks.jar
mvn -f $BASEDIR/pom.xml clean install -Dtest=*$1* -DfailIfNoTests=false -Dcheckstyle.skip

[[ $? -eq 0 ]] || exit

echo "Running benchmarks"
java -jar $BASEDIR/target/benchmarks.jar  true -wi 5 -i 5 -f 1 $@
