#!/bin/bash
BASEDIR=$(dirname $0)

CLEAN=""
if [[ "$1" = "-clean" ]]; then
    echo "Preparing benchmarks. Will clean jars..."
    CLEAN="clean"
    shift
fi

echo "Building RoaringBitmap jar"
if [[ "$CLEAN" = "clean" ]]; then
    rm -f $BASEDIR/../target/RoaringBitmap*.jar
fi
mvn -f $BASEDIR/../pom.xml $CLEAN install -DskipTests -Dgpg.skip=true -Dcheckstyle.skip

[[ $? -eq 0 ]] || exit

echo "Building Real Roaring Dataset jar"
if [[ "$CLEAN" = "clean" ]]; then
    rm -f $BASEDIR/../real-roaring-dataset/target/real-roaring-dataset*.jar
fi
mvn -f $BASEDIR/../real-roaring-dataset/pom.xml $CLEAN install

[[ $? -eq 0 ]] || exit

echo "Building benchmarks jar"
if [[ "$CLEAN" = "clean" ]]; then
    rm -f  $BASEDIR/target/benchmarks.jar
fi
mvn -f $BASEDIR/pom.xml $CLEAN install -Dtest=*${@:$#}* -DfailIfNoTests=false -Dcheckstyle.skip

[[ $? -eq 0 ]] || exit

echo "Running benchmarks"
java -jar $BASEDIR/target/benchmarks.jar true -wi 5 -i 5 -f 1 $@
