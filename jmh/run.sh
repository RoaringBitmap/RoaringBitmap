#!/bin/bash

set -e

BASEDIR=$(dirname $0)

CLEAN=""
if [[ "$1" = "-clean" ]]; then
    echo "Preparing benchmarks. Will clean jars..."
    CLEAN="clean"
    shift
fi

echo "Building RoaringBitmap jar"
if [[ "$CLEAN" = "clean" ]]; then
    $BASEDIR/../gradlew clean
fi

echo "Building benchmarks jar"
$BASEDIR/../gradlew jmhJar

echo "Running benchmarks"
java -jar $BASEDIR/target/benchmarks.jar true -wi 5 -i 5 -f 1 $@
