#!/bin/bash

set -e

BASEDIR=$(dirname $0)
echo $BASEDIR

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
$BASEDIR/../gradlew shadowJar

echo "Running benchmarks"
java --add-modules jdk.incubator.vector -jar $BASEDIR/build/libs/benchmarks.jar true -wi 5 -i 5 -f 1 $@
