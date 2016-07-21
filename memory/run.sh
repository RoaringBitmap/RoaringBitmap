#!/usr/bin/env bash

BASEDIR=$(dirname $0)

echo "Building RoaringBitmap jar"
mvn -f $BASEDIR/../pom.xml clean install -DskipTests -Dgpg.skip=true

echo "Building Real Roaring Dataset jar"
mvn -f $BASEDIR/../real-roaring-dataset/pom.xml clean install

echo "Running benchmarks"
mvn -f $BASEDIR/pom.xml clean test
