#! /bin/bash
trap 'exit' INT
javac -cp ../target/*:. simplebenchmark.java
echo "# bitspervalue nanotimefor2by2and nanotimefor2by2or nanotimeforwideor nanotimeforcontains (first normal then buffer)"
for file in ../real-roaring-dataset/src/main/resources/real-roaring-dataset/*.zip 
do
java -cp ../target/*:. simplebenchmark $file
done
