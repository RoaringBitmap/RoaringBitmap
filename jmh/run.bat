@echo off

echo Building RoaringBitmap.jar
call mvn -f ../pom.xml clean install -DskipTests -Dgpg.skip=true

echo Building Real Roaring Dataset jar
call mvn -f ../real-roaring-dataset/pom.xml clean install

echo Building benchmarks.jar
call mvn -f pom.xml clean package

echo Running benchmarks
java -jar target/benchmarks.jar -wi 5 -i 5 -f 1 %1
