BASEDIR=$(dirname $0)

echo "Building RoaringBitmap jar"
mvn -f $BASEDIR/../pom.xml clean install -DskipTests -Dgpg.skip=true

echo "Building benchmarks jar"
mvn -f $BASEDIR/pom.xml clean install

echo "Running benchmarks"
java -jar $BASEDIR/target/benchmarks.jar -wi 5 -i 5 -f 1 $1
