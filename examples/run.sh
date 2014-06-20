if test -n "$(find ../target/RoaringBitmap-*.jar -maxdepth 1 -name 'files*' -print -quit)"
then
    cd .. && mvn -Dmaven.test.skip=true package && cd examples 
fi

javac -cp "../target/*" SerializeToByteBufferExample.java && java -cp ../target/*:. SerializeToByteBufferExample
rm *.class