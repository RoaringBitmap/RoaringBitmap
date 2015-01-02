if test -n "$(find ../target/RoaringBitmap-*.jar -maxdepth 1 -name 'files*' -print -quit)"
then
    cd .. && mvn -Dmaven.test.skip=true package && cd examples 
fi
echo "Running SerializeToByteBufferExample"
javac -cp "../target/*" SerializeToByteBufferExample.java && java -cp ../target/*:. SerializeToByteBufferExample
echo
echo "Running ImmutableRoaringBitmapExample"
javac -cp "../target/*" ImmutableRoaringBitmapExample.java && java -cp ../target/*:. ImmutableRoaringBitmapExample
echo
echo "Running MemoryMappingExample"
javac -cp "../target/*" MemoryMappingExample.java && java -cp ../target/*:. MemoryMappingExample

rm *.class