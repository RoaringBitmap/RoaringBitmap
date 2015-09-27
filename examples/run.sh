cd .. && mvn -Dmaven.test.skip=true package && cd examples 
echo "Running SerializeToByteBufferExample"
javac -cp "../target/*" SerializeToByteBufferExample.java && java -cp ../target/*:. SerializeToByteBufferExample
echo
echo "Running ImmutableRoaringBitmapExample"
javac -cp "../target/*" ImmutableRoaringBitmapExample.java && java -cp ../target/*:. ImmutableRoaringBitmapExample
echo
echo "Running MemoryMappingExample"
javac -cp "../target/*" MemoryMappingExample.java && java -cp ../target/*:. MemoryMappingExample
echo
echo "Serializing to byte array" 
javac -cp "../target/*":. SerializeToByteArrayExample.java && java -cp ../target/*:. SerializeToByteArrayExample

rm *.class
