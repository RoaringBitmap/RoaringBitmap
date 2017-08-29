cd .. && mvn -Dcheckstyle.skip=true -Dmaven.test.skip=true -Dmaven.javadoc.skip=true package && cd examples
echo "Basic"
javac -cp "../target/*" Basic.java && java -cp ../target/*:. Basic
echo
echo "Bitmap64"
javac -cp "../target/*" Bitmap64.java && java -cp ../target/*:. Bitmap64
echo
echo "Running CompressionResults"
javac -cp "../target/*" CompressionResults.java && java -cp ../target/*:. CompressionResults
echo
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
echo
echo "ForEach example"
javac -cp "../target/*":. ForEachExample.java && java -cp ../target/*:. ForEachExample
echo
echo "Very large example"
javac -cp "../target/*":. VeryLargeBitmap.java && java -cp ../target/*:. VeryLargeBitmap
echo
echo
echo "paging"
javac -cp "../target/*":. PagedIterator.java && java -cp ../target/*:. PagedIterator
echo
echo "Serializing to file "
javac -cp "../target/*" SerializeToDiskExample.java &&  java -cp ../target/*:. SerializeToDiskExample
rm bitmapwithoutruns.bin  bitmapwithruns.bin
rm *.class
