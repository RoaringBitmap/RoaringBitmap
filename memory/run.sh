cp -r ../src/main/java/org .
cp ElementSizeBench.java org/roaringbitmap/ElementSizeBench.java
javac org/roaringbitmap/*.java org/roaringbitmap/buffer/*.java -cp lib/*
java  -cp lib/SizeOf.jar:. -javaagent:lib/SizeOf.jar org.roaringbitmap.ElementSizeBench