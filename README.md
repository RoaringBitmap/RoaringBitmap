RoaringBitmap [![Build Status](https://travis-ci.org/lemire/RoaringBitmap.png)](https://travis-ci.org/lemire/RoaringBitmap)
=============

Bitsets, also called bitmaps, are commonly used as fast data structures.
Unfortunately, they can use too much memory. To compensate, we often use
compressed bitmaps.

Roaring bitmaps are compressed bitmaps which tend to outperform conventional
compressed bitmaps such as WAH, EWAH or Concise. In some instances, roaring bitmaps can
be hundreds of times faster and they often offer significantly better compression.
They can even be faster than uncompressed bitmaps.

This library is used by Apache Spark (https://spark.apache.org/) and 
Druid.io (http://druid.io/). Apache Lucene (http://lucene.apache.org/) uses  Roaring bitmaps, though they have their own [independent implementation](https://svn.apache.org/viewvc/lucene/dev/branches/branch_5x/lucene/core/src/java/org/apache/lucene/util/RoaringDocIdSet.java?view=markup&pathrev=1629606).



(c) 2013-2015 Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber, Seth Pellegrino, Borislav Ivanov, Gregory Ssi-Yan-Kai


This code is licensed under Apache License, Version 2.0 (ASL2.0). 


API docs
---------

http://lemire.me/docs/RoaringBitmap/

Scientific Documentation
--------------------------

Samy Chambi, Daniel Lemire, Owen Kaser, Robert Godin,
Better bitmap performance with Roaring bitmaps,
Software: Practice and Experience (to appear)
http://arxiv.org/abs/1402.6407

This paper used data from http://lemire.me/data/realroaring2014.html


Code sample
-------------

        
        import org.roaringbitmap.*;
        
        //...
        
        RoaringBitmap rr = RoaringBitmap.bitmapOf(1,2,3,1000);
        RoaringBitmap rr2 = new RoaringBitmap();
        for(int k = 4000; k<4255;++k) rr2.add(k);
        
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);

Please see the examples folder for more examples.

Working with memory-mapped bitmaps
---------------------------------------

If you want to have your bitmaps lie in memory-mapped files, you can
use the org.roaringbitmap.buffer package instead. 
        
The following code sample illustrates how to create an ImmutableRoaringBitmap
from a ByteBuffer. In such instances, the constructor only loads the meta-data
in RAM while the actual data is accessed from the ByteBuffer on demand.

        import org.roaringbitmap.buffer.*;
        
        //...
        
        MutableRoaringBitmap rr1 = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
        MutableRoaringBitmap rr2 = MutableRoaringBitmap.bitmapOf( 2, 3, 1010);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        rr1.serialize(dos);
        rr2.serialize(dos);
        dos.close();
        ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
        ImmutableRoaringBitmap rrback1 = new ImmutableRoaringBitmap(bb);
        bb.position(bb.position() + rrback1.serializedSizeInBytes());
        ImmutableRoaringBitmap rrback2 = new ImmutableRoaringBitmap(bb);
         
Operations on an ImmutableRoaringBitmap such as and, or, xor, flip, will
generate a RoaringBitmap which lies in RAM. As the name suggest, the 
ImmutableRoaringBitmap itself cannot be modified.

This design was inspired by druid.io.

One can find a complete working example in the test file TestMemoryMapping.java.

Note that you should not mix the classes from the org.roaringbitmap package with the classes
from the org.roaringbitmap.buffer package. They are incompatible. They serialize to the same output however.

Download
---------

You can download releases from the Maven repository:
http://central.maven.org/maven2/org/roaringbitmap/RoaringBitmap/

or from github:
https://github.com/lemire/RoaringBitmap/releases

Maven repository
----------------
If your project depends on roaring, you  can  specify the dependency in the Maven "pom.xml" file:

        <dependencies>
          <dependency>
            <groupId>org.roaringbitmap</groupId>
            <artifactId>RoaringBitmap</artifactId>
            <version>0.4.9</version>
          </dependency>
        </dependencies>

where you should replace the version number by the version you require.

Usage
------

* Get java
* Get maven 2

* mvn compile will compile
* mvn test will run the unit tests
* mvn package will package in a jar (found in target)

Benchmark
-----------
        
To run JMH benchmarks, use the following command:

         $ ./jmh/run.sh

You can also run specific benchmarks...

         $ ./jmh/run.sh org.roaringbitmap.aggregation.newand.identical.*

Funding 
----------

This work was supported by NSERC grant number 26143.
