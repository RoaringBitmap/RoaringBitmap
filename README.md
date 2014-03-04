RoaringBitmap [![Build Status](https://travis-ci.org/lemire/RoaringBitmap.png)](https://travis-ci.org/lemire/RoaringBitmap)
=============

Bitsets, also called bitmaps, are commonly used as fast data structures.
Unfortunately, they can use too much memory. To compensate, we often use
compressed bitmaps.

Roaring bitmaps are compressed bitmaps which tend to outperform conventional
compressed bitmaps such as WAH, EWAH or Concise. In some instances, they can
be hundreds of times faster and they often offer significantly better compression. 



(c) 2013-2014 Daniel Lemire, Owen Kaser, Samy Chambi

This code is licensed under Apache License, Version 2.0 (ASL2.0). 


API docs
---------

http://lemire.me/docs/RoaringBitmap/

Documentation
--------------

Samy Chambi, Daniel Lemire, Owen Kaser, Robert Godin,
Better bitmap performance with Roaring bitmaps,
http://arxiv.org/abs/1402.6407


Code sample
-------------
        
        import org.roaringbitmap.*;
        
        //...
        
        RoaringBitmap rr = new RoaringBitmap.bitmapOf(1,2,3,1000);
        RoaringBitmap rr2 = new RoaringBitmap();
        for(int k = 4000; k<4255;++k) rr2.add(k);
        
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);

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
    <version>0.0.1</version>
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
