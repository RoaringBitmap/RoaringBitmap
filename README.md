RoaringBitmap
=============

Roaring bitmaps are compressed bitmaps which tend to outperform conventional
compressed bitmaps such as WAH, EWAH, Concise... 


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


Usage
------

* Get java
* Get maven 2

* mvn compile will compile
* mvn test will run the unit tests
* mvn package will package in a jar (found in target)
