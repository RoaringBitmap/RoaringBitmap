Real data sets for bitmap testing
==

Essentially, each file represents a set of integer values. You can create
bitmaps out of these files.

In many cases, the description of the data sets is provided in :

* Samy Chambi, Daniel Lemire, Owen Kaser, Robert Godin, Better bitmap performance with Roaring bitmaps, arXiv:1402.6407.
http://arxiv.org/abs/1402.6407

To be used with software published on http://roaringbitmap.org/




Files starting with the prefix "dimension" were prepared by Xavier Léauté from
a Druid dump.


---

All files are zipped collection of textfiles except for bitsets_1925630_96.gz. These can be deserialized by first reading an int, that is the amout of rows to come (e.g. 1925630 rows). A row is read by first reading an int, the amount of longs to come (e.g. 96 longs), and then reading those longs. Used DataInputStream to write this. See also BitSetUtilBenchmark.
