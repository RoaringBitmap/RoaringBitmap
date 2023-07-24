---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
If it is a bug you encountered while working with the library, you should produce a code sample that allows us to reproduce the problem. Your code should be complete and immediately executable:

For example, this is a good example:
```Java
import java.io.IOException;

import org.roaringbitmap.RoaringBitmap;

public class Main {
    public static void main(String[] args) throws IOException {

        RoaringBitmap s1 = RoaringBitmap.bitmapOf(-587409880, 605467000);
        RoaringBitmap s2 = RoaringBitmap.bitmapOf(-587409880, 347844183);

        System.out.println(RoaringBitmap.andNotCardinality(s1, s2));
        System.out.println(RoaringBitmap.andNot(s1, s2).getCardinality());
    }
}
```

If you fail to provide the necessary information for us to reproduce the issue, we may reject your bug report.

**RoaringBitmap version:**

Please report bugs after testing the latest version of the library. If you are using an older version of the library, please explain.

Make sure to compare your version against the latest available version at https://mvnrepository.com/artifact/org.roaringbitmap/RoaringBitmap


**Java version:**

Please tell us which Java version you are using. 



