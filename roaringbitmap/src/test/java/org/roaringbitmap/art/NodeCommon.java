package org.roaringbitmap.art;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.roaringbitmap.art.BranchNode.ILLEGAL_IDX;

public class NodeCommon {
    static void checkKeyAndPosAlign(BranchNode node) {
        for (int i = 0; i < 256; i++) {
            int pos = node.getChildPos((byte)i);
            Node n1 = (pos != ILLEGAL_IDX) ? node.getChild(pos) : null;
            Node n2 = node.getChildAtKey((byte)i);
            assertSame(n1, n2);
        }

    }
}
