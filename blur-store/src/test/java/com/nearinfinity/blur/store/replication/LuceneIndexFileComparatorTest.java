package com.nearinfinity.blur.store.replication;

import static org.junit.Assert.*;

import org.junit.Test;

public class LuceneIndexFileComparatorTest {
    
    @Test
    public void testLuceneIndexFileComparator() {
        LuceneIndexFileComparator comparator = new LuceneIndexFileComparator();
        assertTrue(comparator.compare("hey.tis", "hey2.tis") < 0);
        assertTrue(comparator.compare("hey.tii", "hey.tis") < 0);
        assertTrue(comparator.compare("hey.tis", "hey.tii") > 0);
    }

}
