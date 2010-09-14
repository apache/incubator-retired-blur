package com.nearinfinity.blur.manager.hits;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

public class PeekableIteratorTest {
    
    @Test
    public void testPeekableIterator1() {
        PeekableIterator<Integer> iterator = new PeekableIterator<Integer>(Arrays.asList(0,1,2,3,4,5,6,7,8,9).iterator());
        while (iterator.hasNext()) {
            for (int i = 0; i < 3; i++) {
                System.out.println(iterator.peek());
            }
            System.out.println(iterator.next());
        }
    }
    
    @Test
    public void testPeekableIteratorEmpty() {
        PeekableIterator<Integer> iterator = new PeekableIterator<Integer>(new ArrayList<Integer>().iterator());
        for (int i = 0; i < 3; i++) {
            System.out.println(iterator.peek());
        }
        while (iterator.hasNext()) {
            for (int i = 0; i < 3; i++) {
                System.out.println(iterator.peek());
            }
            System.out.println(iterator.next());
        }
    }

}
