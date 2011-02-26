package com.nearinfinity.blur.store.replication;

import java.util.Comparator;

public class LuceneIndexFileComparator implements Comparator<String> {

    private String[] order = new String[]{"","fnm","del","tii","blf","nrm","tis","frq","prx","fdx","fdt"};
    
    @Override
    public int compare(String o1, String o2) {
        String e1 = getExtension(o1);
        String e2 = getExtension(o2);
        if (e1.equals(e2)) {
            return o1.compareTo(o2);
        }
        int e1Index = getIndex(e1);
        int e2Index = getIndex(e2);
        if (e1Index == e2Index) {
            return o1.compareTo(o2);
        }
        return e1Index < e2Index ? -1 : 1;
    }

    private int getIndex(String extension) {
        for (int i = 0; i < order.length; i++) {
            if (order[i].equals(extension)) {
                return i;
            }
        }
        return Integer.MAX_VALUE;
    }

    private String getExtension(String file) {
        int index = file.indexOf('.');
        if (index < 0) {
            return "";
        }
        int endIndex = file.indexOf('.',index + 1);
        if (endIndex < 0) {
            return file.substring(index+1);
        } else {
            return file.substring(index+1,endIndex);
        }
    }

}
