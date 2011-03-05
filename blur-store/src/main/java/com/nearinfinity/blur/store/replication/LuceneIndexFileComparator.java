/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
