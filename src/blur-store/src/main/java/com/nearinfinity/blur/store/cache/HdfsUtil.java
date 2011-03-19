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

package com.nearinfinity.blur.store.cache;

import org.apache.hadoop.fs.Path;

public class HdfsUtil {
    
    private static final String SEP = "__";
    private static final int SEP_LENGTH = SEP.length();
    
    public static String getDirName(String table, String shard) {
        checkIfValid(table);
        checkIfValid(shard);
        return table + SEP + shard;
    }

    private static void checkIfValid(String s) {
        if (s.contains(SEP)) {
            throw new IllegalArgumentException("[" + s + "] cannot contain [" + SEP + "]");
        }
    }
    
    public static Path getHdfsPath(Path base, String dirName) {
        String table = getTable(dirName);
        String shard = getShard(dirName);
        Path tablePath = new Path(base,table);
        return new Path(tablePath,shard);
    }

    public static String getShard(String dirName) {
        return dirName.substring(indexOfSep(dirName) + SEP_LENGTH);
    }

    public static String getTable(String dirName) {
        return dirName.substring(0,indexOfSep(dirName));
    }
    
    private static int indexOfSep(String dirName) {
        int index = dirName.indexOf(SEP);
        if (index < 0) {
            throw new IllegalArgumentException("Dirname [" + dirName + "] is not valid.");
        }
        return index;
    }

}
