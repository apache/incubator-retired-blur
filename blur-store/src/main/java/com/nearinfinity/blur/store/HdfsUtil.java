package com.nearinfinity.blur.store;

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
