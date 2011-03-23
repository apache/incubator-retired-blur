package com.nearinfinity.blur;


public class BlurShardName {
    
    public static String getShardName(String prefix, int id) {
        return prefix + buffer(id,8);
    }
    
    private static String buffer(int value, int length) {
        String str = Integer.toString(value);
        while (str.length() < length) {
            str = "0" + str;
        }
        return str;
    }

}
