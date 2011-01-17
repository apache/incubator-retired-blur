package com.nearinfinity.blur.store.replication;
import java.io.File;
import java.util.Date;

public class CheckFs {
    public static void main(String[] args) throws InterruptedException {
        while (true) {
            for (String path : args) {
                File file = new File(path);
                if (file.canWrite()) {
                    System.out.println("Can Write [" + new Date() + 
                    		"] [" + file + 
                    		"] [" + file.exists() + 
                            "]");
                } else {
                    System.out.println("Can't Write [" + new Date() + 
                            "] [" + file + 
                            "] [" + file.exists() + 
                            "]");
                }
            }
            Thread.sleep(1000);
        }
    }
}
