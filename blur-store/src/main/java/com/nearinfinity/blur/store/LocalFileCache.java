package com.nearinfinity.blur.store;

import java.io.File;
import java.util.Random;

public class LocalFileCache {

    private File[] files;
    private Random random = new Random();

    public LocalFileCache(File... files) {
        this.files = files;
        for (int i = 0; i < files.length; i++) {
            files[i].mkdirs();
        }
    }

    public File getLocalFile(String dirName, String name) {
        File file = locateFileExistingFile(dirName,name);
        if (file != null) {
            return file;
        }
        return newFile(dirName,name);
    }

    private File newFile(String dirName, String name) {
        int index = random.nextInt(files.length);
        File dir = new File(files[index],dirName);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return new File(dir,name);
    }

    private File locateFileExistingFile(String dirName, String name) {
        for (int i = 0; i < files.length; i++) {
            File dir = new File(files[i],dirName);
            File file = new File(dir,name);
            if (file.exists()) {
                return file;
            }
        }
        return null;
    }
}
