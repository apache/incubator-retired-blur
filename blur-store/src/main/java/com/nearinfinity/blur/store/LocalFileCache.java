package com.nearinfinity.blur.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalFileCache {
    
    private static Log LOG = LogFactory.getLog(LocalFileCache.class);

    private File[] files;
    private Random random = new Random();

    public LocalFileCache(File... files) {
        for (int i = 0; i < files.length; i++) {
            files[i].mkdirs();
        }
        this.files = getValid(files);
    }
    
    public File getLocalFile(String dirName, String name) {
        File file = locateFileExistingFile(dirName,name);
        if (file != null) {
            return file;
        }
        return newFile(dirName,name);
    }
    
    public void delete(String dirName) {
        for (File cacheDir : files) {
            File dirFile = new File(cacheDir,dirName);
            if (dirFile.exists()) {
                rm(dirFile);
            }
        }
    }

    public static void rm(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }

    private File[] getValid(File[] files) {
        List<File> result = new ArrayList<File>();
        for (File f : files) {
            if (isValid(f)) {
                result.add(f);
            }
        }
        if (result.isEmpty()) {
            fatalNoLocalDirs();
        }
        return result.toArray(new File[]{});
    }
    
    private void fatalNoLocalDirs() {
        LOG.fatal("No valid local directories, JVM exiting.");
        System.exit(1);
    }

    private boolean isValid(File f) {
        if (f.exists() && f.isDirectory()) {
            File file = new File(f,".blur.touchfile" + UUID.randomUUID().toString());
            try {
                if (file.createNewFile()) {
                    file.delete();
                    return true;
                }
            } catch (IOException e) {
                return false;
            }
        }
        return false;
    }

    private File newFile(String dirName, String name) {
        int index = random.nextInt(files.length);
        for (int i = 0; i < files.length; i++) {
            File dir = new File(files[index],dirName);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            if (isValid(dir)) {
                return new File(dir,name);
            }
            index++;
            if (index >= files.length) {
                index = 0;
            }
        }
        fatalNoLocalDirs();
        return null;
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
