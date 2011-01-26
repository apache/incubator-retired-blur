package com.nearinfinity.blur.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalFileCache {
    
    private static Log LOG = LogFactory.getLog(LocalFileCache.class);

    private File[] files;
    private Random random = new Random();
    private ExistenceCheck existenceCheck;

    private Timer daemon;
    
    public LocalFileCache(File... files) {
        this(new ExistenceCheck() {
            @Override
            public boolean existsInBase(String dirName, String name) {
                return true;
            }
        },files);
    }

    public LocalFileCache(ExistenceCheck existenceCheck, File... files) {
        for (int i = 0; i < files.length; i++) {
            files[i].mkdirs();
        }
        this.files = getValid(files);
        this.existenceCheck = existenceCheck;
        startFileGCDaemon();
    }
    
    private void startFileGCDaemon() {
        daemon = new Timer("LocalFileCache-FileGC-Daemon",true);
        daemon.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                fileGc();
            }
        }, getStartTime(), TimeUnit.DAYS.toMillis(1));
    }

    private Date getStartTime() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.set(Calendar.HOUR, 1);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.DATE, 1);
        return calendar.getTime();
    }

    private synchronized void fileGc() {
        LOG.info("Starting file gc.");
        for (File baseDir : files) {
            try {
                if (isValid(baseDir)) {
                    fileGc(baseDir);
                } else {
                    LOG.info("Dir [" + baseDir + "] is not valid.");
                }
            } catch (Exception e) {
                LOG.error("Error while trying gc files [" + baseDir.getAbsolutePath() + "].",e);
            }
        }
    }

    private void fileGc(File baseDir) {
        LOG.info("File gc processing base dir [" + baseDir.getAbsolutePath() + "].");
        for (File dir : baseDir.listFiles()) {
            fileGc(dir.getName(),dir);
            if (isEmpty(dir)) {
                LOG.info("Dir [" + dir.getAbsolutePath() + "] empty, removing.");
                dir.delete();
            }
        }
    }

    private boolean isEmpty(File dir) {
        File[] listFiles = dir.listFiles();
        if (listFiles == null || listFiles.length == 0) {
            return true;
        }
        return false;
    }

    private void fileGc(String dirName, File dir) {
        LOG.info("File gc processing dir [" + dirName + "] at [" + dir.getAbsolutePath() + "].");
        for (File file : dir.listFiles()) {
            try {
                if (!existenceCheck.existsInBase(dirName,file.getName())) {
                    LOG.info("Removing file [" + file.getAbsolutePath() + "] in dir [" + dirName + "] at [" + dir.getAbsolutePath() + "].");
                    file.delete();
                }
            } catch (Exception e) {
                LOG.info("Error while processing file [" + file.getAbsolutePath() + "] in dir [" + dirName + "] at [" + dir.getAbsolutePath() + "].",e);
            }
        }
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
                    return true;
                }
            } catch (IOException e) {
                return false;
            } finally {
                file.delete();
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
