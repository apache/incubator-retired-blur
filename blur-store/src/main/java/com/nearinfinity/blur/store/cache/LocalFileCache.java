package com.nearinfinity.blur.store.cache;

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
    private static final ExistenceCheck DEFAULT_EXISTENCE_CHECK = new ExistenceCheck() {
        @Override
        public boolean existsInBase(String dirName, String name) {
            return true;
        }
    };
    
    private File[] files;
    private Random random = new Random();
    private ExistenceCheck existenceCheck = DEFAULT_EXISTENCE_CHECK;
    private Timer daemon;
    private File[] potentialDirs;
    private boolean setup = false;
    private Date gcStartTime = getStartTime();
    private long gcWaitPeriod = TimeUnit.DAYS.toMillis(1);
    
    public void open() {
        tryToCreateAllDirs();
        files = getValid(potentialDirs);
        daemon = new Timer("LocalFileCache-FileGC-Daemon",true);
        daemon.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    fileGc();
                } catch (Exception e) {
                    LOG.error("Unknown error while trying to GC",e);
                }
            }
        }, gcStartTime, gcWaitPeriod);
        setup = true;
    }
    
    public void close() {
        daemon.cancel();
        daemon.purge();
    }
    
    public File getLocalFile(String dirName, String name) {
        checkIfOpen();
        File file = locateFileExistingFile(dirName,name);
        if (file != null) {
            return file;
        }
        return newFile(dirName,name);
    }
    
    public void delete(String dirName) {
        checkIfOpen();
        for (File cacheDir : files) {
            File dirFile = new File(cacheDir,dirName);
            if (dirFile.exists()) {
                rm(dirFile);
            }
        }
    }

    public static void rm(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
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

    private void checkIfOpen() {
        if (setup) {
            return;
        }
        throw new RuntimeException("Local File Cache not open, run open().");
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
    
    private void tryToCreateAllDirs() {
        for (File f : potentialDirs) {
            f.mkdirs();
        }
    }

    public ExistenceCheck getExistenceCheck() {
        return existenceCheck;
    }

    public void setExistenceCheck(ExistenceCheck existenceCheck) {
        this.existenceCheck = existenceCheck;
    }

    public File[] getPotentialFiles() {
        return potentialDirs;
    }

    public void setPotentialFiles(File... potentialFiles) {
        this.potentialDirs = potentialFiles;
    }

    public File[] getFiles() {
        return files;
    }

    public Date getGcStartTime() {
        return gcStartTime;
    }

    public void setGcStartTime(Date gcStartTime) {
        this.gcStartTime = gcStartTime;
    }

    public long getGcWaitPeriod() {
        return gcWaitPeriod;
    }

    public void setGcWaitPeriod(long gcWaitPeriod) {
        this.gcWaitPeriod = gcWaitPeriod;
    }
}
