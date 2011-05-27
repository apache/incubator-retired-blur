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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class LocalFileCache {
    
    private static Log LOG = LogFactory.getLog(LocalFileCache.class);
    private static final LocalFileCacheCheck DEFAULT_EXISTENCE_CHECK = new LocalFileCacheCheck() {
        @Override
        public boolean isSafeForRemoval(String dirName, String name) {
            return false;
        }
    };
    
    private File[] files;
    private Random random = new Random();
    private LocalFileCacheCheck localFileCacheCheck = DEFAULT_EXISTENCE_CHECK;
    private Timer daemon;
    private File[] potentialDirs;
    private boolean setup = false;
    private long gcStartDelay = TimeUnit.MINUTES.toMillis(5);
    private long gcWaitPeriod = TimeUnit.HOURS.toMillis(1);
    private boolean closed;
    
    public void init() {
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
        }, gcStartDelay, gcWaitPeriod);
        setup = true;
    }
    
    public synchronized void close() {
        if (!closed) {
            closed = true;
            daemon.cancel();
            daemon.purge();
        }
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

    private synchronized void fileGc() {
        LOG.info("Starting file gc.");
        for (File baseDir : files) {
            try {
                fileGc(baseDir);
            } catch (Exception e) {
                LOG.error("Error while trying gc files [{0}].",e,baseDir.getAbsolutePath());
            }
        }
    }

    private void fileGc(File baseDir) {
        LOG.info("File gc processing base dir [{0}].",baseDir.getAbsolutePath());
        for (File dir : baseDir.listFiles()) {
            fileGc(dir.getName(),dir);
            if (isEmpty(dir)) {
                LOG.info("Dir [{0}] empty, removing.",dir.getAbsolutePath());
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
        LOG.info("File gc processing dir [{0}] at [{1}].",dirName,dir.getAbsolutePath());
        for (File file : dir.listFiles()) {
            try {
                if (localFileCacheCheck.isSafeForRemoval(dirName,file.getName())) {
                    LOG.info("Removing file [{0}] in dir [{1}] at [{2}].",file.getAbsolutePath(),dirName,dir.getAbsolutePath());
                    file.delete();
                }
            } catch (Exception e) {
                LOG.info("Error while processing file [{0}] in dir [{1}] at [{2}].",e,file.getAbsolutePath(),dirName,dir.getAbsolutePath());
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
        //@todo fix this!!!!
        //LOG.debug("need a way to check if path is good or not");
        return false;
    }

    private File newFile(String dirName, String name) {
        int index = random.nextInt(files.length);
        for (int i = 0; i < files.length; i++) {
            File dir = new File(files[index],dirName);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            try {
                createDir(dir);
                File file = new File(dir,name);
                return file;
            } catch (IOException e) {
                LOG.error("Could not create file in directory [{0}]",dir);
            }
            index++;
            if (index >= files.length) {
                index = 0;
            }
        }
        fatalNoLocalDirs();
        return null;
    }

    private void createDir(File dir) throws IOException {
        File file = new File(dir,UUID.randomUUID().toString());
        file.createNewFile();
        file.delete();
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

    public void setLocalFileCacheCheck(LocalFileCacheCheck localFileCacheCheck) {
        this.localFileCacheCheck = localFileCacheCheck;
    }

    public void setPotentialFiles(File... potentialFiles) {
        this.potentialDirs = potentialFiles;
    }

    public void setGcWaitPeriod(long gcWaitPeriod) {
        this.gcWaitPeriod = gcWaitPeriod;
    }

    public void setGcStartDelay(long gcStartDelay) {
        this.gcStartDelay = gcStartDelay;
    }
}
