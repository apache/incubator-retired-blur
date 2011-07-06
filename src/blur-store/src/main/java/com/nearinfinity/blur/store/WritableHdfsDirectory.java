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

package com.nearinfinity.blur.store;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.Constants;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.indexinput.FileIndexInput;
import com.nearinfinity.blur.store.indexinput.FileNIOIndexInput;
import com.nearinfinity.blur.store.indexinput.MMapIndexInput;

public class WritableHdfsDirectory extends HdfsDirectory {

    private static final String SEGMENTS_GEN = "segments.gen";
    private static final Log LOG = LogFactory.getLog(WritableHdfsDirectory.class);
    private static final long BACK_OFF = TimeUnit.SECONDS.toMillis(60);
    
    protected Random random = new Random();
    protected LocalFileCache _localFileCache;
    protected String _dirName;
    protected Progressable _progressable;
    protected int _retryCount = 10;
    protected ExecutorService _service;
    
    public WritableHdfsDirectory(String dirName, Path hdfsDirPath, LocalFileCache localFileCache, LockFactory lockFactory) throws IOException {
        this(dirName,hdfsDirPath,localFileCache,lockFactory,new Progressable() {
            @Override
            public void progress() {
                
            }
        });
    }

    public WritableHdfsDirectory(String dirName, Path hdfsDirPath, LocalFileCache localFileCache, LockFactory lockFactory, Progressable progressable) throws IOException {
        super(hdfsDirPath);
        _dirName = dirName;
        _progressable = progressable;
        File segments = localFileCache.getLocalFile(dirName, SEGMENTS_GEN);
        if (segments.exists()) {
            segments.delete();
        }
        _localFileCache = localFileCache;
        setLockFactory(lockFactory);
        _service = Executors.newSingleThreadExecutor();
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        File file = _localFileCache.getLocalFile(_dirName, name);
        if (file.exists()) {
            file.delete();
        }
        LOG.debug("Opening local file for writing [{0}]",file.getAbsolutePath());
        return new FileIndexOutput(_progressable,file);
    }

    @Override
    public void sync(String name) throws IOException {
        CopyCallable callable = new CopyCallable(name, this);
        Future<Boolean> future = _service.submit(callable);
        try {
            if (future.get()) {
                return;
            }
            throw new IOException("Unknown error during sync.");
        } catch (InterruptedException e) {
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    public static class CopyCallable implements Callable<Boolean> {
        
        private String _name;
        private WritableHdfsDirectory _directory;
        
        public CopyCallable(String name, WritableHdfsDirectory directory) {
            _directory = directory;
            _name = name;
        }
        
        @Override
        public Boolean call() throws Exception {
            File file = _directory._localFileCache.getLocalFile(_directory._dirName, _name);
            int count = 0;
            while (true) {
                Path dest = new Path(_directory._hdfsDirPath, _name + ".sync." + count);
                try {
                    LOG.info("Syncing local file [{0}] to [{1}]",file.getAbsolutePath(),_directory._hdfsDirPath);
                    FSDataOutputStream outputStream = _directory.getFileSystem().create(dest);
                    InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
                    byte[] buffer = new byte[4096];
                    int num;
                    while ((num = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, num);
                        _directory._progressable.progress();
                    }
                    _directory.close(outputStream);
                    _directory.close(inputStream);
                    _directory.rename(_name + ".sync." + count,_name);
                    return true;
                } catch (IOException e) {
                    LOG.error("Error during copy of [{0}] local file [{1}] to [{2}], backing off copy for a moment.",e,count,file.getAbsolutePath(),dest);
                    try {
                        Thread.sleep(BACK_OFF);
                    } catch (InterruptedException ex) {
                        return false;
                    }
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(_directory.random.nextInt(60)) + 1);
                    } catch (InterruptedException ex) {
                        return false;
                    }
                    _directory.reopenFileSystem();
                    if (count < _directory._retryCount) {
                        LOG.error("Error sync retry count [{0}] local file [{1}] to [{2}]",e,count,file.getAbsolutePath(),dest);
                        count++;
                        try {
                            _directory.getFileSystem().delete(dest, false);
                        } catch (IOException ex) {
                            LOG.error("Error trying to delete back file [{0}]",ex,dest);
                        }
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    private void close(Closeable closeable) throws IOException {
        closeable.close();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (super.fileExists(name)) {
            super.deleteFile(name);
        }
        File localFile = _localFileCache.getLocalFile(_dirName, name);
        if (localFile.exists()) {
            localFile.delete();
        }
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        if (super.fileExists(name)) {
            return true;
        }
        if (fileExistsLocally(name)) {
            return true;
        }
        return false;
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (super.fileExists(name)) {
            return super.fileLength(name);
        } else {
            return _localFileCache.getLocalFile(_dirName, name).length();
        }
    }

    @Override
    public long fileModified(String name) throws IOException {
        if (super.fileExists(name)) {
            return super.fileModified(name);
        } else {
            return _localFileCache.getLocalFile(_dirName, name).lastModified();
        }
    }

    @Override
    public String[] listAll() throws IOException {
        return super.listAll();
    }

    @Override
    public IndexInput openInput(String name, int bufferSize) throws IOException {
        if (!fileExists(name)) {
            throw new FileNotFoundException(name);
        }
        if (!fileExistsLocally(name)) {
            return openFromHdfs(name, bufferSize);
        } else {
            return openFromLocal(name, bufferSize);
        }
    }

    public boolean fileExistsLocally(String name) throws IOException {
        return _localFileCache.getLocalFile(_dirName, name).exists();
    }

    public IndexInput openFromLocal(String name, int bufferSize) throws IOException {
        if (Constants.WINDOWS) {
            return new FileIndexInput(_localFileCache.getLocalFile(_dirName, name), bufferSize);
        } else if (name.endsWith(".fdt") || !Constants.JRE_IS_64BIT) {
            return new FileNIOIndexInput(_localFileCache.getLocalFile(_dirName, name), bufferSize);
        } else {
            return new MMapIndexInput(_localFileCache.getLocalFile(_dirName, name));
        }
    }

    public IndexInput openFromHdfs(String name, int bufferSize) throws IOException {
        return super.openInput(name, bufferSize);
    }

    public static class FileIndexOutput extends BufferedIndexOutput {

        private RandomAccessFile file = null;

        // remember if the file is open, so that we don't try to close it more than once
        private volatile boolean isOpen;

        private Progressable progressable;

        public FileIndexOutput(Progressable progressable, File path) throws IOException {
            file = new RandomAccessFile(path, "rw");
            isOpen = true;
            this.progressable = progressable;
        }

        @Override
        public void flushBuffer(byte[] b, int offset, int size) throws IOException {
            file.write(b, offset, size);
            progressable.progress();
        }

        @Override
        public void close() throws IOException {
            // only close the file if it has not been closed yet
            if (isOpen) {
                boolean success = false;
                try {
                    super.close();
                    success = true;
                } finally {
                    isOpen = false;
                    if (!success) {
                        try {
                            file.close();
                        } catch (Throwable t) {
                            // Suppress so we don't mask original exception
                        }
                    } else {
                        file.close();
                    }
                }
            }
        }

        @Override
        public void seek(long pos) throws IOException {
            super.seek(pos);
            file.seek(pos);
        }

        @Override
        public long length() throws IOException {
            return file.length();
        }

        @Override
        public void setLength(long length) throws IOException {
            file.setLength(length);
        }
    }
}
