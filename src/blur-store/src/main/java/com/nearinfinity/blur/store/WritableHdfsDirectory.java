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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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

    protected LocalFileCache _localFileCache;
    protected String _dirName;
    protected Progressable _progressable;
    protected int _retryCount = 10;
    
    public WritableHdfsDirectory(String dirName, Path hdfsDirPath, FileSystem fileSystem,
            LocalFileCache localFileCache, LockFactory lockFactory) throws IOException {
        this(dirName,hdfsDirPath,fileSystem,localFileCache,lockFactory,new Progressable() {
            @Override
            public void progress() {
                
            }
        });
    }

    public WritableHdfsDirectory(String dirName, Path hdfsDirPath, FileSystem fileSystem,
            LocalFileCache localFileCache, LockFactory lockFactory, Progressable progressable) throws IOException {
        super(hdfsDirPath, fileSystem);
        this._dirName = dirName;
        this._progressable = progressable;
        File segments = localFileCache.getLocalFile(dirName, SEGMENTS_GEN);
        if (segments.exists()) {
            segments.delete();
        }
        this._localFileCache = localFileCache;
        setLockFactory(lockFactory);
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
        File file = _localFileCache.getLocalFile(_dirName, name);
        int count = 0;
        while (true) {
            Path dest = new Path(hdfsDirPath,name + ".sync." + count);
            try {
                LOG.debug("Syncing local file [{0}] to [{1}]",file.getAbsolutePath(),hdfsDirPath);
                FSDataOutputStream outputStream = fileSystem.create(dest);
                InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
                byte[] buffer = new byte[4096];
                int num;
                while ((num = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, num);
                    _progressable.progress();
                }
                rename(name + ".sync." + count,name);
                return;
            } catch (IOException e) {
                if (count < _retryCount) {
                    LOG.error("Error sync retry count [{0}] local file [{1}] to [{2}]",e,count,file.getAbsolutePath(),dest);
                    count++;
                    try {
                        fileSystem.delete(dest, false);
                    } catch (IOException ex) {
                        LOG.error("Error trying to delete back file [{0}]",ex,dest);
                    }
                } else {
                    throw e;
                }
            }
        }
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
