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

import static com.nearinfinity.blur.store.Constants.BUFFER_SIZE;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class HdfsDirectory extends Directory {

    protected Path _hdfsDirPath;
    protected AtomicReference<FileSystem> _fileSystemRef = new AtomicReference<FileSystem>();
    protected Configuration _configuration;

    public HdfsDirectory(Path hdfsDirPath) throws IOException {
        _hdfsDirPath = hdfsDirPath;
        
        _configuration = new Configuration();
        String disableCacheName = String.format("fs.%s.impl.disable.cache", _hdfsDirPath.toUri().getScheme());
        _configuration.setBoolean(disableCacheName, true);
        
        reopenFileSystem();
        try {
            if (!getFileSystem().exists(hdfsDirPath)) {
                getFileSystem().mkdirs(hdfsDirPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void reopenFileSystem() throws IOException {
        FileSystem fileSystem = FileSystem.get(_hdfsDirPath.toUri(), _configuration);
        FileSystem oldFs = _fileSystemRef.get();
        _fileSystemRef.set(fileSystem);
        if (oldFs != null) {
            oldFs.close();
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        final FSDataOutputStream outputStream = getOutputStream(name);
        return new IndexOutput() {

            private long length = 0;

            @Override
            public void close() throws IOException {
                outputStream.close();
            }

            @Override
            public void flush() throws IOException {
                outputStream.flush();
            }

            @Override
            public long getFilePointer() {
                return length;
            }

            @Override
            public long length() throws IOException {
                return length;
            }

            @Override
            public void seek(long pos) throws IOException {
                throw new RuntimeException("not supported");
            }

            @Override
            public void writeByte(byte b) throws IOException {
                outputStream.write(b);
                length++;
            }

            @Override
            public void writeBytes(byte[] b, int off, int len) throws IOException {
                outputStream.write(b, off, len);
                length += len;
            }
        };
    }

    protected void rename(String currentName, String newName) throws IOException {
        getFileSystem().rename(new Path(_hdfsDirPath,currentName), new Path(_hdfsDirPath,newName));
    }
    
    protected FSDataOutputStream getOutputStream(String name) throws IOException {
        return getFileSystem().create(new Path(_hdfsDirPath, name));
    }
    
    @Override
    public IndexInput openInput(String name) throws IOException {
        return openInput(name, BUFFER_SIZE);
    }

    @Override
    public IndexInput openInput(final String name, int bufferSize) throws IOException {
        long length = fileLength(name);
        final FSDataInputStream inputStream = getFileSystem().open(new Path(_hdfsDirPath, name));
        return new HdfsBufferedIndexInput(inputStream,length,bufferSize,name);
    }
    
    @Override
    public void deleteFile(String name) throws IOException {
        if (!fileExists(name)) {
            throw new FileNotFoundException(name);
        }
        getFileSystem().delete(new Path(_hdfsDirPath, name), false);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return fileExistsHdfs(name);
    }
    
    public boolean fileExistsHdfs(String name) throws IOException {
        return getFileSystem().exists(new Path(_hdfsDirPath, name));
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (!fileExists(name)) {
            throw new FileNotFoundException(name);
        }
        FileStatus fileStatus = getFileSystem().getFileStatus(new Path(_hdfsDirPath, name));
        return fileStatus.getLen();
    }

    @Override
    public long fileModified(String name) throws IOException {
        if (!fileExists(name)) {
            throw new FileNotFoundException(name);
        }
        FileStatus fileStatus = getFileSystem().getFileStatus(new Path(_hdfsDirPath, name));
        return fileStatus.getModificationTime();
    }

    @Override
    public String[] listAll() throws IOException {
        FileStatus[] listStatus = getFileSystem().listStatus(_hdfsDirPath);
        List<String> files = new ArrayList<String>();
        for (FileStatus status : listStatus) {
            if (!status.isDir()) {
                files.add(status.getPath().getName());
            }
        }
        return files.toArray(new String[] {});
    }

    @Override
    public void touchFile(String name) throws IOException {
        // do nothing
    }

    public Path getHdfsDirPath() {
        return _hdfsDirPath;
    }

    public FileSystem getFileSystem() {
        return _fileSystemRef.get();
    }

    public static class HdfsBufferedIndexInput extends BufferedIndexInput {
        
        private long length;
        private FSDataInputStream inputStream;
        private boolean isClone = false;
        private String name;
        
        public HdfsBufferedIndexInput(FSDataInputStream inputStream, long length, int bufferSize, String name) {
            super(bufferSize);
            this.length = length;
            this.inputStream = inputStream;
            this.name = name;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public void close() throws IOException {
            if (!isClone) inputStream.close();
        }

        @Override
        protected void seekInternal(long pos) throws IOException {

        }

        @Override
        protected void readInternal(byte[] b, int offset, int len) throws IOException {
            long position = getFilePointer();
            if (position >= length) {
                throw new IOException("EOF [" + name + "]");
            }
            inputStream.readFully(position, b, offset, len);
        }

        @Override
        public Object clone() {
            HdfsBufferedIndexInput clone = (HdfsBufferedIndexInput) super.clone();
            clone.isClone = true;
            return clone;
        }
    }
}
