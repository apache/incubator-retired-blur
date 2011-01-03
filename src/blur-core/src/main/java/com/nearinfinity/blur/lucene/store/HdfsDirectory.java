/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.lucene.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    private static final int BUFFER_SIZE = 16384;
    private Path hdfsDirPath;
    private FileSystem fileSystem;

    public HdfsDirectory(Path hdfsDirPath, FileSystem fileSystem) {
        this.hdfsDirPath = hdfsDirPath;
        this.fileSystem = fileSystem;
        try {
            if (!fileSystem.exists(hdfsDirPath)) {
                fileSystem.mkdirs(hdfsDirPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        final FSDataOutputStream outputStream = fileSystem.create(new Path(hdfsDirPath, name));
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
    
    @Override
    public IndexInput openInput(String name) throws IOException {
        return openInput(name, BUFFER_SIZE);
    }

    @Override
    public IndexInput openInput(final String name, int bufferSize) throws IOException {
        final FSDataInputStream inputStream = fileSystem.open(new Path(hdfsDirPath, name));
        return new BufferedIndexInput(bufferSize) {

            private long length = fileLength(name);

            @Override
            public long length() {
                return length;
            }

            @Override
            public void close() throws IOException {
                inputStream.close();
            }

            @Override
            protected void seekInternal(long pos) throws IOException {

            }

            @Override
            protected void readInternal(byte[] b, int offset, int length) throws IOException {
                synchronized (inputStream) {
                    long position = getFilePointer();
                    inputStream.seek(position);
                    inputStream.read(b, offset, length);
                }
            }
        };
    }

    @Override
    public void deleteFile(String name) throws IOException {
        fileSystem.delete(new Path(hdfsDirPath, name), false);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return fileSystem.exists(new Path(hdfsDirPath, name));
    }

    @Override
    public long fileLength(String name) throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(hdfsDirPath, name));
        return fileStatus.getLen();
    }

    @Override
    public long fileModified(String name) throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(hdfsDirPath, name));
        return fileStatus.getModificationTime();
    }

    @Override
    public String[] listAll() throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(hdfsDirPath);
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

}
