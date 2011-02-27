package com.nearinfinity.blur.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import com.nearinfinity.blur.store.cache.HdfsUtil;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.indexinput.FileIndexInput;
import com.nearinfinity.blur.store.indexinput.FileNIOIndexInput;

public class WritableHdfsDirectory extends HdfsDirectory {

    private static final String SEGMENTS_GEN = "segments.gen";

    private static final Log LOG = LogFactory.getLog(WritableHdfsDirectory.class);

    protected LocalFileCache localFileCache;
    protected String dirName;
    protected Progressable progressable;
    
    public WritableHdfsDirectory(String table, String shard, Path hdfsDirPath, FileSystem fileSystem,
            LocalFileCache localFileCache, LockFactory lockFactory) throws IOException {
        this(table,shard,hdfsDirPath,fileSystem,localFileCache,lockFactory,new Progressable() {
            @Override
            public void progress() {
                
            }
        });
    }

    public WritableHdfsDirectory(String table, String shard, Path hdfsDirPath, FileSystem fileSystem,
            LocalFileCache localFileCache, LockFactory lockFactory, Progressable progressable) throws IOException {
        super(hdfsDirPath, fileSystem);
        this.dirName = HdfsUtil.getDirName(table, shard);
        this.progressable = progressable;
        File segments = localFileCache.getLocalFile(dirName, SEGMENTS_GEN);
        if (segments.exists()) {
            segments.delete();
        }
        this.localFileCache = localFileCache;
        setLockFactory(lockFactory);
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        File file = localFileCache.getLocalFile(dirName, name);
        if (file.exists()) {
            file.delete();
        }
        LOG.info("Opening local file for writing [{0}]",file.getAbsolutePath());
        return new FileIndexOutput(progressable,file);
    }

    @Override
    public void sync(String name) throws IOException {
        File file = localFileCache.getLocalFile(dirName, name);
        FSDataOutputStream outputStream = super.getOutputStream(name + ".sync");
        FileInputStream inputStream = new FileInputStream(file);
        LOG.info("Syncing local file [{0}] to [{1}]",file.getAbsolutePath(),hdfsDirPath);
        byte[] buffer = new byte[BUFFER_SIZE];
        int num;
        while ((num = inputStream.read(buffer)) != -1) {
            progressable.progress();
            outputStream.write(buffer, 0, num);
        }
        outputStream.close();
        inputStream.close();
        rename(name + ".sync",name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (super.fileExists(name)) {
            super.deleteFile(name);
        }
        File localFile = localFileCache.getLocalFile(dirName, name);
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
            return localFileCache.getLocalFile(dirName, name).length();
        }
    }

    @Override
    public long fileModified(String name) throws IOException {
        if (super.fileExists(name)) {
            return super.fileModified(name);
        } else {
            return localFileCache.getLocalFile(dirName, name).lastModified();
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
        return localFileCache.getLocalFile(dirName, name).exists();
    }

    public IndexInput openFromLocal(String name, int bufferSize) throws IOException {
        if (Constants.WINDOWS) {
            return new FileIndexInput(localFileCache.getLocalFile(dirName, name), bufferSize);
        } else {
            return new FileNIOIndexInput(localFileCache.getLocalFile(dirName, name), bufferSize);
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
