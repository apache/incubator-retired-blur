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

package com.nearinfinity.blur.store.replication;


import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.store.cache.LocalFileCache;

public class ReplicaTestSamples {
    
    private static final long DELAY = 0;
    private static final int CHAOS_NUMBER = 1000;

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name","hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsDirPath = new Path("hdfs://localhost:9000/blur/tables/table/shard-00000");

        LocalFileCache localFileCache = new LocalFileCache();
        localFileCache.setPotentialFiles(new File("./tmp/cache1/"),new File("./tmp/cache2/"));
        localFileCache.init();
        
        Progressable progressable = new Progressable() {
            @Override
            public void progress() {
            }
        };
        
        ReplicationDaemon replicationDaemon = new ReplicationDaemon();
        replicationDaemon.setLocalFileCache(localFileCache);
        replicationDaemon.init();
        
        ReplicaHdfsDirectory directory = new ReplicaHdfsDirectory("table", "shard-00000", hdfsDirPath, fileSystem, 
                localFileCache, new NoLockFactory(), progressable, replicationDaemon, new ReplicationStrategy() {
            @Override
            public boolean replicateLocally(String table, String name) {
                if (name.endsWith(".fdt")) {
                    return false;
                }
                return true;
            }
        });
        
//        WritableHdfsDirectory directory = new WritableHdfsDirectory("table", "shard-00000", hdfsDirPath, fileSystem, localFileCache, new NoLockFactory(), progressable);
        
        createIndex(directory);
        
        for (String f : directory.listAll()) {
            System.out.println(f);
        }
        
//        Directory directory = FSDirectory.open(new File("./tmp-indexing"));
//        SimpleFSDirectory directory = new SimpleFSDirectory(new File("./tmp-indexing"));
//        MMapDirectory directory = new MMapDirectory(new File("./tmp-indexing"));
        
//        IndexReader reader = IndexReader.open(directory);
//        for (int i = 0; i < 10; i++) {
//            runTest(reader);
//        }
    }
    
    private static void runTest(final IndexReader reader) throws IOException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        long s = System.currentTimeMillis();
                        TermEnum terms = reader.terms();
                        long t = 0;
                        long count = 0;
                        while (terms.next()) {
                            t += terms.term().hashCode();
                            count++;
                        }
                        terms.close();
                        System.out.println(System.currentTimeMillis() - s + " " + t + " " + count);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static void slowDown() {
        try {
            Thread.sleep(DELAY);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static File rm(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
        return file;
    }

    private static void createIndex(Directory dir) throws IOException {
        IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30),MaxFieldLength.UNLIMITED);
        writer.setUseCompoundFile(false);
        for (int j = 0; j < 10; j++) {
            writer.addIndexesNoOptimize(index());
            writer.commit();
        }
        writer.optimize(2);
        writer.close();
    }

    private static Directory index() throws IOException {
        RAMDirectory dir = new RAMDirectory();
        IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30),MaxFieldLength.UNLIMITED);
        writer.setUseCompoundFile(false);
        for (int i = 0; i < 100000; i++) {
            writer.addDocument(genDoc());
        }
        writer.optimize();
        writer.close();
        return dir;
    }

    private static Document genDoc() {
        Document document = new Document();
        document.add(new Field("f1",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("f2",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
        document.add(new Field("f3",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
        return document;
    }

    private static Directory slowDir(Directory dir) {
        return new SlowDir(dir);
    }
    
    public static class SlowIndexInput extends IndexInput {
        
        private IndexInput indexInput;
        
        public SlowIndexInput(IndexInput indexInput) {
            this.indexInput = indexInput;
        }

        public Object clone() {
            SlowIndexInput clone = (SlowIndexInput) super.clone();
            clone.indexInput = (IndexInput) indexInput.clone();
            return clone;
        }

        public void close() throws IOException {
            indexInput.close();
        }

        public boolean equals(Object obj) {
            return indexInput.equals(obj);
        }

        public long getFilePointer() {
            return indexInput.getFilePointer();
        }

        public int hashCode() {
            return indexInput.hashCode();
        }

        public long length() {
            return indexInput.length();
        }

        public byte readByte() throws IOException {
            slowDown();
            return indexInput.readByte();
        }

        public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
            slowDown();
            indexInput.readBytes(b, offset, len, useBuffer);
        }

        public void readBytes(byte[] b, int offset, int len) throws IOException {
            slowDown();
            indexInput.readBytes(b, offset, len);
        }

        @SuppressWarnings("deprecation")
        public void readChars(char[] buffer, int start, int length) throws IOException {
            slowDown();
            indexInput.readChars(buffer, start, length);
        }

        public int readInt() throws IOException {
            slowDown();
            return indexInput.readInt();
        }

        public long readLong() throws IOException {
            slowDown();
            return indexInput.readLong();
        }

        public String readString() throws IOException {
            slowDown();
            return indexInput.readString();
        }

        public Map<String, String> readStringStringMap() throws IOException {
            return indexInput.readStringStringMap();
        }

        public int readVInt() throws IOException {
            slowDown();
            return indexInput.readVInt();
        }

        public long readVLong() throws IOException {
            slowDown();
            return indexInput.readVLong();
        }

        public void seek(long pos) throws IOException {
            indexInput.seek(pos);
        }

        public void setModifiedUTF8StringsMode() {
            indexInput.setModifiedUTF8StringsMode();
        }

        @SuppressWarnings("deprecation")
        public void skipChars(int length) throws IOException {
            indexInput.skipChars(length);
        }

        public String toString() {
            return indexInput.toString();
        }
        
    }
    
    public static class SlowDir extends Directory {
        
        private Directory directory;

        public SlowDir(Directory dir) {
            this.directory = dir;
        }

        public void clearLock(String name) throws IOException {
            directory.clearLock(name);
        }

        public void close() throws IOException {
            directory.close();
        }

        public IndexOutput createOutput(String name) throws IOException {
            return directory.createOutput(name);
        }

        public void deleteFile(String name) throws IOException {
            directory.deleteFile(name);
        }

        public boolean equals(Object obj) {
            return directory.equals(obj);
        }

        public boolean fileExists(String name) throws IOException {
            return directory.fileExists(name);
        }

        public long fileLength(String name) throws IOException {
            return directory.fileLength(name);
        }

        public long fileModified(String name) throws IOException {
            return directory.fileModified(name);
        }

        public LockFactory getLockFactory() {
            return directory.getLockFactory();
        }

        public String getLockID() {
            return directory.getLockID();
        }

        public int hashCode() {
            return directory.hashCode();
        }

        public String[] listAll() throws IOException {
            return directory.listAll();
        }

        public Lock makeLock(String name) {
            return directory.makeLock(name);
        }

        public IndexInput openInput(String name, int bufferSize) throws IOException {
            return new SlowIndexInput(directory.openInput(name, bufferSize));
        }

        public IndexInput openInput(String name) throws IOException {
            return new SlowIndexInput(directory.openInput(name));
        }

        public void setLockFactory(LockFactory lockFactory) {
            directory.setLockFactory(lockFactory);
        }

        public void sync(String name) throws IOException {
            directory.sync(name);
        }

        public String toString() {
            return directory.toString();
        }

        public void touchFile(String name) throws IOException {
            directory.touchFile(name);
        }
        
    }
    
    private static Directory chaosDir(FSDirectory dir) {
        return new ChaosDir(dir);
    }

    public static class ChaosDir extends Directory {

        private Directory dir;

        public ChaosDir(Directory dir) {
            this.dir = dir;
        }

        public void clearLock(String name) throws IOException {
            dir.clearLock(name);
        }

        public void close() throws IOException {
            dir.close();
        }

        public IndexOutput createOutput(String name) throws IOException {
            return dir.createOutput(name);
        }

        public void deleteFile(String name) throws IOException {
            dir.deleteFile(name);
        }

        public boolean equals(Object obj) {
            return dir.equals(obj);
        }

        public boolean fileExists(String name) throws IOException {
            return dir.fileExists(name);
        }

        public long fileLength(String name) throws IOException {
            return dir.fileLength(name);
        }

        public long fileModified(String name) throws IOException {
            return dir.fileModified(name);
        }

        public LockFactory getLockFactory() {
            return dir.getLockFactory();
        }

        public String getLockID() {
            return dir.getLockID();
        }

        public int hashCode() {
            return dir.hashCode();
        }

        public String[] listAll() throws IOException {
            return dir.listAll();
        }

        public Lock makeLock(String name) {
            return dir.makeLock(name);
        }

        public IndexInput openInput(String name, int bufferSize) throws IOException {
            return chaosIndexInput(dir.openInput(name, bufferSize));
        }

        public IndexInput openInput(String name) throws IOException {
            return chaosIndexInput(dir.openInput(name));
        }

        public void setLockFactory(LockFactory lockFactory) {
            dir.setLockFactory(lockFactory);
        }

        public void sync(String name) throws IOException {
            dir.sync(name);
        }

        public String toString() {
            return dir.toString();
        }

        public void touchFile(String name) throws IOException {
            dir.touchFile(name);
        }
        
        
    }

    public static IndexInput chaosIndexInput(IndexInput openInput) {
        return new ChaosIndexInput(openInput);
    }
    
    public static class ChaosIndexInput extends IndexInput {
        
        private IndexInput indexInput;
        private Random random = new Random();
        private int accessesToError;
        private int count;

        public ChaosIndexInput(IndexInput input) {
            this.indexInput = input;
            accessesToError = random.nextInt(CHAOS_NUMBER) + 500;
        }
        
        private void throwIOException() throws IOException {
            count++;
            if (count >= accessesToError) {
                throw new IOException("Chaos Monkey....  HA! Count [" + count +
                		"] Error [" + accessesToError +
                		"]");
            }
        }

        public Object clone() {
            return super.clone();
        }

        public void close() throws IOException {
            throwIOException();
            indexInput.close();
        }

        public boolean equals(Object obj) {
            return indexInput.equals(obj);
        }

        public long getFilePointer() {
            return indexInput.getFilePointer();
        }

        public int hashCode() {
            return indexInput.hashCode();
        }

        public long length() {
            return indexInput.length();
        }

        public byte readByte() throws IOException {
            throwIOException();
            return indexInput.readByte();
        }

        public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
            throwIOException();
            indexInput.readBytes(b, offset, len, useBuffer);
        }

        public void readBytes(byte[] b, int offset, int len) throws IOException {
            throwIOException();
            indexInput.readBytes(b, offset, len);
        }

        @SuppressWarnings("deprecation")
        public void readChars(char[] buffer, int start, int length) throws IOException {
            throwIOException();
            indexInput.readChars(buffer, start, length);
        }

        public int readInt() throws IOException {
            throwIOException();
            return indexInput.readInt();
        }

        public long readLong() throws IOException {
            throwIOException();
            return indexInput.readLong();
        }

        public String readString() throws IOException {
            throwIOException();
            return indexInput.readString();
        }

        public Map<String, String> readStringStringMap() throws IOException {
            throwIOException();
            return indexInput.readStringStringMap();
        }

        public int readVInt() throws IOException {
            throwIOException();
            return indexInput.readVInt();
        }

        public long readVLong() throws IOException {
            throwIOException();
            return indexInput.readVLong();
        }

        public void seek(long pos) throws IOException {
            throwIOException();
            indexInput.seek(pos);
        }

        public void setModifiedUTF8StringsMode() {
            indexInput.setModifiedUTF8StringsMode();
        }

        @SuppressWarnings("deprecation")
        public void skipChars(int length) throws IOException {
            throwIOException();
            indexInput.skipChars(length);
        }

        public String toString() {
            return indexInput.toString();
        }
        
    }
}
