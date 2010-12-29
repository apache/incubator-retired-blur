package com.nearinfinity.blur.lucene.store;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;

public class MixedFSDirectory extends FSDirectory {

    private static final Log LOG = LogFactory.getLog(MixedFSDirectory.class);

    private FSDirectory baseDir;
    private MMapDirectory mMapDir;
    
    public MixedFSDirectory(File path, LockFactory lockFactory) throws IOException {
        super(path, lockFactory);
        baseDir = FSDirectory.open(path, lockFactory);
        mMapDir = new MMapDirectory(path);
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        return baseDir.createOutput(name);
    }

    @Override
    public IndexInput openInput(String name) throws IOException {
        if (isMMapFile(name)) {
            return startMapper(name, mMapDir.openInput(name));
        }
        return baseDir.openInput(name);
    }

    private IndexInput startMapper(final String name, IndexInput openInput) {
        final IndexInput input = (IndexInput) openInput.clone();
        run("MMAP-" + name, new Runnable() {
            @Override
            public void run() {
                long length = input.length();
                long total = 0;
                try {
                    for (long l = 0; l < length; l++) {
                        total += input.readByte();
                    }
                } catch (IOException e) {
                    LOG.error("Error reading file [" + name + "] into mmap", e);
                }
                LOG.info("Mmap for file [" + name +
                		"] complete with value [" + total + 
                		"]");
            }
        });
        return openInput;
    }

    private void run(String name, Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName(name);
        thread.setDaemon(true);
        thread.setPriority(Thread.MIN_PRIORITY);
        thread.start();
    }

    private boolean isMMapFile(String name) {
        if (name.endsWith(".tis") || name.endsWith(".frq")) {
            return true;
        }
        return false;
    }
}
