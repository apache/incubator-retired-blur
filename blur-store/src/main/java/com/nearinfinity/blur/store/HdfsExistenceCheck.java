package com.nearinfinity.blur.store;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsExistenceCheck implements ExistenceCheck {
    
    private Path basePath;
    private FileSystem fileSystem;

    public HdfsExistenceCheck(FileSystem fileSystem, Path basePath) {
        this.basePath = basePath;
        this.fileSystem = fileSystem;
    }

    @Override
    public boolean existsInBase(String dirName, String name) throws IOException {
        Path shardPath = HdfsUtil.getHdfsPath(basePath, dirName);
        if (fileSystem.exists(new Path(shardPath,name))) {
            return true;
        }
        return false;
    }

}
