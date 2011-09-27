package com.nearinfinity.blur.store;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public abstract class DirectIODirectory extends Directory {
  
  public abstract IndexOutput createOutputDirectIO(String name) throws IOException;

  public abstract IndexInput openInputDirectIO(String name) throws IOException;

  public static DirectIODirectory wrap(Directory dir) {
    return null;
  }

  public static class DirectIODirectoryWrapper extends DirectIODirectory {
    
    private Directory _directory;

    public DirectIODirectoryWrapper(Directory directory) {
      _directory = directory;
    }

    public void close() throws IOException {
      _directory.close();
    }

    public IndexOutput createOutput(String arg0) throws IOException {
      return _directory.createOutput(arg0);
    }

    public void deleteFile(String arg0) throws IOException {
      _directory.deleteFile(arg0);
    }

    public boolean fileExists(String arg0) throws IOException {
      return _directory.fileExists(arg0);
    }

    public long fileLength(String arg0) throws IOException {
      return _directory.fileLength(arg0);
    }

    public long fileModified(String arg0) throws IOException {
      return _directory.fileModified(arg0);
    }

    public String[] listAll() throws IOException {
      return _directory.listAll();
    }

    public IndexInput openInput(String arg0) throws IOException {
      return _directory.openInput(arg0);
    }

    @SuppressWarnings("deprecation")
    public void touchFile(String arg0) throws IOException {
      _directory.touchFile(arg0);
    }

    @Override
    public IndexOutput createOutputDirectIO(String name) throws IOException {
      return createOutput(name);
    }

    @Override
    public IndexInput openInputDirectIO(String name) throws IOException {
      return openInput(name);
    }

  }

}
