/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.store.hdfs;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

public class SoftlinkHdfsDirectory extends HdfsDirectory {

  private static final Log LOG = LogFactory.getLog(HdfsDirectory.class);

  private static final String UTF_8 = "UTF-8";
  private static final String EXT = ".blur_lnk";
  private final Path _storePath;
  private final Path _linkPath;

  /**
   * Creates a new SoftlinkHdfsDirectory.
   * 
   * @param configuration
   *          the {@link Configuration} object.
   * @param storePath
   *          the path where the data is actually stored.
   * @param linkPath
   *          the path where the links are stored.
   * @throws IOException
   */
  public SoftlinkHdfsDirectory(Configuration configuration, Path storePath, Path linkPath) throws IOException {
    super(configuration, linkPath);
    FileSystem fileSystem = storePath.getFileSystem(configuration);
    _storePath = fileSystem.makeQualified(storePath);
    _linkPath = fileSystem.makeQualified(linkPath);
  }

  @Override
  protected FSDataOutputStream openForOutput(String name) throws IOException {
    createLinkForNewFile(name);
    return _fileSystem.create(getDataPath(name));
  }

  private void createLinkForNewFile(String name) throws IOException {
    String uuid = UUID.randomUUID().toString();
    Path dataPath = new Path(_storePath, uuid);
    createLinkForNewFile(name, dataPath.toUri());
  }

  private void createLinkForNewFile(String name, URI uri) throws IOException, UnsupportedEncodingException {
    String uriStr = uri.toString();
    Path linkPath = createLinkPath(name);
    FSDataOutputStream outputStream = _fileSystem.create(linkPath, false);
    outputStream.write(uriStr.getBytes(UTF_8));
    outputStream.close();
  }

  private Path getDataPath(String name) throws IOException {
    Path linkPath = createLinkPath(name);
    boolean exists = _fileSystem.exists(linkPath);
    if (exists) {
      return new Path(getUri(linkPath));
    } else {
      return new Path(_linkPath, name);
    }
  }

  private URI getUri(Path linkPath) throws IOException {
    FileStatus fileStatus = _fileSystem.getFileStatus(linkPath);
    byte[] buf = new byte[(int) fileStatus.getLen()];
    FSDataInputStream inputStream = _fileSystem.open(linkPath);
    inputStream.readFully(buf);
    inputStream.close();
    try {
      return new URI(new String(buf, UTF_8));
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private Path getLinkPath(String name) throws IOException {
    Path linkPath = createLinkPath(name);
    boolean exists = _fileSystem.exists(linkPath);
    if (exists) {
      return new Path(_linkPath, name + EXT);
    } else {
      return new Path(_linkPath, name);
    }
  }

  private Path createLinkPath(String name) {
    return new Path(_linkPath, name + EXT);
  }

  private String removeLinkExtensionSuffix(String name) {
    if (name.endsWith(EXT)) {
      return name.substring(0, name.length() - EXT.length());
    }
    return name;
  }

  @Override
  public String[] listAll() throws IOException {
    LOG.debug("listAll [{0}]", _path);
    FileStatus[] files = _fileSystem.listStatus(_path, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        try {
          return _fileSystem.isFile(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    String[] result = new String[files.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = removeLinkExtensionSuffix(files[i].getPath().getName());
    }
    return result;
  }

  @Override
  protected FSDataInputStream openForInput(String name) throws IOException {
    return _fileSystem.open(getDataPath(name));
  }

  @Override
  protected boolean exists(String name) throws IOException {
    return _fileSystem.exists(getLinkPath(name));
  }

  @Override
  protected void delete(String name) throws IOException {
    _fileSystem.delete(getLinkPath(name), true);
  }

  @Override
  protected long length(String name) throws IOException {
    return _fileSystem.getFileStatus(getDataPath(name)).getLen();
  }

  @Override
  protected long fileModified(String name) throws IOException {
    return _fileSystem.getFileStatus(getDataPath(name)).getModificationTime();
  }

  @Override
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    if (to instanceof DirectoryDecorator) {
      copy(((DirectoryDecorator) to).getOriginalDirectory(), src, dest, context);
      return;
    } else if (to instanceof SoftlinkHdfsDirectory) {
      LOG.warn("This needs to be tested....");
      SoftlinkHdfsDirectory softlinkHdfsDirectory = (SoftlinkHdfsDirectory) to;
      if (canQuickCopy(softlinkHdfsDirectory, src)) {
        Path linkPath = getLinkPath(src);
        softlinkHdfsDirectory.quickCopy(linkPath, dest);
      }
    }
    slowCopy(to, src, dest, context);
  }

  private void quickCopy(Path linkPath, String dest) throws IOException {
    URI uri = getUri(linkPath);
    createLinkForNewFile(dest, uri);
  }

  private boolean canQuickCopy(SoftlinkHdfsDirectory to, String src) throws IOException {
    if (to._storePath.equals(_storePath)) {
      return _fileSystem.exists(createLinkPath(src));
    }
    return false;
  }

}
