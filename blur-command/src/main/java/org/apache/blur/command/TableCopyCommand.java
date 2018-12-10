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
package org.apache.blur.command;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.blur.command.annotation.Description;
import org.apache.blur.command.annotation.OptionalArgument;
import org.apache.blur.command.annotation.RequiredArgument;
import org.apache.blur.command.commandtype.IndexReadCommandSingleTable;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.hdfs.DirectoryDecorator;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.store.hdfs_v2.JoinDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

@Description("Copies the given table to another location.")
public class TableCopyCommand extends IndexReadCommandSingleTable<Long> {

  private static final Log LOG = LogFactory.getLog(TableCopyCommand.class);

  @RequiredArgument("The hdfs destination uri.  (e.g. hdfs://namenode/path)")
  private String destUri;

  @RequiredArgument("The hdfs user to run copy command.")
  private String user;

  @OptionalArgument("If enabled, the copy will resume by removing files that did not complete and recopying.")
  private boolean resume = false;

  @Override
  public String getName() {
    return "table-copy";
  }

  @Override
  public Long execute(IndexContext context) throws IOException {
    final Configuration configuration = context.getTableContext().getConfiguration();
    final IndexReader indexReader = context.getIndexReader();
    final Shard shard = context.getShard();
    UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(user);
    try {
      return remoteUser.doAs(new PrivilegedExceptionAction<Long>() {
        @Override
        public Long run() throws Exception {
          Path path = new Path(destUri);
          Directory srcDirectory = getDiretory(indexReader);
          HdfsDirectory destDirectory = new HdfsDirectory(configuration, new Path(path, shard.getShard()));
          long total = 0;
          for (String srcFile : srcDirectory.listAll()) {
            if (destDirectory.fileExists(srcFile)) {
              LOG.info("File [{0}] already exists in dest directory.");
              long srcFileLength = srcDirectory.fileLength(srcFile);
              long destFileLength = destDirectory.fileLength(srcFile);
              if (srcFileLength != destFileLength) {
                LOG.info("Deleting file [{0}] length of [{1}] is not same as source [{2}].", srcFile, srcFileLength,
                    destFileLength);
                destDirectory.deleteFile(srcFile);
              } else {
                continue;
              }
            }
            LOG.info("Copying file [{0}] to dest directory.", srcFile);
            total += copy(srcFile, srcDirectory, destDirectory);
          }
          return total;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private long copy(String file, Directory srcDirectory, HdfsDirectory destDirectory) throws IOException {
    long fileLength = srcDirectory.fileLength(file);
    IOContext context = IOContext.DEFAULT;
    IndexOutput os = null;
    IndexInput is = null;
    IOException priorException = null;
    try {
      os = destDirectory.createOutput(file, context);
      is = srcDirectory.openInput(file, context);
      os.copyBytes(is, is.length());
    } catch (IOException ioe) {
      priorException = ioe;
    } finally {
      boolean success = false;
      try {
        IOUtils.closeWhileHandlingException(priorException, os, is);
        success = true;
      } finally {
        if (!success) {
          try {
            destDirectory.deleteFile(file);
          } catch (Throwable t) {
          }
        }
      }
    }
    return fileLength;
  }

  private Directory getStorageDir(Directory directory) throws IOException {
    if (directory instanceof DirectoryDecorator) {
      DirectoryDecorator directoryDecorator = (DirectoryDecorator) directory;
      return getStorageDir(directoryDecorator.getOriginalDirectory());
    }
    if (directory instanceof JoinDirectory) {
      return directory;
    }
    if (directory instanceof HdfsDirectory) {
      return directory;
    }
    if (directory instanceof FSDirectory) {
      return directory;
    }
    throw new IOException("Directory [" + directory + "] not supported.");
  }

  private Directory getDiretory(IndexReader indexReader) throws IOException {
    if (indexReader instanceof DirectoryReader) {
      DirectoryReader reader = (DirectoryReader) indexReader;
      return getStorageDir(reader.directory());
    }
    throw new IOException("Reader Not DirectoryReader.");
  }

  public String getDestUri() {
    return destUri;
  }

  public void setDestUri(String destUri) {
    this.destUri = destUri;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public boolean isResume() {
    return resume;
  }

  public void setResume(boolean resume) {
    this.resume = resume;
  }

}
