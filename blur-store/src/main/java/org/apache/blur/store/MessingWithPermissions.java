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
package org.apache.blur.store;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class MessingWithPermissions {

  public static void main(String[] args) throws IOException, InterruptedException {
    UserGroupInformation blur = UserGroupInformation.createRemoteUser("blur");
    final Path path = new Path("/permission_test/tables");
    final Configuration configuration = new Configuration();
    blur.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fileSystem = path.getFileSystem(configuration);
        FileStatus[] listStatus = fileSystem.listStatus(path);
        for (FileStatus status : listStatus) {
          System.out.println(status.getPath());
        }
        return null;
      }
    });
  }

}
