/**
ing ok * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.blur.thrift;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.user.User;
import org.apache.blur.utils.BlurConstants;

public class BlurClusterTestSecurity extends BlurClusterTestBase {

  private static final String DISCOVER = "discover";
  private static final String READ = "read";

  @Override
  protected void setupTableProperties(TableDescriptor tableDescriptor) {
    tableDescriptor.putToTableProperties(BlurConstants.BLUR_RECORD_SECURITY, "true");
  }

  @Override
  protected RowMutation mutate(RowMutation rowMutation) {
    List<RecordMutation> mutations = rowMutation.getRecordMutations();
    for (RecordMutation mutation : mutations) {
      Record record = mutation.getRecord();
      record.addToColumns(new Column("acl-read", READ));
      record.addToColumns(new Column("acl-discover", DISCOVER));
    }
    return rowMutation;
  }

  @Override
  protected void postTableCreate(TableDescriptor tableDescriptor, Iface client) {
    String name = tableDescriptor.getName();
    try {
      client.addColumnDefinition(name, new ColumnDefinition("test", "acl-read", null, false, "acl-read", null, false));
      client.addColumnDefinition(name, new ColumnDefinition("test", "acl-discover", null, false, "acl-discover", null,
          false));
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected User getUser() {
    return new User("testuser", getUserAttributes());
  }

  @Override
  protected Map<String, String> getUserAttributes() {
    Map<String, String> attributes = new HashMap<String, String>();
    attributes.put(BlurConstants.ACL_READ, READ);
    attributes.put(BlurConstants.ACL_DISCOVER, DISCOVER);
    return attributes;
  }

}
