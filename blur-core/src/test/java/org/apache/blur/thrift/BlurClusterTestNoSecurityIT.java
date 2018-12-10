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
package org.apache.blur.thrift;

import java.util.HashMap;
import java.util.Map;

import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.user.User;

public class BlurClusterTestNoSecurityIT extends BlurClusterTestBase {

  @Override
  protected void setupTableProperties(TableDescriptor tableDescriptor) {
    // do nothing
  }

  @Override
  protected RowMutation mutate(RowMutation rowMutation) {
    return rowMutation;
  }

  @Override
  protected void postTableCreate(TableDescriptor tableDescriptor, Iface client) {

  }

  @Override
  protected User getUser() {
    return null;
  }

  @Override
  protected Map<String, String> getUserAttributes() {
    return new HashMap<String, String>();
  }

}
