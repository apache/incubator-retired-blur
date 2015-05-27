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
package org.apache.blur.utils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.blur.BlurConfiguration;
import org.junit.Before;
import org.junit.Test;

public class BlurConstantsTest {

  private Set<String> _nonPropertyConstants;
  private Set<String> _emptyDefaultProperties;

  @Before
  public void setup() {
    _nonPropertyConstants = new HashSet<String>();
    _nonPropertyConstants.addAll(Arrays.asList("CONTROLLER", "SHARD", "SHARD_PREFIX", "PRIME_DOC", "PRIME_DOC_VALUE",
        "ROW_ID", "RECORD_ID", "FIELDS", "FAMILY", "DEFAULT_FAMILY", "SUPER", "SEP", "BLUR_HOME",
        "DELETE_MARKER_VALUE", "DELETE_MARKER", "BLUR_ZOOKEEPER_TIMEOUT_DEFAULT", "BLUR_THRIFT_DEFAULT_MAX_FRAME_SIZE",
        "ZK_WAIT_TIME", "ACL_DISCOVER", "ACL_READ", "FAST_DECOMPRESSION", "FAST", "HIGH_COMPRESSION", "DEFAULT_VALUE",
        "OFF_HEAP", "DEFAULT", "BLUR_CLUSTER", "BLUR_NODENAME", "BLUR_HTTP_STATUS_RUNNING_PORT",
        "SHARED_MERGE_SCHEDULER_PREFIX"));
    _emptyDefaultProperties = new HashSet<String>();
    _emptyDefaultProperties.addAll(Arrays.asList("BLUR_HDFS_TRACE_PATH", "BLUR_SHARD_HOSTNAME",
        "BLUR_SHARD_BLOCK_CACHE_TOTAL_SIZE", "BLUR_CONTROLLER_HOSTNAME", "BLUR_SHARD_READ_INTERCEPTOR",
        "BLUR_SHARD_BLOCK_CACHE_V2_READ_CACHE_EXT", "BLUR_SHARD_BLOCK_CACHE_V2_WRITE_CACHE_EXT",
        "BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE_PREFIX", "BLUR_FIELDTYPE", "BLUR_SHARD_FILTERED_SERVER_CLASS",
        "BLUR_CONTROLLER_FILTERED_SERVER_CLASS", "BLUR_COMMAND_LIB_PATH", "BLUR_TMP_PATH", "BLUR_SECURITY_SASL_TYPE",
        "BLUR_SECUTIRY_SASL_CUSTOM_CLASS", "BLUR_SECURITY_SASL_LDAP_DOMAIN", "BLUR_SECURITY_SASL_LDAP_BASEDN",
        "BLUR_SECURITY_SASL_LDAP_URL", "BLUR_SERVER_SECURITY_FILTER_CLASS", "BLUR_FILTER_ALIAS",
        "BLUR_BULK_UPDATE_WORKING_PATH"));
  }

  @Test
  public void test() throws IllegalArgumentException, IllegalAccessException, IOException {
    BlurConfiguration blurConfiguration = new BlurConfiguration();
    Field[] declaredFields = BlurConstants.class.getDeclaredFields();

    String string = blurConfiguration.get("blur.shard.block.cache.v2.cacheBlockSize.<ext>");
    System.out.println("[" + string + "]");
    String string2 = blurConfiguration.getProperties().get("blur.shard.block.cache.v2.cacheBlockSize.<ext>");
    System.out.println("[" + string2 + "]");

    for (Field field : declaredFields) {
      String constantName = field.getName();
      if (_nonPropertyConstants.contains(constantName)) {
        continue;
      }
      Object propertyName = field.get(null);
      if (propertyName instanceof String) {
        String propertyNameStr = propertyName.toString();
        String value = blurConfiguration.get(propertyNameStr);
        if (_emptyDefaultProperties.contains(constantName)) {
          assertNull(constantName + "=>" + propertyNameStr, value);
        } else {
          assertNotNull(constantName + "=>" + propertyNameStr, value);
        }
      } else {
        fail(constantName);
      }
    }
  }
}
