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

import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;

import com.google.common.base.Strings;

public class ShardUtil {
  
  public static String getShardName(int id) {
    return getShardName(BlurConstants.SHARD_PREFIX, id);
  }

  public static String getShardName(String prefix, int id) {
    return prefix + buffer(id, 8);
  }

  private static String buffer(int value, int length) {
    String str = Integer.toString(value);
    while (str.length() < length) {
      str = "0" + str;
    }
    return str;
  }

  public static int getShardIndex(String shard) {
    int index = shard.indexOf('-');
    return Integer.parseInt(shard.substring(index + 1));
  }

  public static void validateRowIdAndRecord(String rowId, Record record) {
    if (!validate(record.family)) {
      throw new IllegalArgumentException("Invalid column family name [ " + record.family
          + " ]. It should contain only this pattern [A-Za-z0-9_-]");
    }

    for (Column column : record.getColumns()) {
      if (!validate(column.name)) {
        throw new IllegalArgumentException("Invalid column name [ " + column.name
            + " ]. It should contain only this pattern [A-Za-z0-9_-]");
      }
    }
  }

  public static boolean validate(String s) {
    if(Strings.isNullOrEmpty(s)) {
      return false;
    }
    int length = s.length();
    for (int i = 0; i < length; i++) {
      char c = s.charAt(i);
      if (!validate(c)) {
        return false;
      }
    }
    return true;
  }

  private static boolean validate(char c) {
    if (c >= 'a' && c <= 'z') {
      return true;
    }
    if (c >= 'A' && c <= 'Z') {
      return true;
    }
    if (c >= '0' && c <= '9') {
      return true;
    }
    switch (c) {
    case '_':
      return true;
    case '-':
      return true;
    default:
      return false;
    }
  }

  public static void validateTableName(String tableName) {
    if (!validate(tableName)) {
      throw new IllegalArgumentException("Invalid table name [ " + tableName
          + " ]. It should contain only this pattern [A-Za-z0-9_-]");
    }
  }

  public static void validateShardName(String shardName) {
    if (!validate(shardName)) {
      throw new IllegalArgumentException("Invalid shard name [ " + shardName
          + " ]. It should contain only this pattern [A-Za-z0-9_-]");
    }
  }
}
