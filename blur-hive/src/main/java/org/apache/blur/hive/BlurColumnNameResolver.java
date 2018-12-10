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
package org.apache.blur.hive;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.hive.serde2.SerDeException;

public class BlurColumnNameResolver {

  private Map<String, String> _blurToHive = new HashMap<String, String>();
  private Map<String, String> _hiveToBlur = new HashMap<String, String>();

  public BlurColumnNameResolver(Collection<ColumnDefinition> values) throws SerDeException {
    for (ColumnDefinition columnDefinition : values) {
      String blurName = columnDefinition.getColumnName();
      if (blurName.contains("-")) {
        String derivedHiveName = blurName.replace("-", "_");
        addMapping(blurName, derivedHiveName);
      } else {
        addMapping(blurName);
      }
    }
    addMapping(BlurConstants.ROW_ID);
    addMapping(BlurConstants.RECORD_ID);
  }

  private void addMapping(String blurName) throws SerDeException {
    addMapping(blurName, blurName);
  }

  private void addMapping(String blurName, String derivedHiveName) throws SerDeException {
    if (_blurToHive.containsKey(derivedHiveName)) {
      throw new SerDeException("Name collision while trying to map blur name [" + blurName + "] to hive name ["
          + derivedHiveName + "].  Name already exists.");
    } else if (_hiveToBlur.containsKey(derivedHiveName)) {
      throw new SerDeException("Name collision while trying to map blur name [" + blurName + "] to hive name ["
          + derivedHiveName + "].  Name already exists.");
    } else {
      _blurToHive.put(blurName, derivedHiveName);
      _hiveToBlur.put(derivedHiveName, blurName);
    }
  }

  public String fromHiveToBlur(String hive) {
    return _hiveToBlur.get(hive);
  }

  public String fromBlurToHive(String blur) {
    return _blurToHive.get(blur);
  }

}
