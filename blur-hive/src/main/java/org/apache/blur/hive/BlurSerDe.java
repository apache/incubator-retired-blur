/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.blur.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.NullStructSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

public class BlurSerDe extends AbstractSerDe {
  
  private static final Log LOG = LogFactory.getLog(BlurSerDe.class);

  public static final String BLUR_MR_UPDATE_DISABLED = "blur.mr.update.disabled";
  public static final String BLUR_BLOCKING_APPLY = "blur.blocking.apply";
  public static final String BLUR_CONTROLLER_CONNECTION_STR = "blur.controller.connection.str";
  public static final String FAMILY = "blur.family";
  public static final String TABLE = "blur.table";
  public static final String ZK = BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
  public static final String BLUR_MR_LOAD_ID = "blur.mr.load.id";

  private String _family;
  private Map<String, ColumnDefinition> _schema;
  private ObjectInspector _objectInspector;
  private List<String> _columnNames;
  private List<TypeInfo> _columnTypes;
  private BlurSerializer _serializer;
  private BlurColumnNameResolver _columnNameResolver;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    String table = tbl.getProperty(TABLE);
    nullCheck(TABLE, table);
    _family = tbl.getProperty(FAMILY);
    nullCheck(FAMILY, _family);
    BlurConfiguration configuration;
    String zkConnectionStr;
    try {
      configuration = new BlurConfiguration();
      zkConnectionStr = tbl.getProperty(ZK);
      nullCheck(ZK, zkConnectionStr);
      configuration.set(ZK, zkConnectionStr);
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    Iface client = BlurClient.getClient(configuration);
    Schema schema;
    try {
      List<String> tableList = client.tableList();
      if (!tableList.contains(table)) {
        LOG.warn("Table [{0}] from zk [{1}] does not exist.", table, zkConnectionStr);
        _objectInspector = new NullStructSerDe.NullStructSerDeObjectInspector();
        return;
      }
      if (conf != null) {
        TableDescriptor tableDescriptor = client.describe(table);
        Map<String, String> tableProperties = tableDescriptor.getTableProperties();
        if (tableProperties != null) {
          String workingPath = tableProperties.get(BlurConstants.BLUR_BULK_UPDATE_WORKING_PATH);
          if (conf != null && workingPath != null) {
            if (!conf.getBoolean(BLUR_MR_UPDATE_DISABLED, false)) {
              conf.set(BlurConstants.BLUR_BULK_UPDATE_WORKING_PATH, workingPath);
            }
          }
        }

        BlurOutputFormat.setTableDescriptor(conf, tableDescriptor);
        conf.set(BLUR_CONTROLLER_CONNECTION_STR, getControllerConnectionStr(client));
      }
      schema = client.schema(table);
    } catch (BlurException e) {
      throw new SerDeException(e);
    } catch (TException e) {
      throw new SerDeException(e);
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    Map<String, ColumnDefinition> columns = schema.getFamilies().get(_family);
    if (columns == null) {
      throw new SerDeException("Family [" + _family + "] does not exist in table [" + table + "]");
    }

    _schema = new HashMap<String, ColumnDefinition>();
    for (ColumnDefinition columnDefinition : columns.values()) {
      String subColumnName = columnDefinition.getSubColumnName();
      if (subColumnName == null) {
        _schema.put(columnDefinition.getColumnName(), columnDefinition);
      }
    }

    _columnNameResolver = new BlurColumnNameResolver(_schema.values());

    BlurObjectInspectorGenerator blurObjectInspectorGenerator = new BlurObjectInspectorGenerator(_schema.values(),
        _columnNameResolver);
    _objectInspector = blurObjectInspectorGenerator.getObjectInspector();
    _columnNames = blurObjectInspectorGenerator.getColumnNames();
    _columnTypes = blurObjectInspectorGenerator.getColumnTypes();

    _serializer = new BlurSerializer(_schema, _columnNameResolver);
  }

  private void nullCheck(String name, String value) throws SerDeException {
    if (value == null) {
      throw new SerDeException("Property [" + name + "] cannot be null.");
    }
  }

  private String getControllerConnectionStr(Iface client) throws BlurException, TException {
    List<String> controllerServerList = client.controllerServerList();
    StringBuilder builder = new StringBuilder();
    for (String c : controllerServerList) {
      if (builder.length() != 0) {
        builder.append(',');
      }
      builder.append(c);
    }
    return builder.toString();
  }

  @Override
  public Writable serialize(Object o, ObjectInspector oi) throws SerDeException {
    return _serializer.serialize(o, oi, _columnNames, _columnTypes, _schema, _family);
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return _objectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BlurRecord.class;
  }

  public static boolean shouldUseMRWorkingPath(Configuration configuration) {
    String workingPath = configuration.get(BlurConstants.BLUR_BULK_UPDATE_WORKING_PATH);
    if (workingPath != null) {
      return true;
    }
    return false;
  }

}
