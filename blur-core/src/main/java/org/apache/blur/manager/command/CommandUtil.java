package org.apache.blur.manager.command;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Value;

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

public class CommandUtil {

  public static org.apache.blur.thrift.generated.Response convert(Response response) throws BlurException {
    org.apache.blur.thrift.generated.Response converted = new org.apache.blur.thrift.generated.Response();
    if (response.isAggregatedResults()) {
      converted.setValue(toValue(response.getServerResult()));
    } else {
      converted.setShardToValue(convert(response.getShardResults()));
    }
    return converted;
  }

  public static Map<String, Value> convert(Map<Shard, Object> map) throws BlurException {
    Map<String, Value> result = new HashMap<String, Value>();
    for (Entry<Shard, Object> e : map.entrySet()) {
      // @TODO need to make different setters for shard and server results
      result.put(e.getKey().getShard(), toValue(e.getValue()));
    }
    return result;
  }

  public static Value toValue(Object o) throws BlurException {
    Value value = new Value();
    if (o == null) {
      value.setNullValue(true);
      return value;
    }
    if (o instanceof Long) {
      value.setLongValue((Long) o);
      return value;
    } else if (o instanceof String) {
      value.setStringValue((String) o);
      return value;
    } else if (o instanceof Integer) {
      value.setIntValue((Integer) o);
      return value;
    }
    throw new BException("Object [{0}] not supported.", o);
  }

  public static Args toArgs(Arguments arguments) {
    if (arguments == null) {
      return null;
    }
    Args args = new Args();
    Map<String, Value> values = arguments.getValues();
    Set<Entry<String, Value>> entrySet = values.entrySet();
    for (Entry<String, Value> e : entrySet) {
      args.set(e.getKey(), toObject(e.getValue()));
    }
    return args;
  }

  public static Object toObject(Value value) {
    if (value.isSetNullValue()) {
      return null;
    }
    return value.getFieldValue();
  }

  public static Arguments toArguments(Args args) throws BlurException {
    if (args == null) {
      return null;
    }
    Arguments arguments = new Arguments();
    Set<Entry<String, Object>> entrySet = args.getValues().entrySet();
    for (Entry<String, Object> e : entrySet) {
      arguments.putToValues(e.getKey(), toValue(e.getValue()));
    }
    return arguments;
  }
}
