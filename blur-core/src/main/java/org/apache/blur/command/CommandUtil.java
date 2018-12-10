package org.apache.blur.command;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.blur.command.annotation.OptionalArgument;
import org.apache.blur.command.annotation.RequiredArgument;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Value;
import org.apache.blur.thrift.generated.ValueObject;
import org.apache.blur.thrift.generated.ValueObject._Fields;

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

  public static org.apache.blur.thrift.generated.Response fromObjectToThrift(Response response, BlurObjectSerDe serDe)
      throws BlurException {
    org.apache.blur.thrift.generated.Response converted = new org.apache.blur.thrift.generated.Response();

    if (response.isAggregatedResults()) {
      Object result = response.getServerResult();
      Object supportedThriftObject = serDe.toSupportedThriftObject(result);
      converted.setValue(toValueObject(supportedThriftObject));
    } else {
      Map<Server, Object> serverResults = response.getServerResults();
      if (serverResults == null) {
        Map<Shard, Object> shardResults = response.getShardResults();
        Map<Shard, Object> supportedThriftObjectShardResults = toThriftSupportedObjects(shardResults, serDe);
        Map<org.apache.blur.thrift.generated.Shard, ValueObject> fromObjectToThrift = fromObjectToThrift(supportedThriftObjectShardResults);
        converted.setShardToValue(fromObjectToThrift);
      } else {
        Map<Server, Object> supportedThriftObjectServerResults = toThriftSupportedObjects(serverResults, serDe);
        Map<org.apache.blur.thrift.generated.Server, ValueObject> fromObjectToThrift = fromObjectToThrift(supportedThriftObjectServerResults);
        converted.setServerToValue(fromObjectToThrift);
      }
    }
    return converted;
  }

  public static <T> Map<T, Object> toThriftSupportedObjects(Map<T, Object> map, BlurObjectSerDe serDe) {
    Map<T, Object> results = new HashMap<T, Object>();
    for (Entry<T, Object> e : map.entrySet()) {
      Object supportedThriftObject = serDe.toSupportedThriftObject(e.getValue());
      results.put(e.getKey(), supportedThriftObject);
    }
    return results;
  }

  public static <T> Map<T, Object> fromThriftSupportedObjects(Map<T, Object> map, BlurObjectSerDe serDe) {
    Map<T, Object> results = new HashMap<T, Object>();
    for (Entry<T, Object> e : map.entrySet()) {
      Object fromSupportedThriftObject = serDe.fromSupportedThriftObject(e.getValue());
      results.put(e.getKey(), fromSupportedThriftObject);
    }
    return results;
  }

  @SuppressWarnings("unchecked")
  public static <T, R> Map<R, ValueObject> fromObjectToThrift(Map<T, Object> map) throws BlurException {
    Map<R, ValueObject> result = new HashMap<R, ValueObject>();
    for (Entry<T, Object> e : map.entrySet()) {
      T key = e.getKey();
      if (key instanceof Shard) {
        Shard shard = (Shard) key;
        result.put((R) new org.apache.blur.thrift.generated.Shard(shard.getTable(), shard.getShard()),
            toValueObject(e.getValue()));
      } else if (key instanceof Server) {
        Server server = (Server) key;
        result.put((R) new org.apache.blur.thrift.generated.Server(server.getServer()), toValueObject(e.getValue()));
      }
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
    } else if (o instanceof Boolean) {
      value.setBooleanValue((Boolean) o);
      return value;
    } else if (o instanceof Short) {
      value.setShortValue((Short) o);
      return value;
    } else if (o instanceof byte[]) {
      value.setBinaryValue((byte[]) o);
      return value;
    } else if (o instanceof ByteBuffer) {
      ByteBuffer buff = (ByteBuffer) o;
      byte[] temp = new byte[buff.remaining()];
      buff.get(temp);
      value.setBinaryValue(temp);
      return value;
    } else if (o instanceof Double) {
      value.setDoubleValue((Double) o);
      return value;
    } else if (o instanceof Float) {
      value.setFloatValue((Float) o);
      return value;
    }
    throw new BException("Object [{0}] not supported.", o);
  }

  public static ValueObject toValueObject(Object o) throws BlurException {
    ValueObject valueObject = new ValueObject();
    if (o == null) {
      valueObject.setValue(toValue(o));
    } else if (o instanceof BlurObject || o instanceof BlurArray) {
      valueObject.setBlurObject(ObjectArrayPacking.pack(o));
    } else {
      valueObject.setValue(toValue(o));
    }
    return valueObject;
  }

  public static BlurObject toBlurObject(Arguments arguments) {
    if (arguments == null) {
      return null;
    }
    BlurObject blurObject = new BlurObject();
    Map<String, ValueObject> values = arguments.getValues();
    Set<Entry<String, ValueObject>> entrySet = values.entrySet();
    for (Entry<String, ValueObject> e : entrySet) {
      blurObject.put(e.getKey(), toObject(e.getValue()));
    }
    return blurObject;
  }

  public static Object toObject(Value value) {
    if (value.isSetNullValue()) {
      return null;
    }
    return value.getFieldValue();
  }

  @SuppressWarnings("unchecked")
  public static <T> Map<Shard, T> fromThriftToObjectShard(
      Map<org.apache.blur.thrift.generated.Shard, ValueObject> shardToValue) {
    Map<Shard, T> result = new HashMap<Shard, T>();
    for (Entry<org.apache.blur.thrift.generated.Shard, ValueObject> e : shardToValue.entrySet()) {
      org.apache.blur.thrift.generated.Shard shard = e.getKey();
      result.put(new Shard(shard.getTable(), shard.getShard()), (T) CommandUtil.toObject(e.getValue()));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public static <T> Map<Server, T> fromThriftToObjectServer(
      Map<org.apache.blur.thrift.generated.Server, ValueObject> serverToValue) {
    Map<Server, T> result = new HashMap<Server, T>();
    for (Entry<org.apache.blur.thrift.generated.Server, ValueObject> e : serverToValue.entrySet()) {
      org.apache.blur.thrift.generated.Server server = e.getKey();
      result.put(new Server(server.getServer()), (T) CommandUtil.toObject(e.getValue()));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public static <T> T toObject(ValueObject valueObject) {
    _Fields field = valueObject.getSetField();
    switch (field) {
    case VALUE:
      return (T) toObject(valueObject.getValue());
    case BLUR_OBJECT:
      return (T) ObjectArrayPacking.unpack(valueObject.getBlurObject());
    default:
      throw new RuntimeException("Type unknown.");
    }
  }

  public static Object fromThriftResponseToObject(org.apache.blur.thrift.generated.Response response) {
    org.apache.blur.thrift.generated.Response._Fields setField = response.getSetField();
    switch (setField) {
    case SERVER_TO_VALUE:
      Map<org.apache.blur.thrift.generated.Server, ValueObject> serverToValue = response.getServerToValue();
      return fromThriftToObjectServer(serverToValue);
    case SHARD_TO_VALUE:
      Map<org.apache.blur.thrift.generated.Shard, ValueObject> shardToValue = response.getShardToValue();
      return fromThriftToObjectShard(shardToValue);
    case VALUE:
      return toObject(response.getValue());
    default:
      throw new RuntimeException("Not supported.");
    }
  }

  public static Arguments toArguments(Command<?> command, BlurObjectSerDe serde) throws BlurException {
    Class<?> clazz = command.getClass();
    Map<String, Object> args = new HashMap<String, Object>();
    addArguments(clazz, args, command);
    BlurObject blurObject = serde.serialize(args);
    return toArguments(blurObject);
  }

  public static Arguments toArguments(BlurObject blurObject) throws BlurException {
    Arguments arguments = new Arguments();
    Iterator<String> keys = blurObject.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      Object object = blurObject.get(key);
      arguments.putToValues(key, toValueObject(object));
    }
    return arguments;
  }

  private static void addArguments(Class<?> clazz, Map<String, Object> arguments, Command<?> command) {
    if (!(clazz.equals(Command.class))) {
      addArguments(clazz.getSuperclass(), arguments, command);
    }
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      RequiredArgument requiredArgument = field.getAnnotation(RequiredArgument.class);
      if (requiredArgument != null) {
        field.setAccessible(true);
        String name = field.getName();
        try {
          Object o = field.get(command);
          if (o != null) {
            arguments.put(name, o);
          } else {
            throw new IllegalArgumentException("Field [" + name + "] is required.");
          }
        } catch (IllegalArgumentException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }

      OptionalArgument optionalArgument = field.getAnnotation(OptionalArgument.class);
      if (optionalArgument != null) {
        field.setAccessible(true);
        String name = field.getName();
        try {
          Object o = field.get(command);
          if (o != null) {
            arguments.put(name, o);
          }
        } catch (IllegalArgumentException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
