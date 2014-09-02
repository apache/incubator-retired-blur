package org.apache.blur.manager.command;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurObjectType;
import org.apache.blur.thrift.generated.BlurPackedObject;

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

public class ObjectArrayPacking {

  public static void main(String[] args) throws BlurException {
    BlurObject object = newBlurObject();
    System.out.println(object.toString(1));

    List<BlurPackedObject> packedVersion = pack(object);

    int index = 0;
    for (BlurPackedObject packedObject : packedVersion) {
      System.out.println(index + " " + packedObject);
      index++;
    }

    BlurObject object2 = (BlurObject) unpack(packedVersion);
    System.out.println(object2.toString(1));
    System.out.println(object2.toString());
  }

  public static List<BlurPackedObject> pack(Object object) throws BlurException {
    List<BlurPackedObject> packed = new ArrayList<BlurPackedObject>();
    pack(-1, object, packed);
    return packed;
  }

  public static Object unpack(List<BlurPackedObject> packedVersion) {
    int size = packedVersion.size();
    Object[] objects = new Object[size];
    for (int i = 0; i < size; i++) {
      BlurPackedObject packedObject = packedVersion.get(i);
      switch (packedObject.type) {
      case MAP:
        objects[i] = new BlurObject();
        break;
      case LIST:
        objects[i] = new BlurArray();
        break;
      case VALUE:
        objects[i] = CommandUtil.toObject(packedObject.value);
        break;
      case NAME:
        objects[i] = CommandUtil.toObject(packedObject.value);
        break;
      default:
        throw new RuntimeException();
      }
    }

    for (int i = 0; i < size; i++) {
      BlurPackedObject packedObject = packedVersion.get(i);
      switch (packedObject.type) {
      case NAME:
        break;
      case MAP:
      case LIST:
      case VALUE:
        addValue(i, objects, packedVersion);
        break;
      default:
        throw new RuntimeException();
      }
    }
    return objects[0];
  }

  private static void addValue(int index, Object[] objects, List<BlurPackedObject> packedVersion) {
    BlurPackedObject packedObject = packedVersion.get(index);
    int parentId = packedObject.parentId;
    if (parentId == -1) {
      // root
      return;
    }
    Object value = objects[index];
    BlurPackedObject po = packedVersion.get(parentId);
    if (po.type == BlurObjectType.NAME) {
      BlurObject map = (BlurObject) objects[po.parentId];
      String key = (String) CommandUtil.toObject(po.value);
      map.put(key, value);
    } else if (po.type == BlurObjectType.LIST) {
      BlurArray array = (BlurArray) objects[parentId];
      array.put(value);
    } else {
      throw new RuntimeException();
    }
  }

  private static void pack(int parentId, Object object, List<BlurPackedObject> packed) throws BlurException {
    if (object instanceof BlurObject) {
      int id = packed.size();
      BlurPackedObject packedObject = new BlurPackedObject(parentId, BlurObjectType.MAP, null);
      packed.add(packedObject);
      BlurObject blurObject = (BlurObject) object;
      Iterator<String> keys = blurObject.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        Object o = blurObject.getObject(key);
        BlurPackedObject po = new BlurPackedObject(id, BlurObjectType.NAME, CommandUtil.toValue(key));
        int newId = packed.size();
        packed.add(po);
        pack(newId, o, packed);
      }
    } else if (object instanceof BlurArray) {
      int id = packed.size();
      BlurPackedObject packedObject = new BlurPackedObject(parentId, BlurObjectType.LIST, null);
      packed.add(packedObject);
      BlurArray array = (BlurArray) object;
      int length = array.length();
      for (int i = 0; i < length; i++) {
        Object o = array.getObject(i);
        pack(id, o, packed);
      }
    } else {
      packed.add(new BlurPackedObject(parentId, BlurObjectType.VALUE, CommandUtil.toValue(object)));
    }
  }

  private static BlurObject newBlurObject() {
    BlurObject jsonObject = new BlurObject();
    BlurObject node1 = new BlurObject();
    node1.accumulate("f1", "v1");
    node1.accumulate("f2", "v2a");
    node1.accumulate("f2", "v2b");
    jsonObject.accumulate("node1", node1);
    BlurArray node2 = new BlurArray();
    node2.put("val1");
    node2.put("val2");
    node2.put("val3");
    jsonObject.put("node2", node2);

    BlurObject node3 = new BlurObject();
    BlurObject node3n1 = new BlurObject();
    node3n1.put("a", "b");
    node3.put("n1", node3n1);
    jsonObject.put("node3", node3);
    return jsonObject;
  }

}