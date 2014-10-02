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
package org.apache.blur.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class BlurObjectSerDe {

  private static final String SET = "set";
  private static final String VALUE = "v";
  private static final String KEY = "k";
  private static final String MAP = "map";
  private static final String LIST = "list";
  private static final String TYPE = "serdetype";

  // TODO make real unit test
  
  // public static <E> void main(String[] args) {
  // BlurObjectSerDe serde = new BlurObjectSerDe();
  // Map<String, Long> m = new HashMap<String, Long>();
  // m.put("sub", 100L);
  // Map<String, Object> map = new HashMap<String, Object>();
  // map.put("a", "a");
  // map.put("b", Arrays.asList("a", "b", "c"));
  // map.put("c", m);
  //
  // Set<Integer> set = new HashSet<Integer>();
  // set.add(3);
  // set.add(5);
  // map.put("d", set);
  //
  // BlurObject bo1 = serde.serialize(map);
  // System.out.println(bo1.toString(1));
  //
  // Map<String, ? extends Object> map2 = serde.deserialize(bo1);
  // System.out.println(map2);
  //
  // Set<?> set2 = (Set<?>) map.get("d");
  // System.out.println(set2);
  // }

  public BlurObject serialize(Map<String, ? extends Object> args) {
    BlurObject result = new BlurObject();
    for (Entry<String, ? extends Object> e : args.entrySet()) {
      result.put(e.getKey(), toSupportedThriftObject(e.getValue()));
    }
    return result;
  }

  public Object toSupportedThriftObject(Object o) {
    if (BlurObject.supportedType(o)) {
      return o;
    } else {
      return convertToSupportedType(o);
    }
  }

  @SuppressWarnings("unchecked")
  private BlurObject convertToSupportedType(Object o) {
    if (o instanceof Set) {
      return toBlurObject((Set<?>) o);
    } else if (o instanceof Map) {
      return toBlurObject((Map<Object, Object>) o);
    } else if (o instanceof List) {
      return toBlurObject((List<Object>) o);
    } else {
      return toBlurObjectFromUnknown(o);
    }
  }

  private BlurObject toBlurObject(Map<Object, Object> o) {
    BlurObject blurObject = new BlurObject();
    blurObject.put(TYPE, MAP);
    BlurArray blurArray = new BlurArray();
    Set<Entry<Object, Object>> entrySet = o.entrySet();
    for (Entry<Object, Object> e : entrySet) {
      BlurObject entry = new BlurObject();
      entry.put(KEY, toSupportedThriftObject(e.getKey()));
      entry.put(VALUE, toSupportedThriftObject(e.getValue()));
      blurArray.put(entry);
    }
    blurObject.put(MAP, blurArray);
    return blurObject;
  }

  private BlurObject toBlurObject(List<Object> o) {
    BlurObject blurObject = new BlurObject();
    blurObject.put(TYPE, LIST);
    BlurArray blurArray = new BlurArray();
    for (Object t : o) {
      blurArray.put(toSupportedThriftObject(t));
    }
    blurObject.put(LIST, blurArray);
    return blurObject;
  }

  private BlurObject toBlurObject(Set<?> o) {
    BlurObject blurObject = new BlurObject();
    blurObject.put(TYPE, SET);
    BlurArray blurArray = new BlurArray();
    for (Object t : o) {
      blurArray.put(toSupportedThriftObject(t));
    }
    blurObject.put(SET, blurArray);
    return blurObject;
  }

  private BlurObject toBlurObjectFromUnknown(Object o) {
    throw new RuntimeException("Not implemented.");
  }

  public Map<String, ? extends Object> deserialize(BlurObject blurObject) {
    Map<String, Object> map = new HashMap<String, Object>();
    Iterator<String> keys = blurObject.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      map.put(key, fromSupportedThriftObject(blurObject.get(key)));
    }
    return map;
  }

  public Object fromSupportedThriftObject(Object object) {
    if (object instanceof BlurObject) {
      BlurObject bo = (BlurObject) object;
      if (isCustomObject(bo)) {
        return convertFromSupportedType(bo);
      }
    }
    return object;
  }

  private Object convertFromSupportedType(BlurObject blurObject) {
    String type = blurObject.getString(TYPE);
    if (type.equals(LIST)) {
      return toList(blurObject);
    } else if (type.equals(MAP)) {
      return toMap(blurObject);
    } else if (type.equals(SET)) {
      return toSet(blurObject);
    } else {
      return toUnknownObject(blurObject);
    }
  }

  private Object toSet(BlurObject blurObject) {
    BlurArray blurArray = blurObject.getBlurArray(SET);
    Set<Object> set = new HashSet<Object>();
    int length = blurArray.length();
    for (int i = 0; i < length; i++) {
      set.add(fromSupportedThriftObject(blurArray.get(i)));
    }
    return set;
  }

  private Object toUnknownObject(BlurObject blurObject) {
    throw new RuntimeException("Not implemented.");
  }

  private Map<? extends Object, ? extends Object> toMap(BlurObject blurObject) {
    Map<Object, Object> result = new HashMap<Object, Object>();
    BlurArray blurArray = blurObject.getBlurArray(MAP);
    int length = blurArray.length();
    for (int i = 0; i < length; i++) {
      BlurObject bo = blurArray.getBlurObject(i);
      Object key = bo.get(KEY);
      Object value = bo.get(VALUE);
      result.put(fromSupportedThriftObject(key), fromSupportedThriftObject(value));
    }
    return result;
  }

  private List<? extends Object> toList(BlurObject blurObject) {
    BlurArray blurArray = blurObject.getBlurArray(LIST);
    List<Object> list = new ArrayList<Object>();
    int length = blurArray.length();
    for (int i = 0; i < length; i++) {
      list.add(fromSupportedThriftObject(blurArray.get(i)));
    }
    return list;
  }

  private boolean isCustomObject(BlurObject bo) {
    if (bo.hasKey(TYPE)) {
      return true;
    }
    return false;
  }

}
