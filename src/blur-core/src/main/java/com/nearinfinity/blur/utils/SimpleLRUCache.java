package com.nearinfinity.blur.utils;

import java.util.LinkedHashMap;
import java.util.Map;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class SimpleLRUCache<K, V> extends LinkedHashMap<K, V> {

  private static final Log LOG = LogFactory.getLog(SimpleLRUCache.class);
  private static final long serialVersionUID = -3775525555245664265L;
  private int _cachedElements;
  private String _name;

  public SimpleLRUCache(String name, int cachedElements) {
    super();
    _cachedElements = cachedElements;
    _name = name;
  }

  public SimpleLRUCache(String name, int cachedElements, int initialCapacity, float loadFactor, boolean accessOrder) {
    super(initialCapacity, loadFactor, accessOrder);
    _cachedElements = cachedElements;
    _name = name;
  }

  public SimpleLRUCache(String name, int cachedElements, int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
    _cachedElements = cachedElements;
    _name = name;
  }

  public SimpleLRUCache(String name, int cachedElements, int initialCapacity) {
    super(initialCapacity);
    _cachedElements = cachedElements;
    _name = name;
  }

  public SimpleLRUCache(String name, int cachedElements, Map<? extends K, ? extends V> m) {
    super(m);
    _cachedElements = cachedElements;
    _name = name;
  }

  @Override
  protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
    boolean b = size() > _cachedElements;
    if (b) {
      LOG.debug("Cache [{0}], evicting eldest element [{1}]", _name, eldest);
    }
    return b;
  }

  public boolean touch(K key) {
    V value = remove(key);
    if (value != null) {
      put(key, value);
      return true;
    }
    return false;
  }

  public static void main(String[] args) {
    SimpleLRUCache<String, String> simpleLRUCache = new SimpleLRUCache<String, String>("test", 3);

    simpleLRUCache.put("a", "a");
    simpleLRUCache.put("b", "b");
    simpleLRUCache.put("c", "c");
    simpleLRUCache.touch("a");
    simpleLRUCache.put("d", "d");
    simpleLRUCache.put("e", "e");
  }

}
