package org.apache.blur;

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
import java.io.IOException;
import java.util.Map;

public abstract class CachedMap {

  /**
   * Clears the in memory cache of the map, this forces a re-read from the
   * source.
   */
  public abstract void clearCache() throws IOException;

  /**
   * Fetches the value by key, if the in memory cache is missing the value then
   * re-read from source if missing from source return null.
   * 
   * @param key
   *          the key.
   * @return the value.
   * @throws IOException
   */
  public abstract String get(String key) throws IOException;

  /**
   * Puts the value with the given key into the map if the key was missing.
   * Returns true if the key with the given value was set otherwise false if a
   * key already existed.
   * 
   * @param key
   *          the key.
   * @param value
   *          the value.
   * @return boolean true is successful, false if not.
   */
  public abstract boolean putIfMissing(String key, String value) throws IOException;

  /**
   * Fetches all the keys and values for the map from the source. That means
   * this an expensive operation and should be used sparingly.
   * 
   * @return the map of all keys to values.
   * @throws IOException 
   */
  public abstract Map<String, String> fetchAllFromSource() throws IOException;

}
