/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.analysis;

import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.RECORD_ID;
import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.util.Version;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class BlurAnalyzer extends PerFieldAnalyzerWrapper {

	private static final String FULLTEXT = "fulltext";
    private static final String ALIASES = "aliases";
    private static final String SEP = ".";
	private static final String FIELDS = "fields";
	private static final String DEFAULT = "default";
	
    public static BlurAnalyzer create(Path path) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        return create(fileSystem.open(path));
    }
	
    public static BlurAnalyzer create(File file) throws IOException {
        return create(new FileInputStream(file));
    }
	
	public static BlurAnalyzer create(InputStream input) throws IOException {
	    return create(toString(input));
	}

	private static String toString(InputStream input) {
	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	    byte[] buffer = new byte[1024];
	    int numb = 0;
	    try {
            while ((numb = input.read(buffer)) != -1) {
                outputStream.write(buffer, 0, numb);
            }
            outputStream.close();
            input.close();
            return new String(outputStream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static BlurAnalyzer create(String s) throws IOException {
		if (s == null || s.trim().isEmpty()) {
			return new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_30),"", new HashSet<String>());
		}
		try {
            return create(new JSONObject(s),s);
        } catch (JSONException e) {
            throw new IOException(e);
        }
	}

    private String _originalJsonStr;
    private Map<String,Store> _storeMap = new HashMap<String, Store>();
    private Map<String,Set<String>> _subIndexNameLookups = new HashMap<String, Set<String>>();
    private Set<String> _fullTextFields;
    
    public void addSubField(String name) {
        int lastIndexOf = name.lastIndexOf('.');
        String mainFieldName = name.substring(0,lastIndexOf);
        Set<String> set = _subIndexNameLookups.get(mainFieldName);
        if (set == null) {
            set = new TreeSet<String>();
            _subIndexNameLookups.put(mainFieldName, set);
        }
        set.add(name);
    }
    
    public Set<String> getSubIndexNames(String indexName) {
        return _subIndexNameLookups.get(indexName);
    }
    
    public BlurAnalyzer(Analyzer defaultAnalyzer, String jsonStr) {
        this(defaultAnalyzer,jsonStr,new HashSet<String>());
    }

	public BlurAnalyzer(Analyzer defaultAnalyzer, String jsonStr, Set<String> fullTextFields) {
		super(defaultAnalyzer);
		this._originalJsonStr = jsonStr;
		KeywordAnalyzer keywordAnalyzer = new KeywordAnalyzer();
        addAnalyzer(ROW_ID, keywordAnalyzer);
		addAnalyzer(RECORD_ID, keywordAnalyzer);
		addAnalyzer(PRIME_DOC, keywordAnalyzer);
		_fullTextFields = fullTextFields;
	}
	
	private static BlurAnalyzer create(JSONObject jsonObject, String jsonStr) {
	    try {
	        Object object = null;
	        if (!jsonObject.isNull(ALIASES)) {
	            object = jsonObject.get(ALIASES);
	        }
            Map<String,Class<? extends Analyzer>> aliases = getAliases(object);
    		Analyzer defaultAnalyzer = getAnalyzer(jsonObject.get(DEFAULT),aliases);
    		Set<String> fullText;
    		if (jsonObject.isNull(FULLTEXT)) {
    		    fullText = new HashSet<String>();
    		} else {
    		    fullText = getFullText(jsonObject.getJSONObject(FULLTEXT));
    		}
    		BlurAnalyzer analyzer = new BlurAnalyzer(defaultAnalyzer, jsonStr, fullText);
    		JSONObject o = null;
    		if (!jsonObject.isNull(FIELDS)) {
    		    o = jsonObject.getJSONObject(FIELDS);
    		}
            populate(analyzer, "", o, Store.YES,aliases);
    		return analyzer;
	    } catch (Exception e) {
	        throw new RuntimeException(e);
        }
	}

	private static Set<String> getFullText(JSONObject jsonObject) throws JSONException {
	    Set<String> set = new HashSet<String>();
	    Iterator<?> keys = jsonObject.keys();
	    while (keys.hasNext()) {
	        Object key = keys.next();
	        String keyStr = key.toString();
            JSONArray jsonArray = jsonObject.getJSONArray(keyStr);
	        for (int i = 0; i < jsonArray.length(); i++) {
	            set.add(keyStr + "." + jsonArray.getString(i));
	        }
	    }
        return set;
    }

    private static Map<String, Class<? extends Analyzer>> getAliases(Object o) throws ClassNotFoundException, JSONException {
	    Map<String, Class<? extends Analyzer>> map = new HashMap<String, Class<? extends Analyzer>>();
	    if (o == null) {
            return map;
	    }
	    if (o instanceof JSONArray) {
	        JSONArray array = (JSONArray) o;
	        int length = array.length();
	        for (int i = 0; i < length; i++) {
	            JSONObject object = array.getJSONObject(i);
                addAlias(map,object);
	        }
	        return map;
	    }
	    throw new IllegalArgumentException("aliases has to be an array.");
    }

    @SuppressWarnings("unchecked")
    private static void addAlias(Map<String, Class<? extends Analyzer>> map, JSONObject jsonObject) throws ClassNotFoundException, JSONException {
        Iterator<String> fieldNames = jsonObject.keys();
        while (fieldNames.hasNext()) {
            String name = fieldNames.next();
            String className = jsonObject.getString(name);
            Class<? extends Analyzer> clazz = (Class<? extends Analyzer>) Class.forName(className);
            map.put(name, clazz);
        }
    }

    @SuppressWarnings("unchecked")
    private static void populate(BlurAnalyzer analyzer, String name, Object o, Store store, Map<String, Class<? extends Analyzer>> aliases) throws SecurityException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, JSONException {
		if (o == null) {
			return;
		}
		if (o instanceof JSONObject) {
		    JSONObject jsonObject = (JSONObject) o;
			for (String key : it((Iterator<String>)jsonObject.keys())) {
				populate(analyzer, getName(name,key.toString()), jsonObject.get(key), store, aliases);
			}
		} else if (o instanceof JSONArray) {
		    JSONArray array = (JSONArray) o;
			int length = array.length();
			for (int i = 0; i < length; i++) {
				Object node = array.get(i);
				if (node instanceof JSONObject) {
				    populate(analyzer, name, node, Store.NO, aliases);
				} else {
				    populate(analyzer, name, node, Store.YES, aliases);
				}
			}
		} else {
			Analyzer a = getAnalyzerByClassName(o.toString(), aliases);
			analyzer.addAnalyzer(name, a);
			analyzer.putStore(name,store);
			if (store == Store.NO) {
			    analyzer.addSubField(name);
			}
		}
	}

	public void putStore(String name, Store store) {
	    _storeMap.put(name, store);
    }

    private static <T> Iterable<T> it(final Iterator<T> iterator) {
		return new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return iterator;
			}
		};
	}

	private static String getName(String baseName, String newName) {
		if (baseName.isEmpty()) {
			return newName;
		}
		return baseName + SEP + newName;
	}

	private static Analyzer getAnalyzer(Object o, Map<String, Class<? extends Analyzer>> aliases) throws SecurityException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
		if (o == null) {
			throw new NullPointerException();
		}
		String cn;
		if (o instanceof JSONObject) {
		    JSONObject jsonObject  = (JSONObject) o;
		    cn = jsonObject.toString();
		} else {
			cn = o.toString();
		}
		return getAnalyzerByClassName(cn, aliases);
	}

	@SuppressWarnings("unchecked")
    private static Analyzer getAnalyzerByClassName(String className, Map<String, Class<? extends Analyzer>> aliases) throws ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
	    Class<? extends Analyzer> clazz = aliases.get(className);
	    if (clazz == null) {
	        clazz = (Class<? extends Analyzer>) Class.forName(className);
	    }
		try {
			return (Analyzer) clazz.newInstance();
		} catch (Exception e) {
			Constructor<?> constructor = clazz.getConstructor(new Class[]{Version.class});
			return (Analyzer) constructor.newInstance(Version.LUCENE_30);
		}
	}

    @Override
    public String toString() {
        return _originalJsonStr;
    }

    public Store getStore(String indexName) {
        Store store = _storeMap.get(indexName);
        if (store == null) {
            return Store.YES;
        }
        return store;
    }
    
    public Index getIndex(String indexName) {
        return Index.ANALYZED_NO_NORMS;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_originalJsonStr == null) ? 0 : _originalJsonStr.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BlurAnalyzer other = (BlurAnalyzer) obj;
        if (_originalJsonStr == null) {
            if (other._originalJsonStr != null)
                return false;
        } else if (!_originalJsonStr.equals(other._originalJsonStr))
            return false;
        return true;
    }

    public boolean isFullTextField(String fieldName) {
        return _fullTextFields.contains(fieldName);
    }
}
